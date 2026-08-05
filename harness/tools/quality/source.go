package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const modulePath = "github.com/mnemon-dev/mnemon"

var (
	generatedDirectivePattern = regexp.MustCompile(`^// Code generated .+ DO NOT EDIT\.$`)
	nolintDirectivePattern    = regexp.MustCompile(`^//nolint:([A-Za-z0-9_,]+)(?: -- | // )\S.*$`)
)

type sourceFile struct {
	Path           string
	Absolute       string
	Data           []byte
	AST            *ast.File
	FileSet        *token.FileSet
	IsTest         bool
	MetricExcluded bool
	LineCount      int
}

type architectureFinding struct {
	Rule      string
	Identity  string
	Path      string
	Component string
	Evidence  string
}

func loadHarnessSources(root string) ([]sourceFile, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve root: %w", err)
	}
	harnessRoot := filepath.Join(root, "harness")
	if info, statErr := os.Stat(harnessRoot); statErr != nil || !info.IsDir() {
		return nil, fmt.Errorf("root %q does not contain a harness directory", root)
	}
	exclusions, err := loadQualityExclusions(root, false)
	if err != nil {
		return nil, err
	}
	if err := validateExclusionEvidence(root, exclusions); err != nil {
		return nil, err
	}
	excludedKinds := exclusionKinds(exclusions)
	scope := loadRepositoryGoScope(root)
	var files []sourceFile
	err = filepath.WalkDir(harnessRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != harnessRoot && skipRepositoryDirectory(root, path, entry.Name(), scope) {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 || !strings.HasSuffix(entry.Name(), ".go") {
			return nil
		}
		relative, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return fmt.Errorf("relativize %s: %w", path, relErr)
		}
		relative = filepath.ToSlash(relative)
		if scope.gitScoped {
			if _, included := scope.paths[relative]; !included {
				return nil
			}
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return fmt.Errorf("read %s: %w", path, readErr)
		}
		fileSet := token.NewFileSet()
		parsed, parseErr := parser.ParseFile(fileSet, path, data, parser.ParseComments|parser.SkipObjectResolution)
		if parseErr != nil {
			return fmt.Errorf("parse %s: %w", filepath.ToSlash(relative), parseErr)
		}
		files = append(files, sourceFile{
			Path: relative, Absolute: path, Data: data, AST: parsed, FileSet: fileSet,
			IsTest: strings.HasSuffix(entry.Name(), "_test.go"), MetricExcluded: excludedKinds[relative] != "",
			LineCount: physicalLineCount(data),
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk harness sources: %w", err)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, nil
}

func metricEligibleSources(files []sourceFile) []sourceFile {
	eligible := make([]sourceFile, 0, len(files))
	for _, file := range files {
		if !file.MetricExcluded {
			eligible = append(eligible, file)
		}
	}
	return eligible
}

func isGeneratedSource(data []byte) bool {
	parsed, err := parser.ParseFile(token.NewFileSet(), "generated.go", data, parser.ParseComments|parser.SkipObjectResolution)
	if err != nil {
		return false
	}
	for _, group := range parsed.Comments {
		if group.Pos() > parsed.Package {
			break
		}
		for _, comment := range group.List {
			if generatedDirectivePattern.MatchString(strings.TrimSuffix(comment.Text, "\r")) {
				return true
			}
		}
	}
	return false
}

func physicalLineCount(data []byte) int {
	if len(data) == 0 {
		return 0
	}
	lines := bytes.Count(data, []byte{'\n'})
	if data[len(data)-1] != '\n' {
		lines++
	}
	return lines
}

func gofmtDrift(files []sourceFile) ([]string, error) {
	var drift []string
	for _, file := range files {
		formatted, err := format.Source(file.Data)
		if err != nil {
			return nil, fmt.Errorf("gofmt %s: %w", file.Path, err)
		}
		if !bytes.Equal(formatted, file.Data) {
			drift = append(drift, file.Path)
		}
	}
	return drift, nil
}

func nolintDiagnostics(files []sourceFile) []string {
	var diagnostics []string
	for _, file := range files {
		for _, group := range file.AST.Comments {
			for _, comment := range group.List {
				text := strings.TrimSpace(comment.Text)
				if !strings.HasPrefix(text, "//nolint") {
					continue
				}
				match := nolintDirectivePattern.FindStringSubmatch(text)
				if len(match) != 2 || lintListHasWildcard(match[1]) {
					line := file.FileSet.PositionFor(comment.Pos(), false).Line
					diagnostics = append(diagnostics, fmt.Sprintf("%s:%d", file.Path, line))
				}
			}
		}
	}
	return diagnostics
}

func lintListHasWildcard(list string) bool {
	for _, name := range strings.Split(list, ",") {
		if name == "all" || name == "*" || name == "" {
			return true
		}
	}
	return false
}

func declaredSymbols(file sourceFile) map[string]struct{} {
	symbols := make(map[string]struct{})
	for _, declaration := range file.AST.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		symbols[functionSymbol(function)] = struct{}{}
	}
	return symbols
}

func functionSymbol(function *ast.FuncDecl) string {
	if function.Recv == nil || len(function.Recv.List) == 0 {
		return function.Name.Name
	}
	receiver := function.Recv.List[0].Type
	return receiverSymbol(receiver) + "." + function.Name.Name
}

func receiverSymbol(expression ast.Expr) string {
	switch value := expression.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.StarExpr:
		return "(*" + receiverSymbol(value.X) + ")"
	case *ast.IndexExpr:
		return receiverSymbol(value.X)
	case *ast.IndexListExpr:
		return receiverSymbol(value.X)
	default:
		return "(?)"
	}
}
