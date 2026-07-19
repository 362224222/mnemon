package main

import (
	"go/ast"
	"go/scanner"
	"go/token"
	"sort"
)

type functionMeasurement struct {
	Path         string
	Symbol       string
	Cyclomatic   int
	Cognitive    int
	LogicalLines int
	Statements   int
	Nesting      int
	Tokens       []string
}

type flowMetrics struct {
	cyclomatic int
	cognitive  int
	statements int
	maxNesting int
}

type tokenSpan struct {
	start int
	end   int
}

func measureFunctions(files []sourceFile) ([]functionMeasurement, error) {
	var measured []functionMeasurement
	for _, file := range files {
		for _, declaration := range file.AST.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			symbol := functionSymbol(function)
			if err := measureFunctionTree(file, symbol, function.Type.Pos(), function.Body, &measured); err != nil {
				return nil, err
			}
		}
		if err := measurePackageFunctionLiterals(file, &measured); err != nil {
			return nil, err
		}
	}
	sort.Slice(measured, func(i, j int) bool {
		if measured[i].Path != measured[j].Path {
			return measured[i].Path < measured[j].Path
		}
		return measured[i].Symbol < measured[j].Symbol
	})
	return measured, nil
}

func measureFunction(file sourceFile, symbol string, start token.Pos, body *ast.BlockStmt) functionMeasurement {
	metrics := &flowMetrics{cyclomatic: 1}
	ast.Walk(flowVisitor{metrics: metrics}, body)
	nestedSpans := directFunctionLiteralSpans(file, body)
	return functionMeasurement{
		Path:         file.Path,
		Symbol:       symbol,
		Cyclomatic:   metrics.cyclomatic,
		Cognitive:    metrics.cognitive,
		LogicalLines: logicalLinesExcluding(file, start, body.End(), nil),
		Statements:   metrics.statements,
		Nesting:      metrics.maxNesting,
		Tokens:       normalizedTokenSpan(file, start, body.End(), nestedSpans),
	}
}

type flowVisitor struct {
	metrics  *flowMetrics
	nesting  int
	excluded map[token.Pos]token.Pos
}

func (visitor flowVisitor) Visit(node ast.Node) ast.Visitor {
	if node == nil {
		return nil
	}
	if end, excluded := visitor.excluded[node.Pos()]; excluded && end == node.End() {
		return nil
	}
	if _, nestedFunction := node.(*ast.FuncLit); nestedFunction {
		return nil
	}
	visitor.countStatement(node)
	childNesting := visitor.controlNesting(node)
	visitor.countFlatComplexity(node)
	if childNesting > visitor.metrics.maxNesting {
		visitor.metrics.maxNesting = childNesting
	}
	return flowVisitor{metrics: visitor.metrics, nesting: childNesting, excluded: visitor.excluded}
}

func (visitor flowVisitor) countStatement(node ast.Node) {
	statement, ok := node.(ast.Stmt)
	if !ok {
		return
	}
	switch statement.(type) {
	case *ast.BlockStmt, *ast.EmptyStmt:
	default:
		visitor.metrics.statements++
	}
}

func (visitor flowVisitor) controlNesting(node ast.Node) int {
	childNesting := visitor.nesting
	switch value := node.(type) {
	case *ast.IfStmt:
		visitor.addDecision()
		if value.Else != nil {
			visitor.metrics.cognitive++
		}
		childNesting++
	case *ast.ForStmt, *ast.RangeStmt, *ast.SwitchStmt, *ast.TypeSwitchStmt, *ast.SelectStmt:
		visitor.addDecision()
		childNesting++
	case *ast.CaseClause:
		if len(value.List) > 0 {
			visitor.metrics.cyclomatic++
		}
		childNesting++
	case *ast.CommClause:
		if value.Comm != nil {
			visitor.metrics.cyclomatic++
		}
		childNesting++
	}
	return childNesting
}

func (visitor flowVisitor) countFlatComplexity(node ast.Node) {
	if branch, ok := node.(*ast.BranchStmt); ok {
		if branch.Tok == token.BREAK || branch.Tok == token.CONTINUE || branch.Tok == token.GOTO {
			visitor.metrics.cognitive++
		}
	}
	binary, ok := node.(*ast.BinaryExpr)
	if ok && (binary.Op == token.LAND || binary.Op == token.LOR) {
		visitor.metrics.cyclomatic++
		visitor.metrics.cognitive++
	}
}

func (visitor flowVisitor) addDecision() {
	visitor.metrics.cyclomatic++
	visitor.metrics.cognitive += 1 + visitor.nesting
}

func logicalLinesExcluding(file sourceFile, start, end token.Pos, excluded []tokenSpan) int {
	startOffset := file.FileSet.PositionFor(start, false).Offset
	endOffset := file.FileSet.PositionFor(end, false).Offset
	if startOffset < 0 || endOffset <= startOffset || endOffset > len(file.Data) {
		return 0
	}
	var lexical scanner.Scanner
	lexicalFile := token.NewFileSet().AddFile(file.Path, -1, endOffset-startOffset)
	lexical.Init(lexicalFile, file.Data[startOffset:endOffset], nil, 0)
	lines := make(map[int]struct{})
	for {
		position, item, _ := lexical.Scan()
		if item == token.EOF {
			break
		}
		absoluteOffset := startOffset + lexicalFile.Offset(position)
		if offsetInSpans(absoluteOffset, excluded) {
			continue
		}
		if item == token.SEMICOLON || item == token.LBRACE || item == token.RBRACE || item == token.COMMA || item == token.LPAREN || item == token.RPAREN {
			continue
		}
		lines[lexicalFile.Position(position).Line] = struct{}{}
	}
	return len(lines)
}

func directFunctionLiteralSpans(file sourceFile, body *ast.BlockStmt) []tokenSpan {
	var excluded []tokenSpan
	ast.Inspect(body, func(node ast.Node) bool {
		literal, ok := node.(*ast.FuncLit)
		if !ok {
			return true
		}
		excluded = append(excluded, tokenSpan{
			start: file.FileSet.PositionFor(literal.Pos(), false).Offset,
			end:   file.FileSet.PositionFor(literal.End(), false).Offset,
		})
		return false
	})
	return excluded
}

func normalizedTokenSpan(file sourceFile, start, end token.Pos, excluded []tokenSpan) []string {
	startOffset := file.FileSet.PositionFor(start, false).Offset
	endOffset := file.FileSet.PositionFor(end, false).Offset
	if startOffset < 0 || endOffset <= startOffset || endOffset > len(file.Data) {
		return nil
	}
	var lexical scanner.Scanner
	lexicalFile := token.NewFileSet().AddFile(file.Path, -1, endOffset-startOffset)
	lexical.Init(lexicalFile, file.Data[startOffset:endOffset], nil, 0)
	var tokens []string
	for {
		position, item, literal := lexical.Scan()
		if item == token.EOF {
			return tokens
		}
		absoluteOffset := startOffset + lexicalFile.Offset(position)
		if offsetInSpans(absoluteOffset, excluded) {
			continue
		}
		if item == token.SEMICOLON {
			continue
		}
		switch item {
		case token.INT, token.FLOAT, token.IMAG:
			tokens = append(tokens, "$number")
		case token.CHAR, token.STRING:
			tokens = append(tokens, "$string")
		case token.IDENT:
			tokens = append(tokens, literal)
		default:
			tokens = append(tokens, item.String())
		}
	}
}

func offsetInSpans(offset int, spans []tokenSpan) bool {
	for _, span := range spans {
		if offset >= span.start && offset < span.end {
			return true
		}
	}
	return false
}
