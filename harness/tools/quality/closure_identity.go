package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"go/ast"
	"go/token"
	"strings"
)

type closureLiteral struct {
	literal   *ast.FuncLit
	ancestors []ast.Node
}

const closureDigestBytes = 12

type closureCollector struct {
	ancestors []ast.Node
	closures  *[]closureLiteral
}

type closureIdentityVector struct {
	cyclomatic   int
	cognitive    int
	logicalLines int
	statements   int
	nesting      int
}

type closureIdentityCandidate struct {
	closure    closureLiteral
	base       string
	descriptor string
	equivalent string
}

func measureFunctionTree(file sourceFile, symbol string, start token.Pos, body *ast.BlockStmt, measured *[]functionMeasurement) error {
	*measured = append(*measured, measureFunction(file, symbol, start, body))
	return measureChildClosures(file, symbol, collectDirectClosures(body), measured)
}

func measurePackageFunctionLiterals(file sourceFile, measured *[]functionMeasurement) error {
	var closures []closureLiteral
	for _, declaration := range file.AST.Decls {
		if _, isFunction := declaration.(*ast.FuncDecl); isFunction {
			continue
		}
		closures = append(closures, collectDirectClosures(declaration)...)
	}
	return measureChildClosures(file, "$package", closures, measured)
}

func measureChildClosures(file sourceFile, parent string, closures []closureLiteral, measured *[]functionMeasurement) error {
	candidates := make([]closureIdentityCandidate, 0, len(closures))
	equivalenceByBase := make(map[string]string, len(closures))
	groupOrder := make([]string, 0, len(closures))
	groups := make(map[string][]closureIdentityCandidate, len(closures))
	for _, closure := range closures {
		candidate := stableClosureIdentity(file, parent, closure)
		if prior, exists := equivalenceByBase[candidate.base]; exists && prior != candidate.equivalent {
			return fmt.Errorf("closure identity collision in %s below %s for %s at %s; extract or name the closures explicitly", file.Path, parent, candidate.descriptor, candidate.base)
		}
		if _, exists := equivalenceByBase[candidate.base]; !exists {
			groupOrder = append(groupOrder, candidate.base)
		}
		equivalenceByBase[candidate.base] = candidate.equivalent
		candidates = append(candidates, candidate)
		groups[candidate.base] = append(groups[candidate.base], candidate)
	}
	ordinals := make(map[string]int)
	for _, candidate := range candidates {
		ordinals[candidate.base]++
		symbol := fmt.Sprintf("%s-%03d", candidate.base, ordinals[candidate.base])
		*measured = append(*measured, measureFunction(file, symbol, candidate.closure.literal.Type.Pos(), candidate.closure.literal.Body))
	}
	for _, base := range groupOrder {
		var descendants []closureLiteral
		for _, candidate := range groups[base] {
			descendants = append(descendants, collectDirectClosures(candidate.closure.literal.Body)...)
		}
		if err := measureChildClosures(file, base, descendants, measured); err != nil {
			return err
		}
	}
	return nil
}

func stableClosureIdentity(file sourceFile, parent string, closure closureLiteral) closureIdentityCandidate {
	anchor, descriptor := closureAnchor(closure)
	tokens, vector := closureOwnShape(file, closure.literal)
	digest := tokenFingerprint(tokens)
	shape := hex.EncodeToString(digest[:closureDigestBytes])
	vectorKey := vector.key()
	return closureIdentityCandidate{
		closure:    closure,
		base:       parent + ".$func-" + anchor + "-" + shape + "-" + vectorKey,
		descriptor: descriptor,
		equivalent: descriptor + "\x00" + strings.Join(tokens, "\x00") + "\x00" + vectorKey +
			"\x00" + closureObservationSignature(file, closure),
	}
}

func closureObservationSignature(file sourceFile, closure closureLiteral) string {
	measurement := measureFunction(file, "", closure.literal.Type.Pos(), closure.literal.Body)
	var signature strings.Builder
	fmt.Fprintf(&signature, "metrics:%d:%d:%d:%d:%d", measurement.Cyclomatic, measurement.Cognitive,
		measurement.LogicalLines, measurement.Statements, measurement.Nesting)
	appendClosureSignaturePart(&signature, strings.Join(measurement.Tokens, "\x00"))
	for _, child := range collectDirectClosures(closure.literal.Body) {
		_, descriptor := closureAnchor(child)
		appendClosureSignaturePart(&signature, descriptor)
		appendClosureSignaturePart(&signature, closureObservationSignature(file, child))
	}
	return signature.String()
}

func appendClosureSignaturePart(signature *strings.Builder, value string) {
	fmt.Fprintf(signature, "|%d:", len(value))
	signature.WriteString(value)
}

func (vector closureIdentityVector) key() string {
	return fmt.Sprintf("cy%d-co%d-li%d-st%d-ne%d", vector.cyclomatic, vector.cognitive, vector.logicalLines, vector.statements, vector.nesting)
}

func closureOwnShape(file sourceFile, literal *ast.FuncLit) ([]string, closureIdentityVector) {
	nodes := directNestedClosureCarriers(literal.Body)
	spans := make([]tokenSpan, 0, len(nodes))
	excluded := make(map[token.Pos]token.Pos, len(nodes))
	for _, node := range nodes {
		spans = append(spans, tokenSpan{
			start: file.FileSet.PositionFor(node.Pos(), false).Offset,
			end:   file.FileSet.PositionFor(node.End(), false).Offset,
		})
		excluded[node.Pos()] = node.End()
	}
	tokens := normalizedTokenSpan(file, literal.Type.Pos(), literal.Body.End(), spans)
	metrics := &flowMetrics{cyclomatic: 1}
	ast.Walk(flowVisitor{metrics: metrics, excluded: excluded}, literal.Body)
	return tokens, closureIdentityVector{
		cyclomatic: metrics.cyclomatic, cognitive: metrics.cognitive,
		logicalLines: logicalLinesExcluding(file, literal.Type.Pos(), literal.Body.End(), spans),
		statements:   metrics.statements, nesting: metrics.maxNesting,
	}
}

func directNestedClosureCarriers(body *ast.BlockStmt) []ast.Node {
	seen := make(map[token.Pos]token.Pos)
	var carriers []ast.Node
	for _, closure := range collectDirectClosures(body) {
		carrier := directNestedClosureCarrier(closure)
		if end, exists := seen[carrier.Pos()]; exists && end == carrier.End() {
			continue
		}
		seen[carrier.Pos()] = carrier.End()
		carriers = append(carriers, carrier)
	}
	return carriers
}

func directNestedClosureCarrier(closure closureLiteral) ast.Node {
	for index := len(closure.ancestors) - 1; index >= 0; index-- {
		node := closure.ancestors[index]
		if _, block := node.(*ast.BlockStmt); block {
			continue
		}
		if _, statement := node.(ast.Stmt); statement {
			return node
		}
	}
	return closure.literal
}

func collectDirectClosures(root ast.Node) []closureLiteral {
	var closures []closureLiteral
	ast.Walk(closureCollector{closures: &closures}, root)
	return closures
}

func (collector closureCollector) Visit(node ast.Node) ast.Visitor {
	if node == nil {
		return nil
	}
	if literal, ok := node.(*ast.FuncLit); ok {
		ancestors := append([]ast.Node(nil), collector.ancestors...)
		*collector.closures = append(*collector.closures, closureLiteral{literal: literal, ancestors: ancestors})
		return nil
	}
	ancestors := append(append([]ast.Node(nil), collector.ancestors...), node)
	return closureCollector{ancestors: ancestors, closures: collector.closures}
}

func closureAnchor(closure closureLiteral) (string, string) {
	var contexts []string
	for index := 0; index < len(closure.ancestors); index++ {
		if candidate, _, ok := closureContextDescriptor(closure.ancestors[index], closure.literal); ok {
			if strings.HasPrefix(candidate, "key:") {
				if label, exists := compositeSiblingStringLabel(closure, index); exists {
					candidate += ":label:" + label
				}
			}
			contexts = append(contexts, candidate)
		}
	}
	if len(contexts) == 0 {
		contexts = append(contexts, "closure")
	}
	descriptor := strings.Join(contexts, "/")
	digest := sha256.Sum256([]byte(descriptor))
	return hex.EncodeToString(digest[:closureDigestBytes]), descriptor
}

func compositeSiblingStringLabel(closure closureLiteral, contextIndex int) (string, bool) {
	for index := contextIndex - 1; index >= 0; index-- {
		literal, ok := closure.ancestors[index].(*ast.CompositeLit)
		if !ok {
			continue
		}
		for _, preferred := range []string{"name", "id", "kind", "key"} {
			for _, element := range literal.Elts {
				field, ok := element.(*ast.KeyValueExpr)
				if !ok || expressionName(field.Key) != preferred {
					continue
				}
				value, ok := field.Value.(*ast.BasicLit)
				if !ok || value.Kind != token.STRING {
					continue
				}
				digest := sha256.Sum256([]byte(preferred + "\x00" + value.Value))
				return hex.EncodeToString(digest[:closureDigestBytes]), true
			}
		}
		return "", false
	}
	return "", false
}

func closureContextDescriptor(node ast.Node, literal *ast.FuncLit) (string, bool, bool) {
	switch value := node.(type) {
	case *ast.AssignStmt:
		return assignmentClosureDescriptor(value, literal)
	case *ast.ValueSpec:
		return valueClosureDescriptor(value, literal)
	case *ast.CallExpr:
		return callClosureDescriptor(value, literal)
	case *ast.KeyValueExpr:
		if containsNode(value.Value, literal) {
			return "key:" + expressionName(value.Key), true, true
		}
	case *ast.ReturnStmt:
		for index, result := range value.Results {
			if containsNode(result, literal) {
				return fmt.Sprintf("return:%d", index), false, true
			}
		}
	}
	return "", false, false
}

func assignmentClosureDescriptor(statement *ast.AssignStmt, literal *ast.FuncLit) (string, bool, bool) {
	for index, expression := range statement.Rhs {
		if !containsNode(expression, literal) {
			continue
		}
		name := "result"
		if index < len(statement.Lhs) {
			name = expressionName(statement.Lhs[index])
		}
		return fmt.Sprintf("assign:%s:%d", name, index), name != "_" && name != "result", true
	}
	return "", false, false
}

func valueClosureDescriptor(specification *ast.ValueSpec, literal *ast.FuncLit) (string, bool, bool) {
	for index, expression := range specification.Values {
		if !containsNode(expression, literal) {
			continue
		}
		name := "value"
		if index < len(specification.Names) {
			name = specification.Names[index].Name
		}
		return fmt.Sprintf("value:%s:%d", name, index), name != "value", true
	}
	return "", false, false
}

func callClosureDescriptor(call *ast.CallExpr, literal *ast.FuncLit) (string, bool, bool) {
	for index, argument := range call.Args {
		if containsNode(argument, literal) {
			label, labelled := callLabel(call)
			return fmt.Sprintf("call:%s:%d:%s", expressionName(call.Fun), index, label), labelled, true
		}
	}
	return "", false, false
}

func callLabel(call *ast.CallExpr) (string, bool) {
	for _, argument := range call.Args {
		if literal, ok := argument.(*ast.BasicLit); ok && literal.Kind == token.STRING {
			digest := sha256.Sum256([]byte(literal.Value))
			return hex.EncodeToString(digest[:closureDigestBytes]), true
		}
	}
	return "unlabelled", false
}

func expressionName(expression ast.Expr) string {
	switch value := expression.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.SelectorExpr:
		return expressionName(value.X) + "." + value.Sel.Name
	case *ast.IndexExpr:
		return expressionName(value.X)
	case *ast.IndexListExpr:
		return expressionName(value.X)
	case *ast.StarExpr:
		return "ptr-" + expressionName(value.X)
	default:
		return fmt.Sprintf("%T", expression)
	}
}

func containsNode(container, target ast.Node) bool {
	return container.Pos() <= target.Pos() && container.End() >= target.End()
}
