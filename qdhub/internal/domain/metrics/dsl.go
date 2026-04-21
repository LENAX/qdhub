package metrics

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"qdhub/internal/domain/shared"
)

var metricIDPattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]{0,63}$`)

var windowParamArgPositions = map[string]int{
	"ma":            1,
	"sum":           1,
	"ref":           1,
	"ts_count":      1,
	"pearson_corr":  2,
	"spearman_corr": 2,
	"limit10":       1,
	"limit20":       1,
	"limit30":       1,
	"top":           1,
}

type ValueKind string

const (
	ValueKindUnknown ValueKind = "unknown"
	ValueKindNumber  ValueKind = "number"
	ValueKindBool    ValueKind = "bool"
	ValueKindString  ValueKind = "enum_string"
	ValueKindNull    ValueKind = "null"
)

type Value struct {
	Kind   ValueKind
	Number float64
	Bool   bool
	String string
}

func NullValue() Value              { return Value{Kind: ValueKindNull} }
func NumberValue(v float64) Value   { return Value{Kind: ValueKindNumber, Number: v} }
func BoolValue(v bool) Value        { return Value{Kind: ValueKindBool, Bool: v} }
func StringValue(v string) Value    { return Value{Kind: ValueKindString, String: v} }
func UnknownValue() Value           { return Value{Kind: ValueKindUnknown} }
func (v Value) IsNull() bool        { return v.Kind == ValueKindNull }
func (v Value) IsNumericLike() bool { return v.Kind == ValueKindNumber || v.Kind == ValueKindUnknown }
func (v Value) IsBoolLike() bool    { return v.Kind == ValueKindBool || v.Kind == ValueKindUnknown }
func (v Value) IsStringLike() bool  { return v.Kind == ValueKindString || v.Kind == ValueKindUnknown }
func (v Value) IsComparableLike() bool {
	return v.IsNumericLike() || v.IsStringLike() || v.IsBoolLike()
}

type Node interface {
	nodeName() string
}

type NumberNode struct{ Value float64 }
type BoolNode struct{ Value bool }
type StringNode struct{ Value string }
type IdentifierNode struct{ Name string }
type CallNode struct {
	Name string
	Args []Node
}

func (NumberNode) nodeName() string     { return "number" }
func (BoolNode) nodeName() string       { return "bool" }
func (StringNode) nodeName() string     { return "string" }
func (IdentifierNode) nodeName() string { return "identifier" }
func (CallNode) nodeName() string       { return "call" }

type dslParser struct {
	input string
	pos   int
}

func NewDSLParser() ExpressionParser {
	return &dslParser{}
}

func (p *dslParser) Parse(expression string) (Node, error) {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return nil, shared.NewDomainError(shared.ErrCodeValidation, "expression is required", nil)
	}
	parser := &dslParser{input: expression}
	node, err := parser.parseExpr()
	if err != nil {
		return nil, err
	}
	parser.skipSpaces()
	if parser.pos != len(parser.input) {
		return nil, shared.NewDomainError(shared.ErrCodeValidation, "unexpected trailing tokens in expression", nil)
	}
	return node, nil
}

func (p *dslParser) Validate(metric *MetricDef) error {
	if metric == nil {
		return shared.NewDomainError(shared.ErrCodeValidation, "metric definition is required", nil)
	}
	if !metricIDPattern.MatchString(metric.ID) {
		return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("metric_id %q must match ^[a-zA-Z][a-zA-Z0-9_]{0,63}$", metric.ID), nil)
	}
	node, err := p.Parse(metric.Expression)
	if err != nil {
		return err
	}
	if err := validateWindowParams(node); err != nil {
		return err
	}
	if err := validateSpearmanRoot(node); err != nil {
		return err
	}
	kind, err := inferNodeKind(node)
	if err != nil {
		return err
	}
	switch metric.Kind {
	case MetricKindFactor:
		if kind != ValueKindNumber && kind != ValueKindUnknown {
			return shared.NewDomainError(shared.ErrCodeValidation, "factor expression must return number", nil)
		}
	case MetricKindSignal:
		if kind != ValueKindBool && kind != ValueKindString && kind != ValueKindUnknown {
			return shared.NewDomainError(shared.ErrCodeValidation, "signal expression must return bool or enum_string", nil)
		}
	case MetricKindUniverse:
		if kind != ValueKindBool && kind != ValueKindUnknown {
			return shared.NewDomainError(shared.ErrCodeValidation, "universe expression must return bool", nil)
		}
	}
	return nil
}

func (p *dslParser) CollectIdentifiers(node Node) []string {
	set := make(map[string]struct{})
	collectIdentifiers(node, set)
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	return out
}

func collectIdentifiers(node Node, out map[string]struct{}) {
	switch n := node.(type) {
	case IdentifierNode:
		if !isReservedIdentifier(n.Name) {
			out[n.Name] = struct{}{}
		}
	case *IdentifierNode:
		if !isReservedIdentifier(n.Name) {
			out[n.Name] = struct{}{}
		}
	case CallNode:
		for _, arg := range n.Args {
			collectIdentifiers(arg, out)
		}
	case *CallNode:
		for _, arg := range n.Args {
			collectIdentifiers(arg, out)
		}
	}
}

func isReservedIdentifier(name string) bool {
	return strings.EqualFold(name, "true") || strings.EqualFold(name, "false") || name == "universe"
}

// validateWindowParams ensures that the window-size argument of time-series and cross-sectional
// ranking functions is a literal NumberNode rather than an expression or identifier. This keeps
// the downstream SQL compiler able to inline the window as a constant.
func validateWindowParams(node Node) error {
	switch n := node.(type) {
	case CallNode:
		return validateCallWindowParams(n.Name, n.Args)
	case *CallNode:
		return validateCallWindowParams(n.Name, n.Args)
	}
	return nil
}

func validateCallWindowParams(name string, args []Node) error {
	lowered := strings.ToLower(strings.TrimSpace(name))
	if pos, ok := windowParamArgPositions[lowered]; ok {
		if pos >= len(args) {
			return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("%s missing window argument", lowered), nil)
		}
		if !isNumberLiteral(args[pos]) {
			return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("%s window argument must be a numeric literal", lowered), nil)
		}
	}
	for _, arg := range args {
		if err := validateWindowParams(arg); err != nil {
			return err
		}
	}
	return nil
}

func isNumberLiteral(node Node) bool {
	switch node.(type) {
	case NumberNode, *NumberNode:
		return true
	}
	return false
}

// validateSpearmanRoot enforces that spearman_corr only appears at the root of the expression tree.
// The SQL compiler materialises this as a residual compute path rather than DuckDB SQL, so nesting
// it inside other operators would be undefined.
func validateSpearmanRoot(root Node) error {
	if isSpearmanCall(root) {
		return validateNoSpearmanBelow(rootArgs(root))
	}
	return validateNoSpearmanInTree(root)
}

func rootArgs(node Node) []Node {
	switch n := node.(type) {
	case CallNode:
		return n.Args
	case *CallNode:
		return n.Args
	}
	return nil
}

func validateNoSpearmanBelow(args []Node) error {
	for _, arg := range args {
		if err := validateNoSpearmanInTree(arg); err != nil {
			return err
		}
	}
	return nil
}

func validateNoSpearmanInTree(node Node) error {
	if isSpearmanCall(node) {
		return shared.NewDomainError(shared.ErrCodeValidation, "spearman_corr must be the root expression", nil)
	}
	switch n := node.(type) {
	case CallNode:
		return validateNoSpearmanBelow(n.Args)
	case *CallNode:
		return validateNoSpearmanBelow(n.Args)
	}
	return nil
}

func isSpearmanCall(node Node) bool {
	switch n := node.(type) {
	case CallNode:
		return strings.EqualFold(strings.TrimSpace(n.Name), "spearman_corr")
	case *CallNode:
		return strings.EqualFold(strings.TrimSpace(n.Name), "spearman_corr")
	}
	return false
}

func (p *dslParser) parseExpr() (Node, error) {
	p.skipSpaces()
	if p.pos >= len(p.input) {
		return nil, shared.NewDomainError(shared.ErrCodeValidation, "unexpected end of expression", nil)
	}
	ch := p.input[p.pos]
	if isQuote(ch) {
		return p.parseString()
	}
	if isDigit(ch) || ch == '-' {
		return p.parseNumber()
	}
	if isIdentStart(ch) {
		ident := p.parseIdentifier()
		p.skipSpaces()
		if strings.EqualFold(ident, "true") {
			return BoolNode{Value: true}, nil
		}
		if strings.EqualFold(ident, "false") {
			return BoolNode{Value: false}, nil
		}
		if p.pos < len(p.input) && p.input[p.pos] == '(' {
			p.pos++
			args, err := p.parseArgs()
			if err != nil {
				return nil, err
			}
			return CallNode{Name: ident, Args: args}, nil
		}
		return IdentifierNode{Name: ident}, nil
	}
	return nil, shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("unexpected token at position %d", p.pos), nil)
}

func (p *dslParser) parseArgs() ([]Node, error) {
	args := make([]Node, 0)
	for {
		p.skipSpaces()
		if p.pos >= len(p.input) {
			return nil, shared.NewDomainError(shared.ErrCodeValidation, "unexpected end while parsing arguments", nil)
		}
		if p.input[p.pos] == ')' {
			p.pos++
			return args, nil
		}
		arg, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
		p.skipSpaces()
		if p.pos >= len(p.input) {
			return nil, shared.NewDomainError(shared.ErrCodeValidation, "unexpected end while parsing arguments", nil)
		}
		switch p.input[p.pos] {
		case ',':
			p.pos++
		case ')':
			p.pos++
			return args, nil
		default:
			return nil, shared.NewDomainError(shared.ErrCodeValidation, "expected ',' or ')' in argument list", nil)
		}
	}
}

func (p *dslParser) parseIdentifier() string {
	start := p.pos
	p.pos++
	for p.pos < len(p.input) && isIdentPart(p.input[p.pos]) {
		p.pos++
	}
	return p.input[start:p.pos]
}

func (p *dslParser) parseString() (Node, error) {
	quote := p.input[p.pos]
	p.pos++
	start := p.pos
	for p.pos < len(p.input) && p.input[p.pos] != quote {
		p.pos++
	}
	if p.pos >= len(p.input) {
		return nil, shared.NewDomainError(shared.ErrCodeValidation, "unterminated string literal", nil)
	}
	value := p.input[start:p.pos]
	p.pos++
	return StringNode{Value: value}, nil
}

func (p *dslParser) parseNumber() (Node, error) {
	start := p.pos
	if p.input[p.pos] == '-' {
		p.pos++
	}
	for p.pos < len(p.input) && (isDigit(p.input[p.pos]) || p.input[p.pos] == '.') {
		p.pos++
	}
	value, err := strconv.ParseFloat(p.input[start:p.pos], 64)
	if err != nil {
		return nil, shared.NewDomainError(shared.ErrCodeValidation, "invalid numeric literal", err)
	}
	return NumberNode{Value: value}, nil
}

func (p *dslParser) skipSpaces() {
	for p.pos < len(p.input) && (p.input[p.pos] == ' ' || p.input[p.pos] == '\n' || p.input[p.pos] == '\t' || p.input[p.pos] == '\r') {
		p.pos++
	}
}

func inferNodeKind(node Node) (ValueKind, error) {
	switch n := node.(type) {
	case NumberNode, *NumberNode:
		return ValueKindNumber, nil
	case BoolNode, *BoolNode:
		return ValueKindBool, nil
	case StringNode, *StringNode:
		return ValueKindString, nil
	case IdentifierNode, *IdentifierNode:
		return ValueKindUnknown, nil
	case CallNode:
		return inferCallKind(n.Name, n.Args)
	case *CallNode:
		return inferCallKind(n.Name, n.Args)
	default:
		return ValueKindUnknown, shared.NewDomainError(shared.ErrCodeValidation, "unsupported node type", nil)
	}
}

func inferCallKind(name string, args []Node) (ValueKind, error) {
	name = strings.ToLower(strings.TrimSpace(name))
	return inferCallSignature(name, args)
}

func inferCallSignature(name string, args []Node) (ValueKind, error) {
	switch name {
	case "add", "sub", "mul", "div", "sum", "ma", "ref", "gtval", "ltval", "pearson_corr", "spearman_corr", "ts_count", "rank", "count":
		if name == "ts_count" || name == "count" {
			if len(args) == 0 {
				return ValueKindUnknown, shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("%s requires arguments", name), nil)
			}
			argKind, err := inferNodeKind(args[0])
			if err != nil {
				return ValueKindUnknown, err
			}
			if argKind != ValueKindBool && argKind != ValueKindUnknown {
				return ValueKindUnknown, shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("%s requires bool expression", name), nil)
			}
		}
		return ValueKindNumber, nil
	case "gt", "lt", "eq", "gte", "lte", "and", "or", "not", "top", "limit10", "limit20", "limit30":
		if (name == "and" || name == "or" || name == "not") && len(args) > 0 {
			for _, arg := range args {
				argKind, err := inferNodeKind(arg)
				if err != nil {
					return ValueKindUnknown, err
				}
				if argKind != ValueKindBool && argKind != ValueKindUnknown {
					return ValueKindUnknown, shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("%s requires bool arguments", name), nil)
				}
			}
		}
		return ValueKindBool, nil
	default:
		return ValueKindUnknown, shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("unsupported DSL function: %s", name), nil)
	}
}

func ToNumber(v Value) (float64, bool) {
	if v.Kind != ValueKindNumber {
		return 0, false
	}
	if math.IsNaN(v.Number) {
		return 0, false
	}
	return v.Number, true
}

func ToBool(v Value) (bool, bool) {
	if v.Kind != ValueKindBool {
		return false, false
	}
	return v.Bool, true
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isIdentStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isIdentPart(ch byte) bool {
	return isIdentStart(ch) || isDigit(ch)
}

func isQuote(ch byte) bool {
	return ch == '\'' || ch == '"'
}
