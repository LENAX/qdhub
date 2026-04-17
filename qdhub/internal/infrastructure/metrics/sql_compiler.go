package metrics

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"qdhub/internal/domain/datastore"
	domain "qdhub/internal/domain/metrics"
	"qdhub/internal/domain/shared"
)

// SQLCompiler turns a MetricDef (DSL expression) into a LogicalPlan ready to be
// executed against DuckDB. Compilation steps:
//   1. Parse expression into an AST (via domain parser).
//   2. Detect spearman_corr root — route to ResidualPlan.
//   3. Probe daily_basic / moneyflow existence and dep metric kinds.
//   4. Build BaseViewSpec + base CTE.
//   5. Lower AST to a scalar SQL expression, materialising any nested windows
//      as stage CTEs.
//   6. Wrap in INSERT ... SELECT for the target kind.
//   7. Validate full SQL via `EXPLAIN`.
type SQLCompiler struct {
	db         datastore.QuantDB
	parser     domain.ExpressionParser
	metricRepo domain.MetricDefRepository
	cache      *compileCache
}

// NewSQLCompiler returns a compiler bound to the given datastore and parser.
func NewSQLCompiler(db datastore.QuantDB, parser domain.ExpressionParser, metricRepo domain.MetricDefRepository) *SQLCompiler {
	return &SQLCompiler{db: db, parser: parser, metricRepo: metricRepo}
}

// Compile produces a LogicalPlan for the given metric and date range.
// When the compiler has a cache attached, stable plan bodies are reused across
// calls with different date ranges (the dates only flow into plan.Args).
func (c *SQLCompiler) Compile(ctx context.Context, metric *domain.MetricDef, startDate, endDate string) (*LogicalPlan, error) {
	if c.cache != nil {
		return c.cachedCompile(ctx, metric, startDate, endDate)
	}
	return c.compileFresh(ctx, metric, startDate, endDate)
}

// compileFresh runs the full compile pipeline without consulting the cache.
func (c *SQLCompiler) compileFresh(ctx context.Context, metric *domain.MetricDef, startDate, endDate string) (*LogicalPlan, error) {
	node, err := c.parser.Parse(metric.Expression)
	if err != nil {
		return nil, err
	}

	spec, err := c.buildBaseSpec(ctx, metric, startDate, endDate)
	if err != nil {
		return nil, err
	}

	if isSpearmanRoot(node) {
		return c.compileResidual(ctx, metric, spec, node)
	}

	plan, err := c.compileSQL(ctx, metric, spec, node)
	if err != nil {
		return nil, err
	}
	if err := c.validateSQL(ctx, plan.SQL, plan.Args); err != nil {
		return nil, fmt.Errorf("validate compiled SQL: %w", err)
	}
	return plan, nil
}

// buildBaseSpec gathers runtime-dependent facts needed for the base CTE.
func (c *SQLCompiler) buildBaseSpec(ctx context.Context, metric *domain.MetricDef, startDate, endDate string) (BaseViewSpec, error) {
	dailyBasicExists, _ := c.db.TableExists(ctx, "daily_basic")
	moneyflowExists, _ := c.db.TableExists(ctx, "moneyflow")

	spec := BaseViewSpec{
		StartDate:        startDate,
		EndDate:          endDate,
		DailyBasicExists: dailyBasicExists,
		MoneyflowExists:  moneyflowExists,
		Frequency:        string(metric.Frequency),
	}

	for _, depID := range metric.DependsOn {
		dep, err := c.metricRepo.Get(ctx, depID)
		if err != nil {
			return BaseViewSpec{}, fmt.Errorf("resolve depends_on %s: %w", depID, err)
		}
		switch dep.Kind {
		case domain.MetricKindFactor:
			spec.DependsOnFactors = append(spec.DependsOnFactors, depID)
		case domain.MetricKindSignal:
			spec.DependsOnSignals = append(spec.DependsOnSignals, depID)
		case domain.MetricKindUniverse:
			spec.DependsOnUniverses = append(spec.DependsOnUniverses, depID)
		default:
			return BaseViewSpec{}, fmt.Errorf("depends_on %s has unsupported kind %s", depID, dep.Kind)
		}
	}
	return spec, nil
}

// compileResidual builds a ResidualPlan for spearman_corr.
func (c *SQLCompiler) compileResidual(ctx context.Context, metric *domain.MetricDef, spec BaseViewSpec, node domain.Node) (*LogicalPlan, error) {
	call, args := callNameAndArgs(node)
	if call == "" || len(args) != 3 {
		return nil, shared.NewDomainError(shared.ErrCodeValidation, "spearman_corr requires exactly 3 arguments", nil)
	}
	ctxc := newCompileCtx(spec)
	xSQL, err := ctxc.compileScalar(args[0])
	if err != nil {
		return nil, err
	}
	ySQL, err := ctxc.compileScalar(args[1])
	if err != nil {
		return nil, err
	}
	window, err := literalInt(args[2])
	if err != nil {
		return nil, err
	}
	if ctxc.hasStages() {
		return nil, shared.NewDomainError(shared.ErrCodeValidation, "spearman_corr arguments may not contain window functions", nil)
	}

	// Residual InputSQL exposes (entity_id, trade_date, x, y) ordered by (entity_id, trade_date).
	var b strings.Builder
	b.WriteString("WITH ")
	b.WriteString(ctxc.spec.baseResult().CTE)
	b.WriteString("\n")
	b.WriteString("SELECT entity_id, trade_date, ")
	b.WriteString(xSQL)
	b.WriteString(" AS x, ")
	b.WriteString(ySQL)
	b.WriteString(" AS y\nFROM base\nORDER BY entity_id, trade_date")

	return &LogicalPlan{
		Kind: metric.Kind,
		Residual: &ResidualPlan{
			InputSQL:  b.String(),
			InputArgs: ctxc.spec.baseResult().Args,
			Fn:        "spearman_corr",
			Window:    window,
		},
	}, nil
}

// compileSQL lowers a pure-SQL DSL tree into an INSERT ... SELECT plan.
func (c *SQLCompiler) compileSQL(ctx context.Context, metric *domain.MetricDef, spec BaseViewSpec, node domain.Node) (*LogicalPlan, error) {
	ctxc := newCompileCtx(spec)
	exprSQL, err := ctxc.compileScalar(node)
	if err != nil {
		return nil, err
	}

	var b strings.Builder
	switch metric.Kind {
	case domain.MetricKindFactor:
		b.WriteString("INSERT INTO factor_value (metric_id, entity_id, trade_date, frequency, version, value, created_at)\n")
	case domain.MetricKindSignal:
		b.WriteString("INSERT INTO signal_value (metric_id, entity_id, trade_date, frequency, version, bool_value, text_value, created_at)\n")
	case domain.MetricKindUniverse:
		b.WriteString("INSERT INTO universe_membership (universe_id, entity_id, trade_date, frequency, version, created_at)\n")
	default:
		return nil, shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("unsupported metric kind %s", metric.Kind), nil)
	}

	// WITH base AS (...), stage1 AS (...), ...
	b.WriteString("WITH ")
	b.WriteString(ctxc.spec.baseResult().CTE)
	for _, stage := range ctxc.stages {
		b.WriteString(",\n")
		b.WriteString(stage.CTE)
	}
	b.WriteString("\n")

	metricIDLit := "'" + escapeSQLLiteral(metric.ID) + "'"
	freqLit := "'" + escapeSQLLiteral(string(metric.Frequency)) + "'"
	versionLit := strconv.Itoa(metric.Version)
	lastSource := ctxc.currentSource

	switch metric.Kind {
	case domain.MetricKindFactor:
		b.WriteString(fmt.Sprintf("SELECT %s AS metric_id, r.entity_id, r.trade_date, %s AS frequency, %s AS version, r.value, CURRENT_TIMESTAMP AS created_at\n",
			metricIDLit, freqLit, versionLit))
		b.WriteString("FROM (\n")
		b.WriteString(fmt.Sprintf("\tSELECT entity_id, trade_date, CAST(%s AS DOUBLE) AS value\n", exprSQL))
		b.WriteString(fmt.Sprintf("\tFROM %s\n", lastSource))
		b.WriteString(") r\n")
		b.WriteString("WHERE r.value IS NOT NULL AND NOT isnan(r.value) AND NOT isinf(r.value)")
	case domain.MetricKindSignal:
		b.WriteString(fmt.Sprintf("SELECT %s AS metric_id, r.entity_id, r.trade_date, %s AS frequency, %s AS version, r.bool_value, NULL AS text_value, CURRENT_TIMESTAMP AS created_at\n",
			metricIDLit, freqLit, versionLit))
		b.WriteString("FROM (\n")
		b.WriteString(fmt.Sprintf("\tSELECT entity_id, trade_date, CAST(%s AS BOOLEAN) AS bool_value\n", exprSQL))
		b.WriteString(fmt.Sprintf("\tFROM %s\n", lastSource))
		b.WriteString(") r\n")
		b.WriteString("WHERE r.bool_value IS NOT NULL")
	case domain.MetricKindUniverse:
		b.WriteString(fmt.Sprintf("SELECT %s AS universe_id, r.entity_id, r.trade_date, %s AS frequency, %s AS version, CURRENT_TIMESTAMP AS created_at\n",
			metricIDLit, freqLit, versionLit))
		b.WriteString("FROM (\n")
		b.WriteString(fmt.Sprintf("\tSELECT entity_id, trade_date, CAST(%s AS BOOLEAN) AS included\n", exprSQL))
		b.WriteString(fmt.Sprintf("\tFROM %s\n", lastSource))
		b.WriteString(") r\n")
		b.WriteString("WHERE COALESCE(r.included, FALSE) = TRUE")
	}

	return &LogicalPlan{
		Kind: metric.Kind,
		SQL:  b.String(),
		Args: ctxc.spec.baseResult().Args,
	}, nil
}

// validateSQL runs EXPLAIN against the DB to catch syntax/semantic errors early.
func (c *SQLCompiler) validateSQL(ctx context.Context, sql string, args []any) error {
	if strings.TrimSpace(sql) == "" {
		return nil
	}
	_, err := c.db.Query(ctx, "EXPLAIN "+sql, args...)
	return err
}

// ===================== Compile Context =====================

type stageCTE struct {
	Name    string
	Col     string
	CTE     string // full stageN AS (...) fragment
	From    string
}

type compileCtx struct {
	spec          BaseViewSpec
	baseCached    *BaseViewResult
	stages        []stageCTE
	currentSource string
	nextStage     int
}

func newCompileCtx(spec BaseViewSpec) *compileCtx {
	return &compileCtx{
		spec:          spec,
		currentSource: "base",
	}
}

func (ctx *compileCtx) hasStages() bool { return len(ctx.stages) > 0 }

// baseResult lazily caches the base view result so we only format once.
func (spec BaseViewSpec) baseResult() BaseViewResult {
	return BuildBaseView(spec)
}

// ===================== Scalar Compilation =====================

func (ctx *compileCtx) compileScalar(node domain.Node) (string, error) {
	switch n := node.(type) {
	case domain.NumberNode:
		return formatNumber(n.Value), nil
	case *domain.NumberNode:
		return formatNumber(n.Value), nil
	case domain.BoolNode:
		if n.Value {
			return "TRUE", nil
		}
		return "FALSE", nil
	case *domain.BoolNode:
		if n.Value {
			return "TRUE", nil
		}
		return "FALSE", nil
	case domain.StringNode:
		return "'" + escapeSQLLiteral(n.Value) + "'", nil
	case *domain.StringNode:
		return "'" + escapeSQLLiteral(n.Value) + "'", nil
	case domain.IdentifierNode:
		return ctx.compileIdentifier(n.Name), nil
	case *domain.IdentifierNode:
		return ctx.compileIdentifier(n.Name), nil
	case domain.CallNode:
		return ctx.compileCall(n.Name, n.Args)
	case *domain.CallNode:
		return ctx.compileCall(n.Name, n.Args)
	}
	return "", shared.NewDomainError(shared.ErrCodeValidation, "unsupported DSL node", nil)
}

// compileIdentifier maps a DSL identifier to a SQL column reference in the current source.
// The `universe` reserved identifier is emitted as NULL — for operators that actually
// consume it (none in phase 1) this would surface as an error downstream.
func (ctx *compileCtx) compileIdentifier(name string) string {
	if strings.EqualFold(name, "universe") {
		return "NULL"
	}
	return name
}

// compileCall dispatches to scalar operators or window/cross-sectional emitters.
func (ctx *compileCtx) compileCall(name string, args []domain.Node) (string, error) {
	lname := strings.ToLower(strings.TrimSpace(name))
	switch lname {
	case "add":
		return ctx.binaryArith(args, "+")
	case "sub":
		return ctx.binaryArith(args, "-")
	case "mul":
		return ctx.binaryArith(args, "*")
	case "div":
		if err := expectArgCount(lname, args, 2); err != nil {
			return "", err
		}
		left, err := ctx.compileScalar(args[0])
		if err != nil {
			return "", err
		}
		right, err := ctx.compileScalar(args[1])
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("(CAST(%s AS DOUBLE) / NULLIF(CAST(%s AS DOUBLE), 0))", left, right), nil
	case "gt":
		return ctx.binaryCompare(args, ">")
	case "gte":
		return ctx.binaryCompare(args, ">=")
	case "lt":
		return ctx.binaryCompare(args, "<")
	case "lte":
		return ctx.binaryCompare(args, "<=")
	case "eq":
		return ctx.binaryCompare(args, "=")
	case "and":
		return ctx.boolCombine(args, "AND")
	case "or":
		return ctx.boolCombine(args, "OR")
	case "not":
		if err := expectArgCount(lname, args, 1); err != nil {
			return "", err
		}
		inner, err := ctx.compileScalar(args[0])
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("(NOT COALESCE(%s, FALSE))", inner), nil
	case "gtval":
		if err := expectArgCount(lname, args, 2); err != nil {
			return "", err
		}
		left, err := ctx.compileScalar(args[0])
		if err != nil {
			return "", err
		}
		right, err := ctx.compileScalar(args[1])
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("GREATEST(%s, %s)", left, right), nil
	case "ltval":
		if err := expectArgCount(lname, args, 2); err != nil {
			return "", err
		}
		left, err := ctx.compileScalar(args[0])
		if err != nil {
			return "", err
		}
		right, err := ctx.compileScalar(args[1])
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("LEAST(%s, %s)", left, right), nil
	case "ref":
		return ctx.emitRef(args)
	case "ma":
		return ctx.emitRollingAgg(args, "AVG")
	case "sum":
		return ctx.emitRollingAgg(args, "SUM")
	case "ts_count":
		return ctx.emitTSCount(args)
	case "pearson_corr":
		return ctx.emitCorr(args)
	case "rank":
		return ctx.emitRank(args)
	case "top":
		return ctx.emitTop(args)
	case "count":
		return ctx.emitCrossCount(args)
	case "limit10":
		return ctx.emitLimit(args, "is_limit10")
	case "limit20":
		return ctx.emitLimit(args, "is_limit20")
	case "limit30":
		return ctx.emitLimit(args, "is_limit30")
	case "spearman_corr":
		return "", shared.NewDomainError(shared.ErrCodeValidation, "spearman_corr is only allowed at expression root", nil)
	}
	return "", shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("unsupported DSL function: %s", name), nil)
}

func (ctx *compileCtx) binaryArith(args []domain.Node, op string) (string, error) {
	if err := expectArgCount("arith", args, 2); err != nil {
		return "", err
	}
	left, err := ctx.compileScalar(args[0])
	if err != nil {
		return "", err
	}
	right, err := ctx.compileScalar(args[1])
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("(CAST(%s AS DOUBLE) %s CAST(%s AS DOUBLE))", left, op, right), nil
}

func (ctx *compileCtx) binaryCompare(args []domain.Node, op string) (string, error) {
	if err := expectArgCount("compare", args, 2); err != nil {
		return "", err
	}
	left, err := ctx.compileScalar(args[0])
	if err != nil {
		return "", err
	}
	right, err := ctx.compileScalar(args[1])
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("(%s %s %s)", left, op, right), nil
}

func (ctx *compileCtx) boolCombine(args []domain.Node, op string) (string, error) {
	if len(args) == 0 {
		return "", shared.NewDomainError(shared.ErrCodeValidation, "logical op requires at least one argument", nil)
	}
	parts := make([]string, 0, len(args))
	for _, arg := range args {
		sql, err := ctx.compileScalar(arg)
		if err != nil {
			return "", err
		}
		parts = append(parts, fmt.Sprintf("COALESCE(%s, FALSE)", sql))
	}
	return "(" + strings.Join(parts, " "+op+" ") + ")", nil
}

// ===================== Window Emitters =====================

const windowPartEntityOrderDate = "PARTITION BY entity_id ORDER BY trade_date"
const windowPartDate = "PARTITION BY trade_date"

// lowerWindowArg ensures the value-arg of a window function never contains
// another window function, materialising it into a stage CTE column if it does.
func (ctx *compileCtx) lowerWindowArg(arg domain.Node) (domain.Node, error) {
	if !containsWindowCall(arg) {
		return arg, nil
	}
	compiled, err := ctx.compileScalar(arg)
	if err != nil {
		return nil, err
	}
	colName := fmt.Sprintf("__stage_val_%d", ctx.nextStage+1)
	stageName := fmt.Sprintf("stage%d", ctx.nextStage+1)
	cte := fmt.Sprintf("%s AS (\n\tSELECT *, %s AS %s\n\tFROM %s\n)", stageName, compiled, colName, ctx.currentSource)
	ctx.stages = append(ctx.stages, stageCTE{Name: stageName, Col: colName, CTE: cte, From: ctx.currentSource})
	ctx.currentSource = stageName
	ctx.nextStage++
	return domain.IdentifierNode{Name: colName}, nil
}

func (ctx *compileCtx) emitRef(args []domain.Node) (string, error) {
	if err := expectArgCount("ref", args, 2); err != nil {
		return "", err
	}
	arg0, err := ctx.lowerWindowArg(args[0])
	if err != nil {
		return "", err
	}
	valueSQL, err := ctx.compileScalar(arg0)
	if err != nil {
		return "", err
	}
	offset, err := literalInt(args[1])
	if err != nil {
		return "", err
	}
	if offset < 0 {
		return "", shared.NewDomainError(shared.ErrCodeValidation, "ref offset must be non-negative", nil)
	}
	return fmt.Sprintf("LAG(%s, %d) OVER (%s)", valueSQL, offset, windowPartEntityOrderDate), nil
}

func (ctx *compileCtx) emitRollingAgg(args []domain.Node, fn string) (string, error) {
	if err := expectArgCount(strings.ToLower(fn), args, 2); err != nil {
		return "", err
	}
	arg0, err := ctx.lowerWindowArg(args[0])
	if err != nil {
		return "", err
	}
	valueSQL, err := ctx.compileScalar(arg0)
	if err != nil {
		return "", err
	}
	window, err := literalInt(args[1])
	if err != nil {
		return "", err
	}
	if window < 1 {
		return "", shared.NewDomainError(shared.ErrCodeValidation, "rolling window must be >= 1", nil)
	}
	return fmt.Sprintf("%s(CAST(%s AS DOUBLE)) OVER (%s ROWS BETWEEN %d PRECEDING AND CURRENT ROW)",
		fn, valueSQL, windowPartEntityOrderDate, window-1), nil
}

func (ctx *compileCtx) emitTSCount(args []domain.Node) (string, error) {
	if err := expectArgCount("ts_count", args, 2); err != nil {
		return "", err
	}
	arg0, err := ctx.lowerWindowArg(args[0])
	if err != nil {
		return "", err
	}
	condSQL, err := ctx.compileScalar(arg0)
	if err != nil {
		return "", err
	}
	window, err := literalInt(args[1])
	if err != nil {
		return "", err
	}
	if window < 1 {
		return "", shared.NewDomainError(shared.ErrCodeValidation, "ts_count window must be >= 1", nil)
	}
	return fmt.Sprintf("CAST(SUM(CASE WHEN %s THEN 1 ELSE 0 END) OVER (%s ROWS BETWEEN %d PRECEDING AND CURRENT ROW) AS DOUBLE)",
		condSQL, windowPartEntityOrderDate, window-1), nil
}

func (ctx *compileCtx) emitCorr(args []domain.Node) (string, error) {
	if err := expectArgCount("pearson_corr", args, 3); err != nil {
		return "", err
	}
	arg0, err := ctx.lowerWindowArg(args[0])
	if err != nil {
		return "", err
	}
	arg1, err := ctx.lowerWindowArg(args[1])
	if err != nil {
		return "", err
	}
	xSQL, err := ctx.compileScalar(arg0)
	if err != nil {
		return "", err
	}
	ySQL, err := ctx.compileScalar(arg1)
	if err != nil {
		return "", err
	}
	window, err := literalInt(args[2])
	if err != nil {
		return "", err
	}
	if window < 2 {
		return "", shared.NewDomainError(shared.ErrCodeValidation, "pearson_corr window must be >= 2", nil)
	}
	return fmt.Sprintf("CORR(CAST(%s AS DOUBLE), CAST(%s AS DOUBLE)) OVER (%s ROWS BETWEEN %d PRECEDING AND CURRENT ROW)",
		xSQL, ySQL, windowPartEntityOrderDate, window-1), nil
}

func (ctx *compileCtx) emitRank(args []domain.Node) (string, error) {
	if err := expectArgCount("rank", args, 1); err != nil {
		return "", err
	}
	arg0, err := ctx.lowerWindowArg(args[0])
	if err != nil {
		return "", err
	}
	valueSQL, err := ctx.compileScalar(arg0)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("CAST(ROW_NUMBER() OVER (%s ORDER BY %s DESC NULLS LAST, entity_id ASC) AS DOUBLE)",
		windowPartDate, valueSQL), nil
}

func (ctx *compileCtx) emitTop(args []domain.Node) (string, error) {
	if err := expectArgCount("top", args, 2); err != nil {
		return "", err
	}
	arg0, err := ctx.lowerWindowArg(args[0])
	if err != nil {
		return "", err
	}
	valueSQL, err := ctx.compileScalar(arg0)
	if err != nil {
		return "", err
	}
	k, err := literalInt(args[1])
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("(ROW_NUMBER() OVER (%s ORDER BY %s DESC NULLS LAST, entity_id ASC) <= %d)",
		windowPartDate, valueSQL, k), nil
}

func (ctx *compileCtx) emitCrossCount(args []domain.Node) (string, error) {
	if err := expectArgCount("count", args, 1); err != nil {
		return "", err
	}
	arg0, err := ctx.lowerWindowArg(args[0])
	if err != nil {
		return "", err
	}
	condSQL, err := ctx.compileScalar(arg0)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("CAST(SUM(CASE WHEN %s THEN 1 ELSE 0 END) OVER (%s) AS DOUBLE)",
		condSQL, windowPartDate), nil
}

// emitLimit translates limit10/20/30(_, window, target, exact). The first arg
// is ignored (evaluator parity). `featureCol` is the base CTE boolean column.
func (ctx *compileCtx) emitLimit(args []domain.Node, featureCol string) (string, error) {
	if err := expectArgCount(featureCol, args, 4); err != nil {
		return "", err
	}
	window, err := literalInt(args[1])
	if err != nil {
		return "", err
	}
	if window < 1 {
		return "", shared.NewDomainError(shared.ErrCodeValidation, "limit window must be >= 1", nil)
	}
	target, err := literalInt(args[2])
	if err != nil {
		return "", err
	}
	exact, err := literalBoolLike(args[3])
	if err != nil {
		return "", err
	}
	rolling := fmt.Sprintf("SUM(CASE WHEN %s THEN 1 ELSE 0 END) OVER (%s ROWS BETWEEN %d PRECEDING AND CURRENT ROW)",
		featureCol, windowPartEntityOrderDate, window-1)
	op := ">="
	if exact {
		op = "="
	}
	return fmt.Sprintf("(%s %s %d)", rolling, op, target), nil
}

// ===================== Utilities =====================

func expectArgCount(name string, args []domain.Node, want int) error {
	if len(args) != want {
		return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("%s requires exactly %d arguments", name, want), nil)
	}
	return nil
}

func formatNumber(v float64) string {
	if math.IsInf(v, 1) {
		return "'Infinity'::DOUBLE"
	}
	if math.IsInf(v, -1) {
		return "'-Infinity'::DOUBLE"
	}
	if math.IsNaN(v) {
		return "'NaN'::DOUBLE"
	}
	if v == float64(int64(v)) {
		return strconv.FormatInt(int64(v), 10)
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func literalInt(node domain.Node) (int, error) {
	switch n := node.(type) {
	case domain.NumberNode:
		return int(n.Value), nil
	case *domain.NumberNode:
		return int(n.Value), nil
	}
	return 0, shared.NewDomainError(shared.ErrCodeValidation, "expected numeric literal", nil)
}

// literalBoolLike accepts BoolNode or NumberNode (0 → false, non-zero → true)
// mirroring the old evaluator's evalBoolLikeArg.
func literalBoolLike(node domain.Node) (bool, error) {
	switch n := node.(type) {
	case domain.BoolNode:
		return n.Value, nil
	case *domain.BoolNode:
		return n.Value, nil
	case domain.NumberNode:
		return n.Value != 0, nil
	case *domain.NumberNode:
		return n.Value != 0, nil
	}
	return false, shared.NewDomainError(shared.ErrCodeValidation, "expected bool-like literal", nil)
}

func callNameAndArgs(node domain.Node) (string, []domain.Node) {
	switch n := node.(type) {
	case domain.CallNode:
		return strings.ToLower(strings.TrimSpace(n.Name)), n.Args
	case *domain.CallNode:
		return strings.ToLower(strings.TrimSpace(n.Name)), n.Args
	}
	return "", nil
}

func isSpearmanRoot(node domain.Node) bool {
	name, _ := callNameAndArgs(node)
	return name == "spearman_corr"
}

// containsWindowCall reports whether the AST subtree contains any window-producing call.
func containsWindowCall(node domain.Node) bool {
	switch n := node.(type) {
	case domain.CallNode:
		if isWindowCallName(n.Name) {
			return true
		}
		for _, arg := range n.Args {
			if containsWindowCall(arg) {
				return true
			}
		}
	case *domain.CallNode:
		if isWindowCallName(n.Name) {
			return true
		}
		for _, arg := range n.Args {
			if containsWindowCall(arg) {
				return true
			}
		}
	}
	return false
}

func isWindowCallName(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "ma", "sum", "ref", "ts_count", "pearson_corr", "rank", "top", "count", "limit10", "limit20", "limit30":
		return true
	}
	return false
}
