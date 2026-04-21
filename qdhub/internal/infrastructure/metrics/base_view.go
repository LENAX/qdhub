package metrics

import (
	"fmt"
	"sort"
	"strings"
)

// BaseViewSpec captures all runtime-resolved inputs needed to materialise the
// base CTE shared by every DSL expression. It is produced by the SQLCompiler
// before lowering so that the CTE remains stable across stage CTEs.
type BaseViewSpec struct {
	StartDate          string
	EndDate            string
	DependsOnFactors   []string
	DependsOnSignals   []string
	DependsOnUniverses []string
	DailyBasicExists   bool
	MoneyflowExists    bool
	Frequency          string
}

// BaseViewResult is the output of BuildBaseView — a single CTE definition to be
// embedded in a WITH clause, together with its positional arguments.
type BaseViewResult struct {
	CTE  string
	Args []any
}

// BuildBaseView assembles the base CTE SQL plus its positional arguments.
//
// The CTE exposes a fixed set of columns, whose semantics must mirror the old
// Go evaluator exactly:
//   - entity_id, trade_date
//   - open, high, low, close, volume, amount, pre_close, change, pct_chg
//   - turnover_rate, pe_ttm, net_mf_amount (zero constants if table missing)
//   - market, name
//   - is_limit10 / is_limit20 / is_limit30 (BOOLEAN)
//   - one column per dependency metric (value for factor, bool for signal, true flag for universe)
//
// The filter clause matches evaluator.loadDataset: list_status='L', not ST, vol>0,
// trade_date within [start, end].
func BuildBaseView(spec BaseViewSpec) BaseViewResult {
	var b strings.Builder
	b.WriteString("base AS (\n")
	b.WriteString("\tSELECT\n")
	b.WriteString("\t\td.ts_code AS entity_id,\n")
	b.WriteString("\t\td.trade_date AS trade_date,\n")
	b.WriteString("\t\td.open, d.high, d.low, d.close,\n")
	b.WriteString("\t\td.vol AS volume, d.amount, d.pre_close, d.change, d.pct_chg,\n")
	b.WriteString("\t\tCOALESCE(s.market, '') AS market,\n")
	b.WriteString("\t\tCOALESCE(s.name, '') AS name,\n")
	if spec.DailyBasicExists {
		b.WriteString("\t\tCOALESCE(db.turnover_rate, 0.0) AS turnover_rate,\n")
		b.WriteString("\t\tCOALESCE(db.pe_ttm, db.pe, 0.0) AS pe_ttm,\n")
	} else {
		b.WriteString("\t\t0.0 AS turnover_rate,\n")
		b.WriteString("\t\t0.0 AS pe_ttm,\n")
	}
	if spec.MoneyflowExists {
		b.WriteString("\t\tCOALESCE(mf.net_mf_amount, 0.0) AS net_mf_amount,\n")
	} else {
		b.WriteString("\t\t0.0 AS net_mf_amount,\n")
	}
	b.WriteString("\t\tCASE WHEN COALESCE(s.market, '') NOT IN ('科创板', '北交所') AND COALESCE(d.pct_chg, 0.0) >= 9.8 THEN TRUE ELSE FALSE END AS is_limit10,\n")
	b.WriteString("\t\tCASE WHEN COALESCE(s.market, '') = '科创板' AND COALESCE(d.pct_chg, 0.0) >= 19.8 THEN TRUE ELSE FALSE END AS is_limit20,\n")
	b.WriteString("\t\tCASE WHEN COALESCE(s.market, '') = '北交所' AND COALESCE(d.pct_chg, 0.0) >= 29.8 THEN TRUE ELSE FALSE END AS is_limit30")

	// Dep columns come after the fixed ones; emit deterministic ordering.
	factors := dedupSorted(spec.DependsOnFactors)
	signals := dedupSorted(spec.DependsOnSignals)
	universes := dedupSorted(spec.DependsOnUniverses)
	for _, id := range factors {
		b.WriteString(",\n\t\t")
		b.WriteString(fmt.Sprintf("dep_factor_%s.value AS %s", safeAlias(id), quoteColumn(id)))
	}
	for _, id := range signals {
		b.WriteString(",\n\t\t")
		b.WriteString(fmt.Sprintf("dep_signal_%s.bool_value AS %s", safeAlias(id), quoteColumn(id)))
	}
	for _, id := range universes {
		b.WriteString(",\n\t\t")
		b.WriteString(fmt.Sprintf("CASE WHEN dep_universe_%s.entity_id IS NOT NULL THEN TRUE ELSE FALSE END AS %s", safeAlias(id), quoteColumn(id)))
	}

	b.WriteString("\n\tFROM daily d\n")
	b.WriteString("\tJOIN stock_basic s ON s.ts_code = d.ts_code\n")
	if spec.DailyBasicExists {
		b.WriteString("\tLEFT JOIN daily_basic db ON db.ts_code = d.ts_code AND db.trade_date = d.trade_date\n")
	}
	if spec.MoneyflowExists {
		b.WriteString("\tLEFT JOIN moneyflow mf ON mf.ts_code = d.ts_code AND mf.trade_date = d.trade_date\n")
	}
	for _, id := range factors {
		b.WriteString(fmt.Sprintf("\tLEFT JOIN factor_value dep_factor_%s ON dep_factor_%s.metric_id = '%s' AND dep_factor_%s.entity_id = d.ts_code AND dep_factor_%s.trade_date = d.trade_date\n",
			safeAlias(id), safeAlias(id), escapeSQLLiteral(id), safeAlias(id), safeAlias(id)))
	}
	for _, id := range signals {
		b.WriteString(fmt.Sprintf("\tLEFT JOIN signal_value dep_signal_%s ON dep_signal_%s.metric_id = '%s' AND dep_signal_%s.entity_id = d.ts_code AND dep_signal_%s.trade_date = d.trade_date\n",
			safeAlias(id), safeAlias(id), escapeSQLLiteral(id), safeAlias(id), safeAlias(id)))
	}
	for _, id := range universes {
		b.WriteString(fmt.Sprintf("\tLEFT JOIN universe_membership dep_universe_%s ON dep_universe_%s.universe_id = '%s' AND dep_universe_%s.entity_id = d.ts_code AND dep_universe_%s.trade_date = d.trade_date\n",
			safeAlias(id), safeAlias(id), escapeSQLLiteral(id), safeAlias(id), safeAlias(id)))
	}
	b.WriteString("\tWHERE d.trade_date >= ? AND d.trade_date <= ?\n")
	b.WriteString("\t\tAND COALESCE(s.list_status, 'L') = 'L'\n")
	b.WriteString("\t\tAND NOT (TRIM(COALESCE(s.name, '')) LIKE 'ST%' OR TRIM(COALESCE(s.name, '')) LIKE '*ST%')\n")
	b.WriteString("\t\tAND COALESCE(d.vol, 0) > 0\n")
	b.WriteString(")")

	return BaseViewResult{
		CTE:  b.String(),
		Args: []any{spec.StartDate, spec.EndDate},
	}
}

// dedupSorted returns a deduplicated, lexicographically sorted slice.
func dedupSorted(ids []string) []string {
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		set[id] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// safeAlias converts a metric_id into a SQL-safe alias stem. Metric IDs are
// already constrained by metricIDPattern so this is a no-op for the happy path;
// we still strip anything non-alphanumeric/_ as belt-and-braces.
func safeAlias(id string) string {
	var b strings.Builder
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}

// quoteColumn wraps an identifier in double quotes for use as a SELECT column alias.
func quoteColumn(id string) string {
	return `"` + strings.ReplaceAll(id, `"`, `""`) + `"`
}

// escapeSQLLiteral escapes single quotes in a string literal for embedding into SQL.
func escapeSQLLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
