// Command metrics_summary consumes the tab-separated report written by
// TestE2E_HTTPAPI_MetricsPipeline_WriteFactorSignalResultsReport and prints,
// for a given trade_date, per-metric distribution stats: count, NaN count,
// min / p25 / p50 / p75 / max, mean, stdev. Signals get True count + coverage.
//
// Usage: metrics_summary <report_file> <trade_date>
package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: metrics_summary <report_file> <trade_date>")
		os.Exit(2)
	}
	reportPath := os.Args[1]
	tradeDate := os.Args[2]

	f, err := os.Open(reportPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	factorValues := map[string][]float64{} // metric -> values
	factorNaN := map[string]int{}
	signalTrue := map[string]int{}
	signalTotal := map[string]int{}

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	section := ""
	for scanner.Scan() {
		line := scanner.Text()
		switch line {
		case "Factors", "Signals":
			section = line
			continue
		}
		if strings.HasPrefix(line, "metric_id\t") {
			continue
		}
		cols := strings.Split(line, "\t")
		switch section {
		case "Factors":
			if len(cols) != 4 {
				continue
			}
			if cols[1] != tradeDate {
				continue
			}
			metric := cols[0]
			v, err := strconv.ParseFloat(cols[3], 64)
			if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
				factorNaN[metric]++
				continue
			}
			factorValues[metric] = append(factorValues[metric], v)
		case "Signals":
			if len(cols) < 4 {
				continue
			}
			if cols[1] != tradeDate {
				continue
			}
			metric := cols[0]
			signalTotal[metric]++
			if strings.EqualFold(cols[3], "true") {
				signalTrue[metric]++
			}
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "scan: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("=== Factor distribution @ %s ===\n", tradeDate)
	fmt.Printf("%-24s %8s %6s %12s %12s %12s %12s %12s %12s %12s\n",
		"metric", "count", "nan", "min", "p25", "p50", "p75", "max", "mean", "stdev")
	metrics := sortedKeys(factorValues)
	for _, metric := range metrics {
		v := factorValues[metric]
		sort.Float64s(v)
		n := len(v)
		if n == 0 {
			fmt.Printf("%-24s %8d %6d %12s\n", metric, 0, factorNaN[metric], "(empty)")
			continue
		}
		mean, stdev := meanStdev(v)
		fmt.Printf("%-24s %8d %6d %12.6g %12.6g %12.6g %12.6g %12.6g %12.6g %12.6g\n",
			metric, n, factorNaN[metric],
			v[0], quantile(v, 0.25), quantile(v, 0.5), quantile(v, 0.75), v[n-1],
			mean, stdev)
	}

	fmt.Printf("\n=== Signal coverage @ %s ===\n", tradeDate)
	fmt.Printf("%-24s %8s %8s %10s\n", "metric", "total", "true", "coverage")
	for _, metric := range sortedKeys(signalTotal) {
		total := signalTotal[metric]
		tr := signalTrue[metric]
		cov := 0.0
		if total > 0 {
			cov = float64(tr) / float64(total)
		}
		fmt.Printf("%-24s %8d %8d %9.2f%%\n", metric, total, tr, cov*100)
	}
}

func sortedKeys[V any](m map[string]V) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func quantile(sorted []float64, q float64) float64 {
	n := len(sorted)
	if n == 0 {
		return math.NaN()
	}
	if n == 1 {
		return sorted[0]
	}
	pos := q * float64(n-1)
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	if lo == hi {
		return sorted[lo]
	}
	return sorted[lo] + (sorted[hi]-sorted[lo])*(pos-float64(lo))
}

func meanStdev(v []float64) (float64, float64) {
	n := float64(len(v))
	sum := 0.0
	for _, x := range v {
		sum += x
	}
	mean := sum / n
	ss := 0.0
	for _, x := range v {
		d := x - mean
		ss += d * d
	}
	if n < 2 {
		return mean, 0
	}
	return mean, math.Sqrt(ss / (n - 1))
}
