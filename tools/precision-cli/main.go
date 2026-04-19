// Command opentrade-cli — ops CLI for OpenTrade. Currently hosts the
// "precision recommend" subcommand (ADR-0053 M2).
//
// Usage:
//
//	opentrade-cli precision recommend \
//	    --symbol BTC-USDT --base BTC --quote USDT --price 50000 \
//	    [--min-quote-amount 5] [--quote-step 0.01]
//
// Prints a SymbolConfig JSON ready to be pasted into
// `curl -X PUT /admin/symbols/BTC-USDT -d @-`. Ops is expected to review
// MaxQty / Shard before applying — the recommender defaults to broad
// values which are safe but non-optimal.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/xargin/opentrade/pkg/dec"
)

func main() {
	if len(os.Args) < 2 {
		usageAndExit(1)
	}
	switch os.Args[1] {
	case "precision":
		precisionCmd(os.Args[2:])
	case "-h", "--help", "help":
		usageAndExit(0)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		usageAndExit(2)
	}
}

func precisionCmd(args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: opentrade-cli precision <recommend> [flags]")
		os.Exit(2)
	}
	switch args[0] {
	case "recommend":
		precisionRecommendCmd(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "unknown precision subcommand %q\n", args[0])
		os.Exit(2)
	}
}

func precisionRecommendCmd(args []string) {
	fs := flag.NewFlagSet("recommend", flag.ExitOnError)
	symbol := fs.String("symbol", "", "symbol, e.g. BTC-USDT (required)")
	base := fs.String("base", "", "base asset, e.g. BTC (required)")
	quote := fs.String("quote", "", "quote asset, e.g. USDT (required)")
	priceStr := fs.String("price", "", "typical / median spot price, decimal (required, > 0)")
	minAmtStr := fs.String("min-quote-amount", "5", "floor order amount in quote units (default 5)")
	quoteStepStr := fs.String("quote-step", "0.01", "QuoteStepSize (default 0.01 for USDT)")
	shard := fs.String("shard", "match-0", "match shard id placeholder (override to fit cluster)")
	pretty := fs.Bool("pretty", true, "pretty-print JSON (default true)")
	_ = fs.Parse(args)

	fatalIfEmpty := func(name, v string) {
		if v == "" {
			fmt.Fprintf(os.Stderr, "--%s is required\n", name)
			os.Exit(2)
		}
	}
	fatalIfEmpty("symbol", *symbol)
	fatalIfEmpty("base", *base)
	fatalIfEmpty("quote", *quote)
	fatalIfEmpty("price", *priceStr)

	price, err := dec.Parse(*priceStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid --price: %v\n", err)
		os.Exit(2)
	}
	minAmt, err := dec.Parse(*minAmtStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid --min-quote-amount: %v\n", err)
		os.Exit(2)
	}
	quoteStep, err := dec.Parse(*quoteStepStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid --quote-step: %v\n", err)
		os.Exit(2)
	}

	cfg, err := Recommend(RecommendInput{
		Symbol:         *symbol,
		BaseAsset:      *base,
		QuoteAsset:     *quote,
		Price:          price,
		MinQuoteAmount: minAmt,
		QuoteStepSize:  quoteStep,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "recommend:", err)
		os.Exit(1)
	}
	cfg.Shard = *shard

	var out []byte
	if *pretty {
		out, err = json.MarshalIndent(cfg, "", "  ")
	} else {
		out, err = json.Marshal(cfg)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "marshal:", err)
		os.Exit(1)
	}
	fmt.Println(string(out))

	// Operator hint (stderr so stdout stays clean JSON for piping).
	fmt.Fprintf(os.Stderr, "\n# review MaxQty / MarketMaxQty / Shard before applying:\n")
	fmt.Fprintf(os.Stderr, "# curl -X PUT http://admin-gateway:8090/admin/symbols/%s -H 'X-API-Key: ...' -d @-\n", *symbol)
}

func usageAndExit(code int) {
	fmt.Fprintln(os.Stderr, `opentrade-cli — OpenTrade ops CLI

Subcommands:
  precision recommend   推荐一个 symbol 的精度配置（ADR-0053 M2）

Examples:
  opentrade-cli precision recommend \
      --symbol BTC-USDT --base BTC --quote USDT --price 50000`)
	os.Exit(code)
}
