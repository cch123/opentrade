module github.com/xargin/opentrade/match

go 1.26

require github.com/xargin/opentrade/pkg v0.0.0-00010101000000-000000000000

require (
	github.com/google/btree v1.1.3 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
)

replace (
	github.com/xargin/opentrade/api => ../api
	github.com/xargin/opentrade/pkg => ../pkg
)
