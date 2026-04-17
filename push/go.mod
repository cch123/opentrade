module github.com/xargin/opentrade/push

go 1.26

require github.com/xargin/opentrade/pkg v0.0.0-00010101000000-000000000000

require (
	go.uber.org/multierr v1.10.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
)

replace (
	github.com/xargin/opentrade/api => ../api
	github.com/xargin/opentrade/pkg => ../pkg
)
