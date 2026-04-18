module github.com/xargin/opentrade/push

go 1.26

require (
	github.com/coder/websocket v1.8.14
	github.com/twmb/franz-go v1.18.0
	github.com/xargin/opentrade/api v0.0.0-00010101000000-000000000000
	github.com/xargin/opentrade/pkg v0.0.0-00010101000000-000000000000
	go.uber.org/zap v1.27.0
	google.golang.org/protobuf v1.34.2
)

require (
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.9.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
)

replace (
	github.com/xargin/opentrade/api => ../api
	github.com/xargin/opentrade/pkg => ../pkg
)
