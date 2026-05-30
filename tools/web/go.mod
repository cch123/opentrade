module github.com/xargin/opentrade/tools/web

go 1.26

require (
	connectrpc.com/connect v1.19.2
	github.com/coder/websocket v1.8.14
	github.com/xargin/opentrade/api v0.0.0-00010101000000-000000000000
	github.com/xargin/opentrade/pkg v0.0.0-00010101000000-000000000000
)

require (
	golang.org/x/net v0.51.0 // indirect
	golang.org/x/text v0.35.0 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
)

replace (
	github.com/xargin/opentrade/api => ../../api
	github.com/xargin/opentrade/pkg => ../../pkg
)
