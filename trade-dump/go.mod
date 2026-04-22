module github.com/xargin/opentrade/trade-dump

go 1.26

require (
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/go-sql-driver/mysql v1.8.1
	github.com/twmb/franz-go v1.18.0
	github.com/xargin/opentrade/api v0.0.0-00010101000000-000000000000
	github.com/xargin/opentrade/counter v0.0.0-00010101000000-000000000000
	github.com/xargin/opentrade/pkg v0.0.0-00010101000000-000000000000
	go.uber.org/zap v1.27.0
	google.golang.org/protobuf v1.36.10
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/aws/aws-sdk-go-v2 v1.41.6 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.9 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.22 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.22 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.23 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.14 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.22 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.22 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.99.1 // indirect
	github.com/aws/smithy-go v1.25.0 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.9.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
)

replace (
	github.com/xargin/opentrade/api => ../api
	github.com/xargin/opentrade/counter => ../counter
	github.com/xargin/opentrade/pkg => ../pkg
)
