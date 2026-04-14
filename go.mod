module github.com/realdatadriven/etlx

go 1.26.1

require (
	github.com/BurntSushi/toml v1.6.0
	github.com/Masterminds/sprig/v3 v3.3.0
	//github.com/alexbrainman/odbc v0.0.0-20250601004241-49e6b2bc0cf0
	github.com/aws/aws-sdk-go-v2 v1.41.5
	github.com/aws/aws-sdk-go-v2/config v1.32.14
	github.com/aws/aws-sdk-go-v2/credentials v1.19.14
	github.com/aws/aws-sdk-go-v2/service/s3 v1.99.0
	github.com/duckdb/duckdb-go/v2 v2.10502.0
	github.com/goccy/go-yaml v1.19.2
	github.com/jlaffaye/ftp v0.2.0
	github.com/jmoiron/sqlx v1.4.0
	github.com/joho/godotenv v1.5.1
	github.com/lib/pq v1.12.3
	github.com/mattn/go-sqlite3 v1.14.42
	github.com/microsoft/go-mssqldb v1.9.8
	github.com/pkg/sftp v1.13.10
	github.com/xuri/excelize/v2 v2.10.1
	github.com/yuin/goldmark v1.8.2
	golang.org/x/crypto v0.50.0
	golang.org/x/text v0.36.0
	gopkg.in/yaml.v3 v3.0.1
)

require go.abhg.dev/goldmark/frontmatter v0.3.0

require (
	github.com/aws/aws-sdk-go-v2/service/signin v1.0.9 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/darwin-amd64 v0.10502.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/darwin-arm64 v0.10502.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/linux-amd64 v0.10502.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/linux-arm64 v0.10502.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/windows-amd64 v0.10502.0 // indirect
)

require (
	dario.cat/mergo v1.0.2 // indirect
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/semver/v3 v3.4.0 // indirect
	github.com/apache/arrow-go/v18 v18.5.2 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.8 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.21 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.21 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.21 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.6 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.22 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.21 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.21 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.19 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.41.10 // indirect
	github.com/aws/smithy-go v1.24.3 // indirect
	github.com/duckdb/duckdb-go-bindings v0.10502.0 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/huandu/xstrings v1.5.0 // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/kr/fs v0.1.0 // indirect
	//	github.com/duckdb/duckdb-go/arrowmapping v0.0.21 // indirect
	//	github.com/duckdb/duckdb-go/mapping v0.0.21 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/richardlehane/mscfb v1.0.6 // indirect
	github.com/richardlehane/msoleps v1.0.6 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/spf13/cast v1.10.0 // indirect
	github.com/tiendc/go-deepcopy v1.7.2 // indirect
	github.com/xuri/efp v0.0.1 // indirect
	github.com/xuri/nfp v0.0.2-0.20250530014748-2ddeb826f9a9 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	golang.org/x/exp v0.0.0-20260312153236-7ab1446f8b90 // indirect
	golang.org/x/mod v0.35.0 // indirect
	golang.org/x/net v0.53.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
	golang.org/x/telemetry v0.0.0-20260409153401-be6f6cb8b1fa // indirect
	golang.org/x/tools v0.43.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
)
