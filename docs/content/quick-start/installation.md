+++
title = 'Installation'
weight = 21
draft = false
+++

### Installation

#### Option 1: Precompiled Binaries

Precompiled binaries for Linux, macOS, and Windows are available on the [releases page](https://github.com/realdatadriven/etlx/releases). Download the appropriate binary for your system and make it executable:

```bash
# Example for Linux or macOS
chmod +x etlx
./etlx --help
```

#### Option 2: Install via Go (as a library)

```bash
# Install ETLX
go get github.com/realdatadriven/etlx
```

#### Option 3: Clone Repo

```bash
git clone https://github.com/realdatadriven/etlx.git
cd etlx
```

And then:

```bash
go run cmd/main.go --config etl_config.md --date 2023-10-31
```

On Windows you may have build issues; in that case using the latest libduckdb from [duckdb/releases](https://github.com/duckdb/duckdb/releases) and building with `-tags=duckdb_use_lib` may help:

```bash
CGO_ENABLED=1 CGO_LDFLAGS="-L/path/to/libs" go run -tags=duckdb_use_lib main.go --config etl_config.md --date 2023-10-31
```
