<!-- markdownlint-disable MD022 -->
<!-- markdownlint-disable MD031 -->

# REMOTE_EXEC
```yaml
name: ...
runs_as: REMOTE_EXEC
timeout: 10_000
```

## 127.0.0.1
```yaml
name: local.test
host: 127.0.0.1
port: 22
user: ubuntu
key: $HOME/.ssh/id...
working_dir: $HOME/etlx
upload_files: 
  - {source: .env, dest: .env }
download_files: 
  - {source: temp.db , dest: ./database/temp.db  }
commands:
  - curl https://realdatadriven.github.io/etlxdocs/install.sh | sh 
  - etlx --config pipeline.md
```

## 127.0.0.2
```yaml
name: local.test2
host: 127.0.0.2
port: 22
user: ubuntu
key: $HOME/.ssh/id...
working_dir: $HOME/etlx
upload_files: 
  - {source: .env, dest: .env }
download_files: 
  - {source: temp.db , dest: ./database/temp.db  }
commands:
  - curl https://realdatadriven.github.io/etlxdocs/install.sh | sh 
  - etlx --config pipeline.md
```

```env .env
CONN=temp.db
```
