# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **Breaking:** The `configs` package import path has been renamed to `config` to align the
  directory name with the package declaration.

  Update your imports from:
  ```go
  "github.com/Trendyol/go-dcp-cassandra/configs"
  ```
  to:
  ```go
  "github.com/Trendyol/go-dcp-cassandra/config"
  ```

- **Breaking:** The duplicate `mapper.go` in the root package has been removed. The
  `Map` and `SetCollectionTableMappings` functions are no longer exported from the root
  package. Use `connector.DefaultMapper` and `connector.SetCollectionTableMappings` directly,
  or rely on the automatic mapping via `CollectionTableMapping` config.
