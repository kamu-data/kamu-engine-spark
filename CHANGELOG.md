# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Changed
- Updated Rust toolchain and minor dependencies

## [0.23.0] - 2024-02-11
### Changed
- Upgraded Spark version to `3.5.0`
- Enabled [ANSI mode](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html) by default
### Added
- Building a multi-platform image for `arm64` architectures

## [0.22.1] - 2024-01-11
### Fixed
- Coalesce output of `RawQuery` to avoid a crash on multiple partitions

## [0.22.0] - 2024-01-10
### Changed
- Upgraded to unified ODF schema

## [0.21.1] - 2024-01-03
### Fixed
- Escape special symbols in input view names

## [0.21.0] - 2024-01-03
### Removed
- Removed deprecated Spark-only ingest
### Changed
- Upgraded to new ODF engine protocol

## [0.20.0] - 2024-01-03
### Changed
- Upgraded to new ODF schemas

## [0.19.1] - 2023-07-14
### Fixed
- Transform engine will now read the list of input files as specified in the request instead of reading the entire directory of Parquet files

## [0.19.0] - 2023-05-29
### Changed
- Configuring Spark to write timestamps in `Timestamp(MILLIS,true)` logical Parquet format instead of `int96` that is considered deprecated and causing compatibility issues

## [0.18.0] - 2023-05-05
### Changed
- Removed ingest checkpoints
- Unique naming of temporary output directories

## [0.17.0] - 2022-07-22
### Added
- Support for reading Apache Parquet files directly

## [0.16.0] - 2022-04-22
### Changed
- Updated to latest ODF schemas
- Adapter will now tar/untar checkpoints to have them managed as files

## [0.15.1] - 2022-03-16
### Fixed
- Ignore `offset` column in merge strategies

## [0.11.2] - 2021-09-03
### Fixed
- Handle Shapefiles with subdirectories 

## [0.11.1] - 2021-08-17
### Fixed
- Helpful error messages for invalid event time column upon ingest

## [0.11.0] - 2021-08-16
### Changed
- Upgraded to Spark 3.1.2 and latest Sedona

## [0.9.1] - 2020-11-14
### Fixed
- Fixed data hashing when dataframe is empty

## [0.9.0] - 2020-10-31
### Changed
- Upgraded to Spark 3.0.1 and latest Sedona (GeoSpark)

## [0.8.3] - 2020-10-30
### Changed
- Upgraded to ODF manifests

## [0.8.0] - 2020-07-12
### Changed
- Upgraded to ODF manifests

## [0.7.0] - 2020-06-28
### Added
- Windows support improvements

## [0.6.0] - 2020-06-28
### Changed
- Minimizing use of Hadoop FS

## [0.5.0] - 2020-06-23
### Added
- Support watermarking in ingest and transform

## [0.4.0] - 2020-06-14
### Changed
- Further engine interface improvements
- Event time column is now required
- System time column is not considered when computing the hash
- Result files will be named using system time

## [0.3.0] - 2020-05-25
### Changed
- Ensuring data is sorted in Parquet files based on event time
- Naming output files using system time

## [0.2.0] - 2020-05-09
### Changed
- Major refactoring and combined with the ingress

## [0.1.0] - 2020-05-03
### Changed
- Separated engine build from `kamu-cli`
