# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
