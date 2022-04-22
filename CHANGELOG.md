# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
