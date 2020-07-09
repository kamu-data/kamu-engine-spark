# Apache Spark Engine

This the implementation of the `Engine` contract of [Open Data Fabric](http://opendatafabric.org/) using the [Apache Spark](https://spark.apache.org/) data processing framework. It is currently in use in [kamu-cli](https://github.com/kamu-data/kamu-cli) data management tool.

## Features

- Spark engine currently provides the most rich SQL dialect for map/filter style transformations
- Integrates [GeoSpark](http://geospark.datasyslab.org/) to provide geo-spatial SQL functions
- It is used by [kamu-cli](https://github.com/kamu-data/kamu-cli) for ingesting data into Parquet
- It is used by [kamu-cli](https://github.com/kamu-data/kamu-cli) along with [Apache Livy](https://livy.apache.org/) to provide SQL queries functionality in the Jupyter notebooks

## Known Issues

- Takes a long time to start up which is hurting the user experience
- Does not support temporal table joins
  - You might be better off using [Flink-based engine](https://github.com/kamu-data/kamu-engine-flink) for joining and aggregating event streams
- TODO