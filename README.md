# Prism: Redshift Spectrum Streaming Loader

Prism is a scalable, fast Redshift Spectrum Streaming Loader.
This software is developped at Cookpad in working time.

## Prerequisites
- OpenJDK (Temurin) 11

## Build
```
% ./gradlew build
```

## Components
- stream/: Prism Stream converts JSONL S3 objects to Parquet objects.
- merge/: Prism Merge merges small Parquet objects into large one.
- batch/: Prism Batch Jobs update Glue Catalog to reflect latest partition info.
- gc/: Prism GC deletes unused merged objects.

In addition, Docker base image definition for Prism is here: https://github.com/cookpad/prism-base-image

## Setup
TBD

## License
MIT license.  See LICENSE file for details.

## Authors
- Hidekazu Kobayashi @koba789 (original author)
- Minero Aoki @aamine (current maintainer)
