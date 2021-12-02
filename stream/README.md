# Prism Stream

## Overview

- `Main`
  - `events.SqsEventDispatcher`
    - polls event messages from the SQS
    - `events.StagingObjectDispatcher`
      - queries prism_staging_objects table
      - issues or obtain object ID
      - `ParquetConverter`
        - is called for each input object
        - `jsonl.JsonlReader` & `jsonl.JsonlRecordReader` (in shared)
          - reads the input object as `Record`s
        - `PartitionedWriter`
          - writes them to parquets
          - and uploads it as S3 objects
    - delete the message from the SQS
