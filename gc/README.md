# Prism GC

Ruby application to delete unused S3 objects on the Prism bucket.

Prism continously rebuilds merged objects again and again when a new small object is coming,
to keep a partition data up to date.  But when Prism updates a merged partition, old merged
objects are left.  So there are so many old, unused merged objects on the Prism bucket.
This application collects and deletes such unused objects.

Prism does not update merged objects in the partitions older than 14 days, applying GC to
partitions which is 15 or 16 days older is safe and recommended.

## Usage

```
% PRISM_BUCKET_NAME=prism-example-bucket ./bin/garbage-collect
```

Options:
```
Usage: garbage-collect [options]
        --prism-ds=NAME              Prism meta data data source (default: prism)
        --list-tables                Shows target prism tables and quit.
        --table=SCHEMA_TABLE         Processes only this table.
        --list-objects               Shows garbage object list and quit.
        --partition-expr=EXPR        AWS Glue Catalog partition expression to filter target partitions.  e.g. "dt = '2021-03-15'"
    -e, --environment=ENV            Bricolage execution environment. (default: development)
    -C, --home=PATH                  Bricolage home directory. (default: /Users/minero-aoki/c/prism.ghe/gc)
        --help                       Prints this message and quit.
```

Environment:

- PRISM_BUCKET_NAME: Prism S3 bucket name.
- PRISM_DB_HOST: Prism metadata DB host.
- PRISM_DB_PORT: Prism metadata DB port.
- PRISM_DB_DATABASE: Prism metadata DB database.
- PRISM_DB_USERNAME: Prism metadata DB username.
- PRISM_DB_PASSWORD: Prism metadata DB password
- AWS_DEFAULT_REGION: S3 bucket region.
- BRICOLAGE_ENV: Active configuration profile.  Default value is `development`.
- TZ: Timezone to be used to decide "today".
