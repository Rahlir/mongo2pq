# mongo2pq

Quick and dirty script that migrates MongoDB database to parquet files.

I wrote this after being unsatisfied with the current options for exporting
larger MongoDB collections: There is the `mongoexport` command from database
tools, but that tends to be unstable and has the tendency to fail for larger
collections. There are tools such as  Fivetran and Airbyte, but these are big,
heavy, monolithic programs (and, in the case of Fivetran, proprietary) that are
expensive to setup and manage. Moreover, they are not easy to connect to other
components you might have in your technology stack. Finally, I have stumbled
upon [dlt](https://github.com/dlt-hub/dlt). I very much liked the philosophy
and approach of this tool. However, it didn't work well for me in its current
form. Its flexibility (understandably) requires more complexity and it uses
middle layers during the extraction and loading process. This causes
sub-optimal performance for large collections.

Hence, I wrote a simple and efficient extractor and loader that is just focused
on MongoDB and Apache Parquet.

> [!NOTE]
> In its current form, the script is written to serve my very specific need for
> extracting telemetry data. There is no guarantee it will perform optimally in
> all use cases and its flexibility is limited. I have made it public in case
> someone has similar (fairly niche) need for quick and simple tool to deal
> with unstructured MongoDB data.

## Architecture

This script simply utilizes the asynchronous python driver for MongoDB,
[motor](https://motor.readthedocs.io/en/stable/), and the python implementation
of [Apache Arrow](https://arrow.apache.org/). The main idea is that there is no
middle layer: we load a chunk of data using motor from a particular collection
and immediately dump it in a corresponding parquet file. This lowers the demand
for both memory and storage even for large collections. Moreover, utilizing the
`asyncio` python library allows us to speed the process up considerably since the
biggest bottleneck in an EL process tends to be the transfer of data from the
database. With `asyncio`, we can request multiple chunks at once and process them
as they arrive.

## Installation

`mongo2pq` supports Python 3.11+. It is hosted on [PyPi](https://pypi.org/project/mongo2pq/),
so you can install it with `pip`:
```bash
pip install mongo2pq
```
This will add the command `mongo2pq` to your `$PATH`.

To install the script in an isolated virtual environment, use
[pipx](https://pipx.pypa.io/stable/):
```bash
pipx install mongo2pq
```

The dependencies of the project are handled with
[poetry](https://python-poetry.org/). Hence, for a developmental installation,
you can also use `poetry`. Make sure you have `poetry` available on your system
and then clone the repository and run `poetry install`. This will also install
the developmental dependencies which include `ipython` (which is quite heavy).
To install without dev dependencies, run `poetry install --without dev`.

## Usage

A straightforward run would be started with
```bash
mongo2pq -u <URI> -o <OUTDIR> -d <DB> -c <COLLECTION1> <COLLECTION2>
```
where `URI` is the connection string for the MongoDB instance (such as
`mongodb://user:passwd@ip:port/opts`), `OUTDIR` is where the script will save
the parquet files and the `yaml` schema files, and `DB` and `COLLECTION1`
`COLLECTION2` are the database and its collections to download. See more
options with `mongo2pq --help`.

You can also specify the `URI` with the environmental variable `MONGODB_URI`.

Without specifying collections, all collections in a database will be exported,
and without specifying database, all databases and its collections will be
exported (note that to list DBs you need root access to MongoDB).

The script will infer the schema of the database for you which is done by
sampling the collections. For large collections you might need a large sample
set (keys missing from the schema during export are dropped). This inference
can take significant time since it relies on the MongoDB `sample` operation
which is expensive. In case export needs to be repeated or there is an error
during the export, the schema files are saved to `yaml` and can be used next
time when running the script.

### Partitioning

Parquet allows you to have a partitioning for your data. This is supported with
this script using the option `-p` or `--partition` which takes an argument for
the partition key. The, the parquet files will be stored with the structure
```
collection_name.parquet
|
|-partition_key=value1
|  |
|  |-data.parquet
|
|-partition_key=value2
|  |
|  |-data.parquet
|
...
```

Taking advantage of partitioning in parquet has many known benefits, but the
added bonus from the perspective of this script is that it allows it to fully
take advantage of `asyncio` since each partition dataset can be extracted
independently. With at least few partitions, the extraction performance
significantly increases.

### Configuration

Sometimes, you might need to make small modification to the original schema.
This can be done with a config file, which also has a `yaml` format. As of now,
the config file accepts only config for the schema. To specify the schema
config, the root keyword is `schema`, the next keyword is the collection name
for which the config applies. Finally, you specify all the transformations
to item fields with yaml lists. A sample short config would be:
```yaml
schema:
  telemetry_data:
    - type: retype_regex
      fieldname: (?<!string)_id
      fieldtype: string
    - type: retype_contains
      fieldname: flap_orientation
      fieldtype: float
    - type: rename_regex_upper
      oldname: (\S+)@(\S+)
      newname: \2__\1
      upper: [2]
```

The config specified in the list part of the `yaml` contains the schema
transformations. Right now, there are two types of transformations supported:
_retype_ which changes a type of a field from an inferred type (or type in an
input schema) and _rename_ which renames item key from the key in the database.

The retype can be one of `retype_regex`, where `fieldname` is a regex to search
in a key, `retype_contains` which searches for the sub-string `fieldname` in a
key, and `retype_equals` which retypes a specific key.

The rename can be one of `rename_regex`, which takes python regex in `oldname`
field usually with some match groups and `newname` which might reference the
groups from the `oldname`, and `rename_regex_upper` which has an additional key
`upper` that allows you to transform a match group to upper case.

For more details checkout the fully documented [sample config](sample_config.yaml).
