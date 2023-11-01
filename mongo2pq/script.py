from argparse import ArgumentParser, Namespace
import asyncio
from os import environ
from os import get_terminal_size
from pathlib import Path
from sys import exit
from typing import List

from pymongo.errors import ServerSelectionTimeoutError

from mongo2pq import PROJECT_NAME
from mongo2pq.config import parse_config
from mongo2pq.exceptions import SchemaParseError
from mongo2pq.extract_load import extract_load_collection
from mongo2pq.mongo import connect_mongo
from mongo2pq.schema import create_schema, load_schema_from_file


def parse_args() -> Namespace | None:
    parser = ArgumentParser(
        prog=PROJECT_NAME,
        description="Command line utility to migrate MongoDB to Apache Parquet file"
    )
    parser.add_argument(
        '-u', '--uri',
        default=environ.get('MONGODB_URI'),
        help="URI for the MongoDB instance. Can also be set with env var MONGODB_URI"
    )
    parser.add_argument(
        '-o', '--outdir',
        default='./', type=Path,
        help="Destination for the output parquet files and schema files"
    )
    parser.add_argument(
        '-s', '--samples',
        type=int, default=20000,
        help="Number of samples to use when building schema"
    )
    parser.add_argument(
        '-d', '--db',
        help="Database to move to parquet. If not specified, the first database found is moved"
    )
    parser.add_argument(
        '-c', '--collections',
        nargs='*',
        help="Collections to move to parquet. Each will have its own output file. "
        "If not specified, the script will try to get all collections in the db (root permissions needed)"
    )
    parser.add_argument(
        '-p', '--partition',
        help="Partition key for partitioning output parquet file"
    )
    parser.add_argument(
        '-S', '--schemas',
        type=Path, nargs='*',
        help="YAML files defining schema for the collections. "
        "If not provided, schema will be inferred by sampling from the collection. "
        "The name without the extension is considered as name of the collection"
    )
    parser.add_argument(
        '-C', '--config',
        type=Path, default=environ.get('MONGO2PQ_CONFIG'),
        help="YAML config with schema overrides"
    )

    parsed = parser.parse_args()
    if not parsed.uri:
        print("ERROR: URI for MongoDB not passed. Use --uri option or MONGODB_URI env variable.")
        return None

    if parsed.schemas:
        checked_schemas = []
        for schema in parsed.schemas:
            if schema.is_file():
                checked_schemas.append(schema)
            else:
                print(f"WARNING: Schema '{schema!s}' doesn't exist. Schema will be inferred")
        parsed.schemas = checked_schemas

    if parsed.config:
        if not parsed.config.is_file():
            print(f"WARNING: Config '{parsed.config!s}' doesn't exist. No config will be used")
            parsed.config = None

    return parsed


async def main(
    uri: str, *,
    db: str | None = None, collections: List[str] | None = None,
    outdir: Path = Path('.'),
    schema_paths: List[Path] | None = None, samples: int = 20000,
    partition_key: str | None = None, config_file: Path | None = None,
    cli: bool = True
) -> int:
    try:
        client = await connect_mongo(uri)
    except ServerSelectionTimeoutError as err:
        print("Error: Could not connect to MongoDB")
        print(f"Client responded with: {str(err)}")
        return 1

    if not db:
        db_list = await client.list_database_names()
        db = db_list[0]
    if not collections:
        collections = await client[db].list_collection_names()

    assert collections

    config = {}
    if config_file:
        config = parse_config(config_file)

    if cli:
        columns, _ = get_terminal_size()
        print(" Creating Schemas ".center(columns, '='))

    if schema_paths:
        schema_paths_dict = {schema.stem: schema for schema in schema_paths}
    else:
        schema_paths_dict = {}

    schemas = {}
    for collection in collections:
        if cli:
            print(f"> For collection '{collection}'")

        if schema_path := schema_paths_dict.get(collection):
            print(f"loading schema from {schema_path!s}... ", end='')
            try:
                schema = load_schema_from_file(schema_path, config=config.get('schema'))
                print("success")
            except SchemaParseError as err:
                print("failed with message:")
                print(str(err))
                print("Inferring from sample instead...")
                schema = await create_schema(client[db][collection], samples, progress_bar=cli)
                schema.dump_to_file(destination=outdir)
        else:
            schema = await create_schema(client[db][collection], samples,
                                         config=config.get('schema'), progress_bar=cli)
            schema.dump_to_file(destination=outdir)
        schemas[collection] = schema

    if cli:
        columns, _ = get_terminal_size()
        print(" Moving Collections to Parquet ".center(columns, '='))

    for collection in collections:
        if cli:
            print(f"> Collection '{collection}'")
        await extract_load_collection(client[db][collection], schemas[collection],
                                      outdir=outdir, partition_key=partition_key,
                                      progress_bar=cli)

    return 0


if __name__ == "__main__":
    args = parse_args()
    if not args:
        exit(1)

    exit(asyncio.run(
        main(args.uri, db=args.db, collections=args.collections,
             outdir=args.outdir, schema_paths=args.schemas, samples=args.samples,
             partition_key=args.partition, config_file=args.config)
    ))
