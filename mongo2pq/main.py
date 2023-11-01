from asyncio import Runner
from pathlib import Path
from typing import List
from os import get_terminal_size

from pymongo.errors import ServerSelectionTimeoutError

from mongo2pq.mongo import connect_mongo
from mongo2pq.config import parse_config
from mongo2pq.schema import create_schema, load_schema_from_file
from mongo2pq.exceptions import SchemaParseError
from mongo2pq.extract_load import extract_load_collection


def main(
    uri: str, *,
    db: str | None = None, collections: List[str] | None = None,
    outdir: Path = Path('.'),
    schema_paths: List[Path] | None = None, samples: int = 20000,
    partition_key: str | None = None, config_file: Path | None = None,
    cli: bool = True
) -> int:
    with Runner() as runner:
        try:
            client = runner.run(connect_mongo(uri))
        except ServerSelectionTimeoutError as err:
            print("Error: Could not connect to MongoDB")
            print(f"Client responded with: {str(err)}")
            return 1

        if not db:
            db_list = runner.run(client.list_database_names())
            db = db_list[0]
        if not collections:
            collections = runner.run(client[db].list_collection_names())

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
                    schema = runner.run(create_schema(client[db][collection], samples,
                                                      config=config.get('schema'), progress_bar=cli))
                    schema.dump_to_file(destination=outdir)
            else:
                schema = runner.run(create_schema(client[db][collection], samples,
                                                  config=config.get('schema'), progress_bar=cli))
                schema.dump_to_file(destination=outdir)

            schemas[collection] = schema

        if cli:
            columns, _ = get_terminal_size()
            print(" Moving Collections to Parquet ".center(columns, '='))

        for collection in collections:
            if cli:
                print(f"> Collection '{collection}'")
            runner.run(
                extract_load_collection(client[db][collection], schemas[collection],
                                        outdir=outdir, partition_key=partition_key,
                                        progress_bar=cli)
            )

        return 0
