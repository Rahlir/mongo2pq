import asyncio
from argparse import ArgumentParser, Namespace
from os import environ
from sys import exit
from typing import Any, Mapping, List
from pathlib import Path
from os import get_terminal_size

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo.errors import ServerSelectionTimeoutError
from tqdm import tqdm
from bson import BSON
from pyarrow.parquet import ParquetWriter

from mongo2pq import PROJECT_NAME
from mongo2pq.exceptions import ConfigParseError, SchemaParseError
from mongo2pq.schema import create_schema, load_schema_from_file, Schema, parse_schema_config


async def generate_partitions(collection: AsyncIOMotorCollection,
                              partition_key: str) -> List[Any]:
    cur = collection.aggregate([{'$group': {'_id': f"${partition_key}"}}])
    partitions = []
    async for doc in cur:
        partitions.append(doc['_id'])
    return partitions


async def connect_mongo(uri: str, check_connection: bool = True) -> AsyncIOMotorClient:
    if check_connection:
        await AsyncIOMotorClient(uri, serverSelectionTimeoutMS=1000).server_info()
    return AsyncIOMotorClient(uri)


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


def parse_config(config: Path) -> Mapping[str, Any]:
    import yaml
    parsers = {'schema': parse_schema_config}

    parsed_config = {}
    with config.open(mode='r') as c_file:
        parsed_raw = yaml.load(c_file, Loader=yaml.CLoader)
        if not isinstance(parsed_raw, dict):
            print("WARNING: the config couldn't be parsed because:")
            print(f"Outermost node of config must be parsable to dict, not {type(parsed_raw)}")
            print("Config ignored...")
            return parsed_config
    
    for field, parser in parsers.items():
        if valid_item := parsed_raw.pop(field, None):
            try:
                parsed_config[field] = parser(valid_item)
            except ConfigParseError as err:
                print(f"WARNING: the config field {field} parsing failed with message:")
                print(str(err))
                print("Config field ignored...")
    
    if parsed_raw:
        print(f"WARNING: config keys '{', '.join(parsed_raw.keys())}' unknown")

    return parsed_config


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


async def extract_load_collection(
        collection: AsyncIOMotorCollection,
        schema: Schema,
        outdir: Path = Path('.'),
        partition_key: str | None = None,
        batch_size: int | None = None,
        progress_bar: bool = True
):
    n_docs = await collection.estimated_document_count()
    pbar = None
    if progress_bar:
        pbar = tqdm(total=n_docs)
    if not batch_size:
        batch_size_bytes = 12_000_000  # 12MB for batch seem to work well
        n_samples = 1000
        cursor = collection.aggregate([{'$sample': {'size': n_samples}}])
        samples = await cursor.to_list(n_samples)
        sizes = map(lambda x: len(BSON.encode(x)), samples)
        avg_size = sum(sizes) / len(samples)
        batch_size = int(batch_size_bytes / avg_size)


    async with asyncio.TaskGroup() as tgroup:
        if not partition_key:
            outfile = outdir / f'{collection.name}.parquet'
            tgroup.create_task(extract_load_part(
                tgroup,
                collection=collection, schema=schema, path=outfile,
                filter={}, batch_size=batch_size, pbar_instance=pbar
            ))
        else:
            partitions = await generate_partitions(collection, partition_key)
            for partition in partitions:
                outfile = outdir / f'{collection.name}.parquet' / f'{partition_key}={partition}' / 'data.parquet'
                outfile.parent.mkdir(parents=True)
                filter = {partition_key: partition}
                tgroup.create_task(extract_load_part(
                    tgroup, collection=collection, schema=schema,
                    path=outfile, filter=filter,
                    batch_size=batch_size, pbar_instance=pbar
                ))


async def extract_load_part(
        taskgroup: asyncio.TaskGroup,
        *, collection: AsyncIOMotorCollection,
        schema: Schema,
        path: Path,
        filter: Mapping,
        batch_size: int,
        pbar_instance: tqdm | None = None
):
    pwriter = ParquetWriter(path, schema.schema())
    cursor = collection.find(filter, batch_size=batch_size)
    while cursor.alive:
        batch = await cursor.to_list(batch_size)
        taskgroup.create_task(
            write_batch_to_parquet(batch, schema, pwriter, not cursor.alive, pbar_instance)  # type: ignore
        )


async def write_batch_to_parquet(
        batch: List[Mapping[str, Any]],
        schema: Schema,
        writer: ParquetWriter,
        last_batch: bool,
        pbar_instance: tqdm | None = None):
    record_batch = schema.create_record_batch(batch)
    writer.write_batch(record_batch)
    if pbar_instance:
        pbar_instance.update(len(batch))
    if last_batch:
        writer.close()


if __name__ == "__main__":
    args = parse_args()
    if not args:
        exit(1)

    exit(asyncio.run(
        main(args.uri, db=args.db, collections=args.collections,
             outdir=args.outdir, schema_paths=args.schemas, samples=args.samples,
             partition_key=args.partition, config_file=args.config)
    ))
