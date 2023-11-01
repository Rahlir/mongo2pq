import asyncio
from pathlib import Path
from typing import Mapping, List, Any

from motor.motor_asyncio import AsyncIOMotorCollection
from tqdm import tqdm
from bson import BSON
from pyarrow.parquet import ParquetWriter

from mongo2pq.schema import Schema
from mongo2pq.mongo import generate_partitions

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
