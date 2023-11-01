from typing import List, Any

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection


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
