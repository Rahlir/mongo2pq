import asyncio
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, List, Mapping, Optional, Tuple

from bson import Int64, ObjectId
from motor.motor_asyncio import AsyncIOMotorCollection
import pyarrow as pa
import pyarrow.types as pa_types
from tqdm import tqdm
from yaml.constructor import ConstructorError

from mongo2pq.types import PythonPrimitive
from mongo2pq.exceptions import SchemaParseError


class Schema:
    def __init__(self, name: str,
                 fields: Iterable[tuple | pa.Field] | Mapping[str, pa.DataType] | None = None,
                 pa_schema: pa.Schema | None = None):
        self._name = name
        if pa_schema:
            self._schema = pa_schema
        elif fields:
            self._schema = pa.schema(fields)
        else:
            self._schema = pa.schema([])
        self._cast_table_property: dict[str, Tuple[type, Callable]] = {}

    def merge(self, other: 'Schema'):
        if self._name != other._name:
            raise ValueError("You can only merge two schemas with the same name")

        try:
            new_schema = pa.unify_schemas([self._schema, other._schema])
        except pa.ArrowInvalid:
            mutable = pa.schema(other._schema)
            for i, field in enumerate(self._schema):
                if (other_i := mutable.get_field_index(field.name)) >= 0:
                    other_field = mutable.field(field.name)
                    if not other_field.type.equals(field.type):
                        new_type = unify_types(field.type, other_field.type)
                        self._schema = self._schema.set(i, field.with_type(new_type))
                        mutable = mutable.set(other_i, other_field.with_type(new_type))
            new_schema = pa.unify_schemas([self._schema, mutable])

        self._schema = new_schema
        self._cast_table_property = {}

    def use_config(self, config: Mapping[str, list]):
            config_list = config.get(self._name, [])
            for index, field_name in enumerate(self._schema.names):
                for config_node in config_list:
                    field_test = config_node['field_test']
                    field_type = config_node['field_type']
                    if (field_test(field_name) and
                        not self._schema.field(field_name).type.equals(field_type)):
                        self._schema = self._schema.set(
                            index, self._schema.field(index).with_type(field_type)
                        )
                        break

    def dump_to_file(self, filename: str | None = None, destination: Path = Path('.')):
        import yaml

        def schema_representer(dumper: yaml.Dumper, schema: Schema):
            schema_mapping = {
                'name': self._name,
                'fields': {
                    field_name: str(schema._schema.field(field_name).type)
                    for field_name in schema._schema.names
                }
            }
            return dumper.represent_mapping('!schema', schema_mapping)

        yaml.add_representer(Schema, schema_representer, Dumper=yaml.CDumper)  # type: ignore

        if not filename:
            filename = self._name + '.yaml'
        new_file_path = destination / filename
        with new_file_path.open('w') as schema_f:
            yaml.dump(self, schema_f, Dumper=yaml.CDumper, sort_keys=False)

    def create_record_batch(self, batch: List[Mapping[str, Any]]) -> pa.RecordBatch:
        def cast_row(row: Mapping[str, Any]) -> Mapping[str, Any]:
            return {
                key: self._cast_field(key, field)
                for key, field in row.items()
                # 'if field' to ignore empty strings and Nones
                if field and key in self._cast_table()
            }

        return pa.RecordBatch.from_pylist(
            [cast_row(row) for row in batch], schema=self._schema
        )

    def _cast_table(self) -> Mapping[str, Tuple[type, Callable]]:
        if not self._cast_table_property:
            for field_name, field_type in zip(self._schema.names, self._schema.types):
                if pa_types.is_boolean(field_type):

                    def cast_boolean(value: Any) -> bool:
                        if isinstance(value, str):
                            return value == 1
                        elif isinstance(value, str):
                            return value.lower() in ['true', 'yes']
                        return False

                    self._cast_table_property[field_name] = (bool, cast_boolean)
                elif pa_types.is_integer(field_type):
                    self._cast_table_property[field_name] = (int | Int64, int)
                elif pa_types.is_floating(field_type):
                    self._cast_table_property[field_name] = (float, float)
                elif pa_types.is_date(field_type):
                    self._cast_table_property[field_name] = (datetime, datetime.fromisoformat)
                elif pa_types.is_timestamp(field_type):

                    def cast_timestamp_compat(value: Any) -> int | float:
                        try:
                            return int(value)
                        except ValueError:
                            return float(value)

                    self._cast_table_property[field_name] = (int | float, cast_timestamp_compat)
                elif pa_types.is_string(field_type):
                    self._cast_table_property[field_name] = (str, str)
                elif pa_types.is_binary(field_type):
                    self._cast_table_property[field_name] = (bytes, bytes)
                else:
                    raise NotImplementedError(f"Casting for type {field_type} has not been implemented")

        return self._cast_table_property

    def _cast_field(self, key: str, field: Any) -> Any:
        target_type, cast_func = self._cast_table()[key]
        if isinstance(field, target_type):
            return field
        return cast_func(field)

    def name(self) -> str:
        return self._name

    def schema(self) -> pa.Schema:
        return self._schema


def load_schema_from_file(schema_file: Path, config: Mapping[str, list] | None) -> Schema:
    import yaml

    def schema_constructor(loader: yaml.Loader, node: yaml.MappingNode) -> Schema:
        schema_mapping = loader.construct_mapping(node, deep=True)
        try:
            schema_mapping['fields'] = {
                field_name: pa.type_for_alias(field_value)
                for field_name, field_value in schema_mapping['fields'].items()
            }


        except Exception as e:
            raise SchemaParseError(f"PyArrow fields couldn't be constructed: {e!s}")

        return Schema(schema_mapping['name'], fields=schema_mapping['fields'])

    yaml.add_constructor('!schema', schema_constructor, Loader=yaml.CLoader)  # type: ignore

    try:
        with schema_file.open('r') as s_stream:
            schema_obj = yaml.load(s_stream, yaml.CLoader)
            if not isinstance(schema_obj, Schema):
                raise SchemaParseError(f"Schema file {schema_file!s} doesn't contain Schema definition")
    except PermissionError:
        raise SchemaParseError(f"Schema file {schema_file!s} couldn't be opened")
    except ConstructorError as err:
        raise SchemaParseError(f"Schema file {schema_file!s} couldn't be parsed: {err!s}")
    else:
        if config:
            schema_obj.use_config(config)
        return schema_obj


def infer_schema_from_batch(name: str,  batch: Iterable[Mapping[str, Any]]) -> Schema:
    known_fields: Mapping[str, pa.DataType] = {}
    for row in batch:
        for field_name, field in row.items():
            new = infer_type(field, field_name)
            if known := known_fields.get(field_name):
                if not known.equals(new):
                    known_fields[field_name] = unify_types(known, new)
            else:
                known_fields[field_name] = new

    return Schema(name, fields=known_fields)


async def create_schema(collection: AsyncIOMotorCollection,
                        samples: int = 20000, config: Mapping[str, list] | None = None,
                        progress_bar: bool = True) -> Schema:
    batch_size = 1000
    # Need this style in order to accommodate pyright. Otherwise we get
    # 'possibly unbound error'
    pbar = None  
    if progress_bar:
        pbar = tqdm(total=samples)

    async with asyncio.TaskGroup() as tgroup:
        schema = Schema(collection.name)  # type: ignore
        cursor = collection.aggregate([{'$sample': {'size': samples}}], batchSize=batch_size)
        while cursor.alive:
            docs = await cursor.to_list(batch_size)
            tgroup.create_task(update_schema_with_batch(docs, schema))

            if pbar:
                pbar.update(batch_size)

    if config:
        schema.use_config(config)
    return schema


async def update_schema_with_batch(
    batch: Iterable[Mapping[str, Any]], current_schema: Schema
):
    new_schema = infer_schema_from_batch(current_schema.name(), batch)
    current_schema.merge(new_schema)


def unify_types(type1: pa.DataType, type2: pa.DataType) -> pa.DataType:

    def check_category(category_func: Callable, type_tests: List[Callable]) -> Optional[pa.DataType]:
        if category_func(type1) and category_func(type2):
            for type_test in type_tests:
                if type_test(type1):
                    return type1
                if type_test(type2):
                    return type2
        return None

    int_type = check_category(
        pa_types.is_integer,
        [pa_types.is_int64, pa_types.is_int32, pa_types.is_int16, pa_types.is_int8]
    )
    if int_type:
        return int_type

    uint_type = check_category(
        pa_types.is_unsigned_integer,
        [pa_types.is_uint64, pa_types.is_uint32, pa_types.is_uint16, pa_types.is_uint8]
    )
    if uint_type:
        return uint_type

    floating_type = check_category(
        pa_types.is_floating,
        [pa_types.is_float64, pa_types.is_float32, pa_types.is_float16]
    )
    if floating_type:
        return floating_type

    date_type = check_category(
        pa_types.is_date,
        [pa_types.is_date64, pa_types.is_date32]
    )
    if date_type:
        return date_type

    if pa_types.is_integer(type1) and pa_types.is_floating(type2):
        return type2
    if pa_types.is_floating(type1) and pa_types.is_integer(type2):
        return type1

    if (pa_types.is_integer(type1) or pa_types.is_floating(type1)) and pa_types.is_temporal(type2):
        return type1
    if pa_types.is_temporal(type1) and (pa_types.is_integer(type2) or pa_types.is_floating(type2)):
        return type2

    if pa_types.is_binary(type1) or pa_types.is_binary(type2):
        return pa.binary()

    return pa.string()


def cast_from_string(value: str) -> PythonPrimitive:
    if value.lower() in ['true', 'yes']:
        return True
    if value.lower() in ['false', 'no']:
        return False
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return value


def is_timestamp(value: int | float, range: int=5) -> bool:
    year_in_s = 31536000.0
    now_ts_in_s = datetime.now().timestamp()
    return now_ts_in_s - range * year_in_s <= value <= now_ts_in_s + range * year_in_s


def infer_type(value: Any, name: str) -> pa.DataType:
    if isinstance(value, dict | list):
        raise NotImplemented(
            "Inferring types for list or dict fields is not implemented"
        )

    if isinstance(value, str):
        value = cast_from_string(value)
        # Cast unsuccessful, the value remains string
        if isinstance(value, str):
            return pa.string()

    if isinstance(value, ObjectId):
        return pa.string()

    if isinstance(value, Int64):
        return pa.int64()

    if isinstance(value, bool):
        return pa.bool_()

    if isinstance(value, datetime):
        if not value.hour and not value.minute and not value.second and not value.microsecond:
            return pa.date32()
        return pa.date64()

    if isinstance(value, int | float):
        if is_timestamp(value):
            return pa.timestamp('s')

    if isinstance(value, int):
        try:
            pa.scalar(value, pa.int32())
            return pa.int32()
        except (OverflowError, pa.ArrowInvalid):
            try:
                pa.scalar(value, pa.int64())
            except (OverflowError, pa.ArrowInvalid) as err:
                # Too big to be int
                return pa.string()
            return pa.int64()

    if isinstance(value, float):
        try:
            pa.scalar(value, pa.float32())
            return pa.float32()
        except (OverflowError, pa.ArrowInvalid):
            try:
                pa.scalar(value, pa.float64())
            except (OverflowError, pa.ArrowInvalid) as err:
                err.add_note(f"Overflow for value {value} of {name}: {err.add_note}")
                raise err
            return pa.float64()

    if isinstance(value, bytes):
        return pa.binary()

    return pa.string()
