from typing import Mapping, Any, Iterable
from pathlib import Path
from re import compile

from pyarrow import type_for_alias

from mongo2pq.exceptions import ConfigParseError


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


def parse_schema_config(config: Any) -> Mapping[str, Iterable]:
    if not isinstance(config, dict):
        raise ConfigParseError(
            f"The schema config node must be parsable to dict, not {type(config)}"
        )

    parsed_config = {}
    for collection, fieldlist in config.items():
        if not isinstance(fieldlist, list):
            raise ConfigParseError(
                f"The collection node in schema config must be parsable to list, not {type(fieldlist)}"
            )
        parsed_config[collection] = []
        for i, fieldconfig in enumerate(fieldlist):
            if not isinstance(fieldconfig, dict):
                print(
                    f"WARNING: node {i} in schema config for {collection} must be parsable to dict, "
                    f"not {type(fieldconfig)}"
                )
                print("Node ignored...")
            else:
                try:
                    field_type_raw = fieldconfig['fieldtype']
                    field_type = type_for_alias(field_type_raw)
                    field_name = fieldconfig['fieldname']
                    definition_type = fieldconfig['type']
                    field_test = None
                    if definition_type == 'contains':
                        def create_contains(operand: str):
                            return lambda x: operand in x
                        field_test = create_contains(field_name)
                    elif definition_type == 'regex':
                        operator = compile(field_name)
                        field_test = lambda x: operator.search(x) is not None
                    elif definition_type == 'equals':
                        def create_equals(operand: str):
                            return lambda x: x == operand
                        field_test = create_equals(field_name)
                    else:
                        print(
                            f"WARNING: node {i} in schema config for {collection} "
                            f"has unknown type {definition_type}"
                        )
                        print("Node ignored...")
                except KeyError:
                    print(
                        f"WARNING: node {i} in schema config for {collection} "
                        "does not contain all required keys (type, fieldname, or fieldtype)"
                    )
                    print("Node ignored...")
                except ValueError:
                    print(
                        f"WARNING: node {i} in schema config for {collection} "
                        f"has fieldtype {fieldconfig['fieldtype']} which doesn't exist"
                    )
                    print("Node ignored...")
                else:
                    parsed_config[collection].append({
                        'field_type': field_type,
                        'field_test': field_test
                    })
    return parsed_config
