from typing import Mapping, Any, Iterable
from pathlib import Path
from re import Match, compile, sub

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
            print("Outermost node of config must be parsable to dict"
                  f", not {type(parsed_raw)}")
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
                "The collection node in schema config"
                f" must be parsable to list, not {type(fieldlist)}"
            )
        parsed_config[collection] = []
        for i, fieldconfig in enumerate(fieldlist):
            if not isinstance(fieldconfig, dict):
                _print_node_warning(
                    collection, i,
                    '', f"must be parsable to dict, not {type(fieldconfig)}"
                )
            else:
                try:
                    definition_type = fieldconfig['type']
                except KeyError:
                    _print_node_warning(
                        collection, i,
                        '', "is missing the required key 'type'"
                    )
                else:
                    parsed_config_item = None
                    if definition_type.startswith('retype_'):
                        parsed_config_item = _parse_retype_item(collection, i, fieldconfig)
                    elif definition_type.startswith('rename_'):
                        parsed_config_item = _parse_rename_item(collection, i, fieldconfig)
                    else:
                        _print_node_warning(
                            collection, i,
                            '', f"has unknown type {definition_type!s}"
                        )

                    if parsed_config_item:
                        parsed_config[collection].append(parsed_config_item)

    return parsed_config


def _parse_retype_item(collection: str,
                       index: int,
                       item: Mapping[str, str]) -> Mapping[str, Any] | None:
    try:
        field_type_raw = item['fieldtype']
        field_type = type_for_alias(field_type_raw)
        field_name = item['fieldname']
        retype_type = item['type'].removeprefix('retype_')
        field_test = None
        if retype_type == 'contains':
            field_test = lambda x: field_name in x
        elif retype_type == 'regex':
            operator = compile(field_name)
            field_test = lambda x: operator.search(x) is not None
        elif retype_type == 'equals':
            field_test = lambda x: x == field_name
        else:
            _print_node_warning(
                collection, index,
                'retype', f"has unknown type {item['type']}"
            )
            return None
    except KeyError:
        _print_node_warning(
            collection, index,
            'retype',
            "does not contain all required keys (type, fieldname, fieldtype)"
        )
        return None
    except ValueError:
        _print_node_warning(
            collection, index,
            'retype',
            f"has fieldtype {item['fieldtype']} which doesn't exist")
        return None

    return {
        'type': 'retype',
        'field_type': field_type,
        'field_test': field_test
    }


def _parse_rename_item(collection: str,
                       index: int,
                       item: Mapping[str, str]) -> Mapping[str, Any] | None:
    try:
        old_name = item['oldname']
        new_name = item['newname']
        rename_type = item['type'].removeprefix('rename_')
    except KeyError:
        _print_node_warning(
            collection, index,
            'rename',
            "does not contain all required keys (type, oldname, newname)"
        )
        return None

    try:
        pattern = compile(old_name)
    except SyntaxError:
        _print_node_warning(
            collection, index,
            'rename',
            f"has invalid oldname reges '{old_name}'"
        )
        return None

    if rename_type == 'regex':
        rename_function = lambda x: sub(pattern, new_name, x)

    elif rename_type == 'regex_upper':
        try:
            upper_list = item['upper']
        except KeyError:
            _print_node_warning(
                collection, index,
                'rename_regex_upper',
                f"does not contain the required field upper"
            )
            return None
        if not isinstance(upper_list, list):
            _print_node_warning(
                collection, index,
                'rename_regex_upper',
                f"has field upper of type {type(upper_list)}, not list"
            )
            return None

        def upper_regex_repl(matchobj: Match):
            group_index = 1
            new_name_replaced = new_name
            while rf"\{group_index}" in new_name_replaced:
                if group_index in upper_list:  # type: ignore
                    replacement = matchobj.group(group_index).upper()
                else:
                    replacement = matchobj.group(group_index)
                new_name_replaced = new_name_replaced.replace(rf"\{group_index}", replacement)
                group_index += 1
            return new_name_replaced

        rename_function = lambda x: sub(pattern, upper_regex_repl, x)

    else:
        _print_node_warning(
            collection, index,
            'rename', f"has unknown type {item['type']}"
        )
        return None

    return {
        'type': 'rename',
        'rename_function': rename_function
    }


def _print_node_warning(
    collection: str, index: int, node_type: str, error_desc: str
):
    if node_type:
        node_type += ' '
    print(
        "WARNING: "
        f"{node_type}node {index} in schema config for"
        f" {collection} {error_desc}"
    )
    print("Node ignored...")
