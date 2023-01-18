from target_s3parquet.sanitizer import get_valid_types, type_from_anyof


def build_struct_type(attributes, level):
    object_data_types = generate_tap_schema(attributes, level)

    stringfy_data_types = ", ".join([f"{k}:{v}" for k, v in object_data_types.items()])

    return f"struct<{stringfy_data_types}>"


def coerce_types(name, type):
    if name == "_sdc_sequence":
        return "string"

    if name == "_sdc_table_version":
        return "string"

    if type == "number":
        return "double"

    if type == "integer":
        return "int"

    return type


def generate_create_database_ddl(
    database: str="default"
)-> None:
    return f"CREATE DATABASE IF NOT EXISTS {database};"

def execute_sql(sql, athena_client):
    """Run sql expression using athena client

    Args:
        sql (string): a valid sql statement string
        athena_client ([type]): [description]
    """
    athena_client.execute(sql)


def generate_current_target_schema(schema):
    if schema.empty:
        return {}
    return schema.set_index(schema.columns[0])["Type"].to_dict()


def generate_tap_schema(schema, level=0, only_string=False):
    field_definitions = {}
    new_level = level + 1

    for name, attributes in schema.items():
        attribute_type = attributes.get("type") or type_from_anyof(attributes)

        if attribute_type is None:
            raise Exception(f"Invalid schema format: {schema}")

        cleaned_type = get_valid_types(attribute_type)

        if only_string:
            field_definitions[name] = "string"
            continue

        if cleaned_type == "object":
            field_definitions[name] = build_struct_type(
                attributes["properties"], new_level
            )
        elif cleaned_type == "array":
            array_type = get_valid_types(attributes["items"]["type"])

            if array_type == "object":
                array_type = build_struct_type(
                    attributes["items"]["properties"], new_level + 1
                )

            array_type = coerce_types(name, array_type)

            field_definitions[name] = f"array<{array_type}>"
        else:
            type = coerce_types(name, cleaned_type)

            field_definitions[name] = type

    return field_definitions
