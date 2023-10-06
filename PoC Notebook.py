# Databricks notebook source
from dataclasses import dataclass, field
from enum import Enum

TableType = Enum("EXTERNAL","MANAGED")

@dataclass
class Table:
  name: str
  type: str

@dataclass
class Schema:
  name: str
  tables:[Table] = field(default_factory=list)

@dataclass
class Catalog:
  name: str
  schemas:[Schema] = field(default_factory=list) 


# COMMAND ----------

def create_new_assets(schemas, old_catalog_name:str, new_catalog_name: str, managed: bool, external: bool):
    # Create Schemas
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {new_catalog_name}.{schema.name}")
        if len(schema.tables) > 0:
            # Create Managed Tables
            for table in schema.tables:
                if table.type == "MANAGED" and managed:
                    spark.sql(
                        f"""
                       CREATE OR REPLACE TABLE {new_catalog_name}.{schema.name}.{table.name}
                       DEEP CLONE {old_catalog_name}.{schema.name}.{table.name}
                      """
                    )

                if table.type == "EXTERNAL" and external:
                    # Get DDL
                    ddl = spark.sql(
                        f"SHOW CREATE TABLE {old_catalog_name}.{schema.name}.{table.name}"
                    ).collect()[0]["createtab_stmt"]

                    # Drop Existing External Table as UC does not support 1:M mapping for External Locations/Tables
                    spark.sql(f" DROP TABLE {old_catalog_name}.{schema.name}.{table.name}")

                    # Create new External Table
                    new_ddl = ddl.replace(
                        f"CREATE TABLE {old_catalog_name}.{schema.name}.{table.name}",
                        f"CREATE OR REPLACE TABLE {new_catalog_name}.{schema.name}.{table.name}",
                    ).split("TBLPROPERTIES")[0]
                    try:
                        spark.sql(new_ddl)
                    except:
                        spark.sql(ddl.split("TBLPROPERTIES")[0])
                        raise ValueError("Could not create External Table")


def build_asset_tree(catalog_name, schemas, catalog_schemas):
    for schema in schemas:
        new_schema = Schema(name=schema["schema_name"])
        catalog_schemas.append(new_schema)

    tables = spark.sql(
        f"select table_schema, table_name, table_type from {catalog_name}.information_schema.tables where table_schema != 'information_schema'"
    ).collect()

    for table in tables:
        table_entry = Table(name=table["table_name"], type=table["table_type"])

        target_schema = [
            schema for schema in catalog_schemas if schema.name == table["table_schema"]
        ]

        if len(target_schema) == 1:
            target_schema[0].tables.append(table_entry)

        elif len(target_schema) == 0:
            raise ValueError(f"Schema {target_schema} not found")
        else:
            raise ValueError("More than 1 schema found with same name")
    return catalog_schemas


def copy_catalog(
    old_catalog_name: str,
    new_catalog_name: str,
    managed: bool = False,
    external: bool = False,
):
    schemas = spark.sql(
        f"select schema_name from {old_catalog_name}.information_schema.schemata where schema_name != 'information_schema'"
    ).collect()

    # List of schemas for the specific catalog
    list_of_schemas = []

    # Build asset structure for each schema
    build_asset_tree(old_catalog_name, schemas, list_of_schemas)

    # Replicate asset structure into new catalog
    create_new_assets(list_of_schemas,old_catalog_name,new_catalog_name, managed, external)

# COMMAND ----------

copy_catalog(
    old_catalog_name="gshen_catalog",
    new_catalog_name="gshen_catalog_clone",
    managed=True,
    external=False,
)
