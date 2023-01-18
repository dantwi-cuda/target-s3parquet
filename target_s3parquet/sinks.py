"""s3parquet target sink class, which handles writing streams."""
from typing import Dict, List, Optional
import awswrangler as wr
from pandas import DataFrame
from singer_sdk import PluginBase
from singer_sdk.sinks import BatchSink
import json
from target_s3parquet.athena import (
    create_session,
    create_database,
)
from target_s3parquet.data_type_generator import (
    generate_tap_schema,
    generate_current_target_schema,
    generate_create_database_ddl,
)
from target_s3parquet.sanitizer import (
    get_specific_type_attributes,
    apply_json_dump_to_df,
    stringify_df,
)

#from __future__ import annotations

from singer_sdk.sinks import BatchSink

from datetime import datetime

STARTED_AT = datetime.now()

class s3parquetSink(BatchSink):
    """s3parquet target sink class."""
    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self._athena_session = ""
        self._glue_schema = self._get_glue_schema()
        #ddl = generate_create_database_ddl(self.config["athena_database"])
        create_database(self.config["athena_database"]);
        #athena.execute_sql(ddl, self.athena_client)
 

    @property
    def athena_session(self):
        if not self._athena_session:
            self._athena_session = create_session(self.config, self.logger)
        return self._athena_session

    @staticmethod
    def _clean_table_name(stream_name):
        return stream_name.replace("-", "_")
    
    
    def _get_glue_schema(self):
        session=self.athena_session()
        catalog_params = {
            "database": self.config.get("athena_database"),
            "table": self._clean_table_name(self.stream_name),
        }

        if wr.catalog.does_table_exist(**catalog_params,boto3_session=session):
            return wr.catalog.table(**catalog_params,boto3_session=session)
        else:
            return DataFrame()


    max_size = 10000  # Max records to write in one batch

    def start_batch(self, context: dict) -> None:
        """Start a batch.

        Developers may optionally add additional markers to the `context` dict,
        which is unique to this batch.
        """
        # Sample:
        # ------
        # batch_key = context["batch_id"]
        # context["file_path"] = f"{batch_key}.csv"

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record.

        Developers may optionally read or write additional markers within the
        passed `context` dict from the current batch.
        """
        # Sample:
        # ------
        # with open(context["file_path"], "a") as csvfile:
        #     csvfile.write(record)
    def validateJSON(jsonData):
        try:
            json.loads(jsonData)
        except ValueError as err:
            return False
        return True

    def searchStream_Partition_Info(stream_Name,jsonData):
        for attrib in jsonData:
            if attrib['stream_name']==stream_Name:
                return attrib
                break
        else:
            return None

  
        


    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written."""
        # Sample:
        # ------
        # client.upload(context["file_path"])  # Upload file
        # Path(context["file_path"]).unlink()  # Delete local copy

        df = DataFrame(context["records"])
        Partition_Cols=[]
        df["_sdc_started_at"] = STARTED_AT.timestamp()
        validate_partions= self.validateJSON(self.config.get("partition_info"))
        if validate_partions:
            Json_Partions=json.loads(self.config.get("partition_info"))
            partition_Data=self.searchStream_Partition_Info(Json_Partions)
            if partition_Data is not None:
                if 'column_1_Interval' in partition_Data and partition_Data['column_1_Interval']!="":
                    if partition_Data['column_1_Interval'].lower()=="daily":
                        df["_sdc_"+partition_Data["Partition_Column_1"]+"_date_"]=df[partition_Data["Partition_Column_1"]].dt.date
                        Partition_Cols.append(Partition_Cols="_sdc_"+partition_Data["Partition_Column_1"]+"_date_")
                    if partition_Data['column_1_Interval'].lower()=="monthly":
                        df["_sdc_"+partition_Data["Partition_Column_1"]+"_month_"]=df[partition_Data["Partition_Column_1"]].dt.month
                        Partition_Cols.append(Partition_Cols="_sdc_"+partition_Data["Partition_Column_1"]+"Partition_Column_1")
                    if partition_Data['column_1_Interval'].lower()=="yearly":
                        df["_sdc_"+partition_Data["Partition_Column_1"]+"_year_"]=df[partition_Data["Partition_Column_1"]].dt.year
                        Partition_Cols.append(Partition_Cols="_sdc_"+partition_Data["Partition_Column_1"]+"_year_")
                else:
                    partition_columnName=partition_Data['Partition_Column_1']
                    if partition_columnName!="": Partition_Cols.append(partition_columnName)

                if 'column_2_Interval' in partition_Data and partition_Data['column_2_Interval']!="":

                    if partition_Data['column_2_Interval'].lower()=="daily":
                        df["_sdc_"+partition_Data["Partition_Column_2"]+"_date_"]=df[partition_Data["Partition_Column_2"]].dt.date
                        Partition_Cols.append("_sdc_"+partition_Data["Partition_Column_2"]+"_date_")
                    if partition_Data['column_2_Interval'].lower()=="monthly":
                        df["_sdc_"+partition_Data["Partition_Column_2"]+"_month_"]=df[partition_Data["Partition_Column_2"]].dt.month
                        Partition_Cols.append("_sdc_"+partition_Data["Partition_Column_2"]+"_month_")
                    if partition_Data['column_2_Interval'].lower()=="yearly":
                        df["_sdc_"+partition_Data["Partition_Column_2"]+"_year_"]=df[partition_Data["Partition_Column_2"]].dt.year
                        Partition_Cols.append("_sdc_"+partition_Data["Partition_Column_2"]+"_year_")
                else:
                    partition_columnName=partition_Data['Partition_Column_2']
                    if partition_columnName!="": Partition_Cols.append(partition_columnName)





        current_schema = generate_current_target_schema(self._get_glue_schema())
        tap_schema = generate_tap_schema(
            self.schema["properties"], only_string=self.config.get("stringify_schema")
        )

        dtype = {**current_schema, **tap_schema}

        if self.config.get("stringify_schema"):
            attributes_names = get_specific_type_attributes(
                self.schema["properties"], "object"
            )
            df_transformed = apply_json_dump_to_df(df, attributes_names)
            df = stringify_df(df_transformed)

        self.logger.debug(f"DType Definition: {dtype}")

        full_path = "s3://{s3_bucket}/{key_prefix}{database}/{stream}/".format(
            s3_bucket=self.config.get("s3_bucket"),
            key_prefix=self.config.get("s3_key_prefix", ""),
            database=self.config.get("athena_database",""),
            stream=self.stream_name,
        )
        aws_session=self.athena_session()
        wr.s3.to_parquet(
            df=df,
            index=False,
            compression=self.config.get("compression"),
            dataset=True,
            path=full_path,
            database=self.config.get("athena_database"),
            table=self.stream_name,
            mode="append",
            partition_cols=Partition_Cols,
            schema_evolution=True,
            dtype=dtype,
        )

        self.logger.info(f"Uploaded {len(context['records'])}")

        context["records"] = []
