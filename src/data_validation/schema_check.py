from pyspark.sql.functions import col,when
from src.utility.report_lib import write_output

def schema_check(source,target,spark):
    source_schema = source.schema
    print("source_schema is :",source_schema)
    target_schema = target.schema
    print("target_schema is :", target_schema)

    source_schema_df = spark.createDataFrame(["col_name","source_data_type"],
                                             [(field.name.lower(),field.dataType.simpleString()) for field in source_schema])

    target_schema_df = spark.createDataFrame(["col_name","target_data_type"],
                                             [(field.name.lower(),field.dataType.simpleString()) for field in target_schema])


    print("source_schema_df is :", source_schema_df.show())
    print("target_schema_df is :",target_schema_df.show())
    status = 'PASS'
    return status
