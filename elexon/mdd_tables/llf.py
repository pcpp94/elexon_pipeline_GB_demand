import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, LongType

from ..utils_others.utils import get_csv_files


def load_llfs():

    schema = StructType([StructField("MARKET_PARTICIPANT_ID", StringType(), True), StructField("LLF_CLASS_ID", StringType(), True), StructField("START_SETTLEMENT_DATE", TimestampType(), True), StructField("END_SETTLEMENT_DATE", TimestampType(
    ), True), StructField("DAY_TYPE", StringType(), True), StructField("VALUE", DoubleType(), True), StructField("START_SETTLEMENT_PERIOD", LongType(), True), StructField("END_SETTLEMENT_PERIOD", LongType(), True)])

    path = "/path/to/your/llfs_downloaded_files"  # Read README.md

    csv_files = get_csv_files(path)

    dtypes_llf = {'MARKET_PARTICIPANT_ID': str, 'LLF_CLASS_ID': str,
                  'START_SETTLEMENT_PERIOD':  'int64', 'END_SETTLEMENT_PERIOD':  'int64', 'VALUE': 'float64'}

    j = 0
    for g in csv_files:
        if j == 0:
            df_llf_hh = pd.read_csv(g, header=0, low_memory=False, dtype=dtypes_llf, parse_dates=[
                                    'START_SETTLEMENT_DATE', 'END_SETTLEMENT_DATE'], dayfirst=True).drop(columns=['DISTRIBUTOR_BUSINESS_ID'])
        else:
            aux_llf = pd.read_csv(g, header=0, low_memory=False, dtype=dtypes_llf, parse_dates=[
                'START_SETTLEMENT_DATE', 'END_SETTLEMENT_DATE'], dayfirst=True).drop(columns=['DISTRIBUTOR_BUSINESS_ID'])
            df_llf_hh = pd.concat([df_llf_hh, aux_llf])
        j = j+1

    df_llf_hh = df_llf_hh[['MARKET_PARTICIPANT_ID', 'LLF_CLASS_ID', 'START_SETTLEMENT_DATE',
                           'END_SETTLEMENT_DATE', 'DAY_TYPE', 'VALUE', 'START_SETTLEMENT_PERIOD', 'END_SETTLEMENT_PERIOD']]
    df_llf_hh = df_llf_hh.groupby(['MARKET_PARTICIPANT_ID', 'LLF_CLASS_ID', 'START_SETTLEMENT_DATE', 'END_SETTLEMENT_DATE',
                                   'DAY_TYPE', 'VALUE', 'START_SETTLEMENT_PERIOD', 'END_SETTLEMENT_PERIOD']).sum().reset_index()

    save_path = "/tmp/delta/llf"
    df_spark_hh = spark.createDataFrame(df_llf_hh, schema=schema)
    df_spark_hh.write.format("delta").save(save_path)

    # DATABRICKS: Saved as Table: elexon_etl.llf
