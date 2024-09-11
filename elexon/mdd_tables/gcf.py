import pyspark.pandas as ps
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

from ..utils_others.utils import get_csv_files

ps.set_option("compute.ops_on_diff_frames", True)


def load_gcf():
    schema = StructType([StructField("GSP_area", StringType(), True), StructField("Settlement_Date", TimestampType(), True), StructField(
        "Run_Type", StringType(), True), StructField("Settlement_Period", DoubleType(), True), StructField("GCF", DoubleType(), True)])

    path = "/path/to/your/gcf_downloaded_files"  # Read README.md
    csv_files = get_csv_files(path)
    csv_files = [i.replace('/dbfs', '') for i in csv_files]

    j = 0
    for g in csv_files:
        if j == 0:
            gcf = ps.read_csv(g)
        else:
            aux = ps.read_csv(g)
            gcf = ps.concat([gcf, aux])
        j = j+1

    gcf = gcf.rename(columns={
        'GSP_GROUP_ID': 'GSP_area',
        'SETTLEMENT_DATE': 'Settlement_Date',
        'SSR_RUN_TYPE': 'Run_Type',
        'SETTLEMENT_PERIOD': 'Settlement_Period',
        'GSP_GROUP_CORRECTION_FACTOR': 'GCF'
    })

    gcf['Settlement_Date'] = ps.to_datetime(gcf['Settlement_Date'])

    save_path = "/tmp/delta/gcf"
    gcf.to_spark().write.format("delta").save(save_path)

    # DATABRICKS: Saved as Table: elexon_etl.gcf
