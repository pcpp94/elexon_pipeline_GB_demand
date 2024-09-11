import pandas as pd
import requests
import csv
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType

API_KEY = "your_API_KEY"  # Should use a Password Manager.
# You get this API Key by registering in https://www.elexonportal.co.uk/ under "My Profile"


def load_dictionary_elexon():

    # ELEXON PORTAL API - Registered BM Units - csv files

    schema = StructType([StructField('bm_unit_id', StringType(), True), StructField('bmu_name', StringType(), True), StructField('party_name', StringType(), True), StructField('party_id', StringType(), True), StructField('bmu_type', StringType(), True), StructField('ngc_bmu_name', StringType(), True), StructField('gsp_group_id', StringType(), True), StructField('gsp_group_name', StringType(), True), StructField('trading_unit_name', StringType(), True), StructField('prod/cons_flag', StringType(), True), StructField('prod/cons_status', StringType(), True), StructField('tlf', StringType(), True), StructField('gc', StringType(), True), StructField('dc', StringType(), True), StructField(
        'wdcalf', StringType(), True), StructField('nwdcalf', StringType(), True), StructField('secalf', StringType(), True), StructField('wdbmcaic', StringType(), True), StructField('nwdbmcaic', StringType(), True), StructField('wdbmcaec', StringType(), True), StructField('nwdbmcaec', StringType(), True), StructField('exempt_export_flag', StringType(), True), StructField('base_tu_flag', StringType(), True), StructField('fpn_flag', StringType(), True), StructField('interconnector_id', StringType(), True), StructField('effective_from', StringType(), True), StructField('_manual_credit_qualifying_flag', StringType(), True), StructField('_credit_qualifying_status', StringType(), True)])

    save_path = "/tmp/delta/BMUnits"

    r = requests.get(
        f'https://downloads.elexonportal.co.uk/file/download/REGISTERED_BMUNITS_FILE?key={API_KEY}')
    r.text
    r_2 = r.text.splitlines()
    r_3 = csv.reader(r_2)
    r_4 = list(r_3)
    df = pd.DataFrame(r_4)
    df = df.iloc[2:, :]
    new_header = df.iloc[0]
    df = df[1:]
    df.columns = new_header
    df.reset_index(inplace=True, drop=True)
    for i in df.columns:
        df.rename(columns={i: i.replace(" ", "_").lower()}, inplace=True)

    df_spark = spark.createDataFrame(df, schema=schema)
    df_spark.write.format("delta").save(save_path)

    # DATABRICKS: Saved as Table: elexon_etl.elexon_bm_units
