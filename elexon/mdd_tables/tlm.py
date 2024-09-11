import pandas as pd
import requests
import csv
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, LongType

from ..utils_others.utils import get_xlsx_files

API_KEY = "your_API_KEY"  # Should use a Password Manager.
# You get this API Key by registering in https://www.elexonportal.co.uk/ under "My Profile"


def load_tlm():

    schema = StructType([StructField("Settlement_Date", TimestampType(), True), StructField("Settlement_Run_Type", StringType(), True), StructField(
        "Settlement_Period", LongType(), True), StructField("Off_Taking", DoubleType(), True), StructField("Delivering", DoubleType(), True), StructField("gsp_area", StringType(), True)])

    r = requests.get(
        f'https://downloads.elexonportal.co.uk/file/download/TLM_FILE?key={API_KEY}')

    r_2 = r.text.splitlines()
    r_3 = csv.reader(r_2)
    r_4 = list(r_3)
    TLM = pd.DataFrame(r_4)
    new_header = TLM.iloc[0]
    TLM = TLM[1:]
    TLM.columns = new_header

    TLM.reset_index(inplace=True, drop=True)
    TLM = TLM.astype({
        'Settlement Period': 'int64',
        'Zone': 'int64',
        'Off Taking': 'float64',
        'Delivering': 'float64'
    })

    TLM['Settlement Date'] = pd.to_datetime(
        TLM['Settlement Date'], format='%d/%m/%Y')
    TLM = TLM.reset_index(drop=True)

    zone = {
        1: "_A",
        2: "_B",
        3: "_C",
        4: "_D",
        5: "_E",
        6: "_F",
        7: "_G",
        8: "_H",
        9: "_J",
        10: "_K",
        11: "_L",
        12: "_M",
        13: "_N",
        14: "_P"}

    zones = pd.DataFrame(zone.items())
    zones['key'] = 0
    zones = zones.drop(columns=0)

    name = []
    for i in range(len(TLM)):
        name.append(zone[TLM["Zone"].values[i]])
    TLM['gsp_area'] = name
    TLM = TLM.drop(columns='Zone')
    TLM = TLM.sort_values(by=['Settlement Date'])
    TLM = TLM.reset_index(drop=True)

    j = 0

    for d in TLM['Delivering']:
        if d == 0:
            TLM.loc[j, 'Delivering'] = 1
        j = j + 1

    TLM = TLM.rename(columns={'Settlement Date': 'Settlement_Date', 'Settlement Run Type': 'Settlement_Run_Type',
                     'Settlement Period': 'Settlement_Period', 'Off Taking': 'Off_Taking'})

    save_path = "/tmp/delta/tlm"
    tlm_spark = spark.createDataFrame(TLM, schema=schema)
    tlm_spark.write.format("delta").save(save_path)

    # DATABRICKS: Saved as Table: elexon_etl.tlm
