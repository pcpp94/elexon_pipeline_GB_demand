import pandas as pd
import pyspark.pandas as ps
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

from ..utils_others.utils import get_csv_files

ps.set_option("compute.ops_on_diff_frames", True)


def load_pc():

    schema = StructType([StructField("PROFILE_CLASS_ID", StringType(), True), StructField("GSP_GROUP_ID", StringType(), True), StructField("STD_SETTLEMENT_CONFIG_ID", StringType(), True), StructField("TIME_PATTERN_REGIME_ID", StringType(), True), StructField("SETTLEMENT_DATE", TimestampType(), True), StructField("PPC_1", DoubleType(), True), StructField("PPC_2", DoubleType(), True), StructField("PPC_3", DoubleType(), True), StructField("PPC_4", DoubleType(), True), StructField("PPC_5", DoubleType(), True), StructField("PPC_6", DoubleType(), True), StructField("PPC_7", DoubleType(), True), StructField("PPC_8", DoubleType(), True), StructField("PPC_9", DoubleType(), True), StructField("PPC_10", DoubleType(), True), StructField("PPC_11", DoubleType(), True), StructField("PPC_12", DoubleType(), True), StructField("PPC_13", DoubleType(), True), StructField("PPC_14", DoubleType(), True), StructField("PPC_15", DoubleType(), True), StructField("PPC_16", DoubleType(), True), StructField("PPC_17", DoubleType(), True), StructField("PPC_18", DoubleType(), True), StructField("PPC_19", DoubleType(), True), StructField("PPC_20", DoubleType(), True), StructField("PPC_21", DoubleType(), True), StructField("PPC_22", DoubleType(
    ), True), StructField("PPC_23", DoubleType(), True), StructField("PPC_24", DoubleType(), True), StructField("PPC_25", DoubleType(), True), StructField("PPC_26", DoubleType(), True), StructField("PPC_27", DoubleType(), True), StructField("PPC_28", DoubleType(), True), StructField("PPC_29", DoubleType(), True), StructField("PPC_30", DoubleType(), True), StructField("PPC_31", DoubleType(), True), StructField("PPC_32", DoubleType(), True), StructField("PPC_33", DoubleType(), True), StructField("PPC_34", DoubleType(), True), StructField("PPC_35", DoubleType(), True), StructField("PPC_36", DoubleType(), True), StructField("PPC_37", DoubleType(), True), StructField("PPC_38", DoubleType(), True), StructField("PPC_39", DoubleType(), True), StructField("PPC_40", DoubleType(), True), StructField("PPC_41", DoubleType(), True), StructField("PPC_42", DoubleType(), True), StructField("PPC_43", DoubleType(), True), StructField("PPC_44", DoubleType(), True), StructField("PPC_45", DoubleType(), True), StructField("PPC_46", DoubleType(), True), StructField("PPC_47", DoubleType(), True), StructField("PPC_48", DoubleType(), True), StructField("PPC_49", DoubleType(), True), StructField("PPC_50", DoubleType(), True)])

    path = "/path/to/your/profile_coef_downloaded_files"  # Read README.md

    csv_files = get_csv_files(path)
    csv_files = [i.replace('/dbfs', '') for i in csv_files]

    dtypes_pc = {'PROFILE_CLASS_ID': 'str', 'GSP_GROUP_ID': 'str', 'STD_SETTLEMENT_CONFIG_ID': 'str',
                 'TIME_PATTERN_REGIME_ID': 'str', 'PPC_1': 'float64', 'PPC_2': 'float64', 'PPC_3': 'float64',
                 'PPC_4': 'float64', 'PPC_5': 'float64', 'PPC_6': 'float64', 'PPC_7': 'float64', 'PPC_8': 'float64', 'PPC_9': 'float64', 'PPC_10': 'float64',
                 'PPC_11': 'float64', 'PPC_12': 'float64', 'PPC_13': 'float64', 'PPC_14': 'float64', 'PPC_15': 'float64', 'PPC_16': 'float64', 'PPC_17': 'float64',
                 'PPC_18': 'float64', 'PPC_19': 'float64', 'PPC_20': 'float64', 'PPC_21': 'float64', 'PPC_22': 'float64', 'PPC_23': 'float64', 'PPC_24': 'float64',
                 'PPC_25': 'float64', 'PPC_26': 'float64', 'PPC_27': 'float64', 'PPC_28': 'float64', 'PPC_29': 'float64', 'PPC_30': 'float64', 'PPC_31': 'float64',
                 'PPC_32': 'float64', 'PPC_33': 'float64', 'PPC_34': 'float64', 'PPC_35': 'float64', 'PPC_36': 'float64', 'PPC_37': 'float64', 'PPC_38': 'float64',
                 'PPC_39': 'float64', 'PPC_40': 'float64', 'PPC_41': 'float64', 'PPC_42': 'float64', 'PPC_43': 'float64', 'PPC_44': 'float64', 'PPC_45': 'float64',
                 'PPC_46': 'float64', 'PPC_47': 'float64', 'PPC_48': 'float64', 'PPC_49': 'float64', 'PPC_50': 'float64'}

    # Loop over the list of csv files [reading them and appending them to the dataframe]
    j = 0

    for g in csv_files:
        if j == 0:
            df_2 = ps.read_csv(g, header=0, low_memory=False, dtype=dtypes_pc)
        else:
            aux_2 = ps.read_csv(g, header=0, low_memory=False, dtype=dtypes_pc)
            df_2 = ps.concat([df_2, aux_2])
        j = j+1

    df_2['SETTLEMENT_DATE'] = ps.to_datetime(df_2['SETTLEMENT_DATE'])
    df_2 = df_2.reset_index(drop=True)

    save_path = "/tmp/delta/pc"
    df_2.to_spark().write.format("delta").save(save_path)

    # DATABRICKS: Saved as Table: elexon_etl.pc
