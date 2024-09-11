from ..utils_others.utils import sr_to_int, bmu_category_dic
import os
import pandas as pd
import numpy as np
import pyspark.pandas as ps
import datetime
import sys
from pyspark.sql.functions import *
from pyspark.sql.types import *
from databricks.sdk.runtime import *


def load_elexon_p114_s014():
    save_path = "/tmp/delta/SAA014/"
    save_path_all = "/tmp/delta/SAA014_all/"

    df = ps.read_table('elexon.p114_s01421')  # Check README.
    df = df.drop(columns=['DataFlowID', 'InsertedDate'])
    df['SettlementDate'] = ps.to_datetime(
        df['SettlementDate'], format='%Y-%m-%d')
    df['PeriodFPN'] = ps.to_numeric(df['PeriodFPN'])
    df['BMUnitMeteredVolume'] = ps.to_numeric(df['BMUnitMeteredVolume'])
    df['TransmissionLossMultiplier'] = ps.to_numeric(
        df['TransmissionLossMultiplier'])
    df['BMUnitApplicableBalancingServicesVolume'] = ps.to_numeric(
        df['BMUnitApplicableBalancingServicesVolume'])
    # From the bmu_dictionaries sub-package.
    df_2 = ps.read_table('elexon_etl.bmu_dictionary')
    df_2 = df_2[df_2['last_flag'] == True]
    df = df.merge(df_2[['bmu_id', 'bsc_party', 'enapp_name', 'ngc_name', 'ngc_bmu_id', 'ngc_bmu_flag', 'enapp_class', 'all_notify'
                        ]], how='left', left_on='BMUnitId', right_on='bmu_id').drop(columns=['bmu_id'])
    df['ImportExportIndicator'] = df['BMUnitMeteredVolume'].apply(
        lambda x: 'export' if x >= 0 else 'import')
    df['player'] = df['BMUnitId'].str.slice(stop=2)
    df['enapp_class'] = df['enapp_class'].fillna('NONE')
    df = df.merge(ps.DataFrame(bmu_category_dic), how='left')

    df.to_spark().write.format("delta").option(
        'OverwriteSchema', True).save(save_path_all)
    # DATABRICKS: Saved as Table: elexon_t.p114_s014_hh_all

    settlement = ps.DataFrame.from_dict(sr_to_int, orient='index')
    settlement = settlement.reset_index().rename(
        columns={'index': 'Settlement Run', 0: 'Settlement Value'})
    df = ps.merge(df, settlement, how='left', left_on=[
                  'SettlementRunType'], right_on=['Settlement Run'])
    df = df.drop(columns='Settlement Run')
    df = ps.merge(df, df.groupby(['SettlementDate', 'SettlementPeriod', 'BMUnitId']).max().reset_index()[['SettlementDate', 'SettlementPeriod', 'BMUnitId',
                  'Settlement Value']],  how='left', left_on=['SettlementDate', 'SettlementPeriod', 'BMUnitId'], right_on=['SettlementDate', 'SettlementPeriod', 'BMUnitId'])
    df['Last Run Flag'] = (df['Settlement Value_x'] ==
                           df['Settlement Value_y'])
    df = df.drop(columns=['Settlement Value_x', 'Settlement Value_y'])
    df = df[df['Last Run Flag'] == True]
    df = df.drop(columns=['Last Run Flag'])
    df = df.drop_duplicates()

    df.to_spark().write.format("delta").option(
        'OverwriteSchema', True).save(save_path)
    # DATABRICKS: Saved as Table: elexon_t.p114_s014_hh
