import pyspark.pandas as ps
from pyspark.sql.functions import *
from pyspark.sql.types import *

from ..utils_others.utils import sr_to_int


def load_elexon_p114_c042():

    save_path = "/tmp/delta/cdca042/"
    df = ps.read_table('elexon.p114_c0421')  # Check README.
    df['SettlementDate'] = ps.to_datetime(
        df['SettlementDate'], format='%Y-%m-%d')
    df = df[df['SettlementDate'] >= '2015-01-01']
    df['SettlementPeriod'] = ps.to_numeric(df['SettlementPeriod'])
    df['MeterVolume'] = ps.to_numeric(df['MeterVolume'])
    # From bmu_dictionaries sub-package
    df_2 = ps.read_table('elexon_etl.bmu_dictionary')
    df_2 = df_2[df_2['last_flag'] == True]
    df = df.drop(columns=['DataFlowID', 'CDCARunNumber',
                 'DateofAggregation', 'InsertedDate'])
    df['player'] = df['BMUnitId'].str.slice(stop=2)
    settlement = ps.DataFrame.from_dict(sr_to_int, orient='index')
    settlement = settlement.reset_index().rename(
        columns={'index': 'Settlement Run', 0: 'Settlement Value'})
    df.groupby(['BMUnitId', 'SettlementDate', 'SettlementRunType', 'SettlementPeriod',
               'EstimateIndicator', 'ImportExportIndicator', 'player']).sum().reset_index()
    df = ps.merge(df, settlement, how='left', left_on=[
                  'SettlementRunType'], right_on=['Settlement Run'])
    df = df.drop(columns='Settlement Run')
    df = ps.merge(df, df.groupby(['SettlementDate', 'SettlementPeriod', 'BMUnitId', 'ImportExportIndicator']).max().reset_index()[['SettlementDate', 'SettlementPeriod', 'BMUnitId', 'Settlement Value',
                  'ImportExportIndicator']],  how='left', left_on=['SettlementDate', 'SettlementPeriod', 'BMUnitId', 'ImportExportIndicator'], right_on=['SettlementDate', 'SettlementPeriod', 'BMUnitId', 'ImportExportIndicator'])
    df['Last Run Flag'] = (df['Settlement Value_x'] ==
                           df['Settlement Value_y'])
    df = df.drop(columns=['Settlement Value_x', 'Settlement Value_y'])
    df = df[df['Last Run Flag'] == True]
    df = df.drop(columns=['Last Run Flag'])
    df = df.merge(df_2[['bmu_id', 'bsc_party', 'enapp_name', 'ngc_name', 'ngc_bmu_id', 'ngc_bmu_flag', 'enapp_class', 'all_notify',
                        ]], how='left', left_on='BMUnitId', right_on='bmu_id').drop(columns=['bmu_id'])
    df = df.drop_duplicates()
    df.to_spark().write.format("delta").option(
        "overwriteSchema", "true").mode("overwrite").save(save_path)
    # Last Run of the CDCA042 dataflow which is BMU data for only the CVA connected ones.
    # DATABRICKS: Saved as Table: elexon_f.p114_c042

    save_path = "/tmp/delta/big_demand_all_runs/"
    big_demand = ps.read_table('elexon.p114_c0421')  # README.
    big_demand['SettlementDate'] = ps.to_datetime(
        big_demand['SettlementDate'], format='%Y-%m-%d')
    big_demand = big_demand[big_demand['SettlementDate'] >= '2015-01-01']
    big_demand['SettlementPeriod'] = ps.to_numeric(
        big_demand['SettlementPeriod'])
    big_demand['MeterVolume'] = ps.to_numeric(big_demand['MeterVolume'])
    big_demand = big_demand.drop(
        columns=['DataFlowID', 'CDCARunNumber', 'DateofAggregation', 'InsertedDate'])
    big_demand['player'] = big_demand['BMUnitId'].str.slice(stop=2)
    big_demand.groupby(['BMUnitId', 'SettlementDate', 'SettlementRunType', 'SettlementPeriod',
                       'EstimateIndicator', 'ImportExportIndicator', 'player']).sum().reset_index()
    big_demand = big_demand.merge(df_2[['bmu_id', 'enapp_class']], how='left',
                                  left_on='BMUnitId', right_on='bmu_id').drop(columns=['bmu_id'])
    big_demand = big_demand.drop_duplicates()
    big_demand = big_demand[(big_demand['enapp_class'].isin(
        ['CHEMICAL', 'DEMAND', 'RAIL', 'STEEL'])) & (big_demand['ImportExportIndicator'] == 'I')]
    big_demand['SettlementPeriod'] = big_demand['SettlementPeriod'].astype(int)
    big_demand = big_demand[['SettlementDate', 'SettlementRunType',
                             'SettlementPeriod', 'EstimateIndicator', 'MeterVolume', 'enapp_class']]
    big_demand.columns = ['date', 'settlement_run', 'settlement_period',
                          'actual_estimate', 'total_corrected_consumption_gwh', 'description']
    big_demand = big_demand.groupby(
        by=['date', 'settlement_run', 'settlement_period', 'actual_estimate', 'description']).sum().reset_index()
    big_demand['total_corrected_consumption_gwh'] = big_demand['total_corrected_consumption_gwh']/1000
    big_demand['total_uncorrected_consumption_gwh'] = big_demand['total_corrected_consumption_gwh']
    big_demand['uncorrected_consumption_gwh'] = big_demand['total_corrected_consumption_gwh']
    big_demand['corrected_consumption_gwh'] = big_demand['total_corrected_consumption_gwh']
    big_demand['uncorrected_line_loss_gwh'] = 0
    big_demand['corrected_line_loss_gwh'] = 0
    big_demand['gsp_area'] = 'T-connected'
    big_demand['aggregation_type'] = 'H'
    big_demand['actual_estimate'] = big_demand['actual_estimate'].apply(
        lambda x: 'A' if x == False else 'E')
    big_demand['sector'] = 'Non-domestic'
    big_demand['meters_count'] = 0
    big_demand['peak'] = big_demand['settlement_period'].apply(
        lambda x: 'P' if (x >= 33 and x <= 38) else 'N')
    big_demand = big_demand.drop_duplicates()
    big_demand.to_spark().write.format("delta").option(
        "overwriteSchema", "true").mode("overwrite").save(save_path)
    # All Runs of only the T-connected Big Demand --> Chemical, Big Demand, Rail, Steel
    # DATABRICKS: Saved as Table: elexon_t.p114_big_demand_all

    save_path = "/tmp/delta/big_demand_final_run/"
    settlement = settlement.rename(
        columns={'Settlement Run': 'settlement_run'})
    big_demand_final_run = ps.merge(big_demand, settlement, how='left', left_on=[
                                    'settlement_run'], right_on=['settlement_run'])
    big_demand_final_run = ps.merge(big_demand_final_run, big_demand_final_run.groupby(['date', 'settlement_period', 'description']).max().reset_index(
    )[['date', 'settlement_period', 'description', 'Settlement Value']],  how='left', left_on=['date', 'settlement_period', 'description'], right_on=['date', 'settlement_period', 'description'])
    big_demand_final_run['Last Run Flag'] = (
        big_demand_final_run['Settlement Value_x'] == big_demand_final_run['Settlement Value_y'])
    big_demand_final_run = big_demand_final_run.drop(
        columns=['Settlement Value_x', 'Settlement Value_y'])
    big_demand_final_run = big_demand_final_run[big_demand_final_run['Last Run Flag'] == True]
    big_demand_final_run = big_demand_final_run.drop(columns=['Last Run Flag'])
    big_demand_final_run.to_spark().write.format("delta").option(
        "overwriteSchema", "true").mode("overwrite").save(save_path)
    # Big Demand Last Run
    # DATABRICKS: Saved as Table: elexon_f.p114_big_demand_lr

    save_path = "/tmp/delta/embedded_cva/"
    df = ps.read_table('elexon_f.p114_c042')  # From this script.
    df = df[(df['player'] == 'E_')]
    df.to_spark().write.format("delta").option(
        "overwriteSchema", "true").mode("overwrite").save(save_path)
    # Embedded CVA exports
    # DATABRICKS: Saved as Table: elexon_t.p114_embedded_cva
