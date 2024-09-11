import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, BooleanType
import pyspark.pandas as ps


def load_merged_dictionary():

    # Merging EnAPPSys and Elexon Data

    save_path = "/tmp/delta/Full_BMU_Dictionary"

    schema = StructType([StructField('bmu_id', StringType(), True), StructField('player', StringType(), True), StructField('bsc_party', StringType(), True), StructField('ngc_bmu_id', StringType(), True), StructField('ngc_bmu_flag', BooleanType(), True), StructField('last_flag', BooleanType(), True), StructField('currently_active', BooleanType(), True), StructField('effective_from_date', TimestampType(), True), StructField('effective_to_date', StringType(), True), StructField('generation_capacity', DoubleType(), True), StructField('demand_capacity', DoubleType(), True), StructField('enapp_name', StringType(), True), StructField('enapp_class', StringType(), True), StructField('neta_type', StringType(), True), StructField(
        'neta_name', StringType(), True), StructField('ngc_type', StringType(), True), StructField('ngc_name', StringType(), True), StructField('power_station_id', StringType(), True), StructField('power_station_name', StringType(), True), StructField('all_notify', BooleanType(), True), StructField('notify_not_sva', BooleanType(), True), StructField('notify_cva', BooleanType(), True), StructField('notify_sva', BooleanType(), True), StructField('embedded_cva', BooleanType(), True), StructField('directcon_cva', BooleanType(), True), StructField('intercons', BooleanType(), True), StructField('sva_only', BooleanType(), True)])

    # From bmu_dictionaries sub-package
    enapp_df = ps.read_table('elexon_etl.enappsys_bmu_dictionary').to_pandas()
    # From bmu_dictionaries sub-package
    elexon_df = ps.read_table('elexon_etl.elexon_bm_units').to_pandas()
    final_df = enapp_df.merge(elexon_df[['bm_unit_id', 'fpn_flag']], how='left',
                              left_on='bmu_id', right_on='bm_unit_id', indicator=True).drop(columns='bm_unit_id')
    final_df = final_df.reset_index(drop=True)

    # Filling the missing data for these columns:

    groups1 = ['embedded_cva',	'directcon_cva',	'intercons',	'sva_only']
    conditions = ['E_', ['T_', 'M_'],  'I_', '2_']
    iterable = zip(groups1, conditions)

    dictionary1 = dict()

    for k, v in iterable:
        dictionary1[k] = v

    for group in groups1:
        index = final_df[final_df[group].isna()].index
        try:
            final_df.loc[index, group] = pd.DataFrame(final_df.loc[index, 'player']).apply(
                lambda x: True if x['player'] == dictionary1[group] else False, axis=1)
        except:
            final_df.loc[index, group] = pd.DataFrame(final_df.loc[index, 'player']).apply(
                lambda x: True if x['player'] in dictionary1[group] else False, axis=1)

    # These are to be changed depending on National Grid FPN Flag
    groups2 = ['notify_not_sva',	'notify_cva',	'notify_sva']

    index = final_df[(final_df['_merge'] == 'both') &
                     (final_df['all_notify'].isna())].index

    final_df.loc[index, 'all_notify'] = pd.DataFrame(final_df.loc[index, 'fpn_flag']).apply(
        lambda x: False if x['fpn_flag'] == 'F' else True, axis=1)

    groups2 = ['notify_not_sva',	'notify_cva',	'notify_sva']
    conditions2 = [['C_', 'E_', 'I_', 'M_', 'T_', 'V_'],
                   ['T_', 'E_', 'M_'], ['2_']]
    iterable2 = zip(groups2, conditions2)

    dictionary2 = dict()

    for k, v in iterable2:
        dictionary2[k] = v

    for group in groups2:
        final_df.loc[index, group] = final_df.loc[index, ['player', 'all_notify']].apply(
            lambda x: True if x['player'] in dictionary2[group] and x['all_notify'] == True else False, axis=1)

    final_df = final_df.drop(columns=['fpn_flag', '_merge'])

    spark.createDataFrame(final_df, schema=schema).write.format(
        'delta').save(save_path)

    # DATABRICKS: Saved as Table: elexon_etl.bmu_dictionary
