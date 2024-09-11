from ..utils_others.utils import wd_ok, string_to_numeric, numeric_to_string, peak_func, aux_date_func, sr_to_int, get_holidays_calendar
import os
import pandas as pd
import numpy as np
import pyspark.pandas as ps
import datetime
import sys
from pyspark.sql.functions import *
from pyspark.sql.types import *
from databricks.sdk.runtime import *


def load_elexon_p315_277():

    # Getting the P315 - 277 Dataflow adding LLFs, PCs, GCFs, TLMs and transforming:

    df = ps.read_table('elexon.p315_p277')[['zpd', 'hdr', 'pool', 'gsp', 'a', 'b',
                                            # Check README.
                                            'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm']]
    df['pool'] = ps.to_datetime(df['pool'].str.slice(stop=8), format='%Y%m%d')
    df['zpd'] = ps.to_datetime(df['zpd'], format='%Y%m%d')
    df['e'] = ps.to_numeric(df['e'])
    df['e'] = df['e'].astype('int32')
    df['f'] = ps.to_numeric(df['f'])
    df['g'] = ps.to_numeric(df['g'])
    df['h'] = ps.to_numeric(df['h'])
    df['i'] = ps.to_numeric(df['i'])
    df['j'] = ps.to_numeric(df['j'])
    df['k'] = ps.to_numeric(df['k'])
    df['l'] = ps.to_numeric(df['l'])
    df['m'] = ps.to_numeric(df['m'])

    df = df.rename(columns={"zpd": "Date",
                            "hdr": "Settlement Run",
                            "gsp": "GSP Area",
                            "pool": "DATA Date",
                            "a": "Profile Class Id",
                            "b": "Standard Settlement Configuration ID",
                            "c": "Line Loss Factor Class ID",
                            "d": "Distributor ID",
                            "e": "Time Pattern Regime",
                            "f": "SPM Total EAC Report Value",
                            "g": "SPM Total EAC MSID Count",
                            "h": "SPM Total Annualised Advance Report Value",
                            "i": "SPM Total AA MSID Count",
                            "j": "SPM Total Unmetered Consumption Report Value",
                            "k": "SPM Total Unmetered MSID Count",
                            "l": "SPM Default EAC MSID Count",
                            "m": "SPM Default Unmetered MSID Count"})

    settlement = ps.DataFrame.from_dict(sr_to_int, orient='index')
    settlement = settlement.reset_index().rename(
        columns={'index': 'Settlement Run', 0: 'Settlement Value'})
    df = ps.merge(df, settlement, how='left')
    df = ps.merge(df, df.groupby(['Date']).max().reset_index()[
                  ['Date', 'Settlement Value']],  how='left', left_on=['Date'], right_on=['Date'])
    df['Last Run Flag'] = (df['Settlement Value_x'] ==
                           df['Settlement Value_y'])
    df = df.drop(columns=['Settlement Value_x', 'Settlement Value_y'])
    df = df.drop_duplicates()
    set_per = ps.DataFrame({"Settlement Period": range(1, 51), "temp": 1})
    df['tempo'] = 1
    df = ps.merge(df, set_per, how='left', left_on="tempo", right_on="temp")
    df = df.drop(columns=['tempo', 'temp'])

    calendar = ps.from_pandas(get_holidays_calendar())
    df = df.rename(columns={'Line Loss Factor Class ID': 'LLF_ID',
                   'Distributor ID': 'DNO', 'Settlement Period': 'STL_PERIOD'})
    ps.set_option("compute.ops_on_diff_frames", True)
    df = ps.merge(df, calendar[['WD_Date', 'WD']],
                  how='left', left_on='Date', right_on='WD_Date')
    df['WD'] = df['WD'].fillna('W')
    df.rename(columns={'WD': 'WD_BH'}, inplace=True)
    df = df.drop(columns='WD_Date')
    df["WD_BH"] = df["WD_BH"].transform(string_to_numeric)
    df['WD_AUX'] = df['Date'].transform(aux_date_func)
    df["WD"] = df["WD_BH"] + df["WD_AUX"]
    df = df.drop(columns=["WD_BH", "WD_AUX"])
    df["WD"] = df["WD"].transform(numeric_to_string)
    ps.reset_option("compute.ops_on_diff_frames")

    tlm_total = ps.read_table('elexon_etl.tlm')  # From mdd_tables sub-package
    tlm_total['Settlement_Date'] = tlm_total['Settlement_Date'].astype(
        'datetime64[ns]')
    tlm_group = tlm_total.drop(columns="Delivering")

    gcf = ps.read_table('elexon_etl.gcf')  # From mdd_tables sub-package
    min_tlm = tlm_group['Settlement_Date'].min()
    max_tlm = tlm_group['Settlement_Date'].max()
    gcf = gcf[(gcf['Settlement_Date'] >= min_tlm) & (
        gcf['Settlement_Date'] <= max_tlm)].reset_index(drop=True)

    pc = ps.read_table('elexon_etl.pc')  # From mdd_tables sub-package
    columns = list(pc.columns)
    for i in columns:
        pc.rename(columns={i: i.replace("PPC_", "")}, inplace=True)
    pc = ps.melt(pc, id_vars=["PROFILE_CLASS_ID", "GSP_GROUP_ID", "STD_SETTLEMENT_CONFIG_ID", "TIME_PATTERN_REGIME_ID", "SETTLEMENT_DATE"],
                 var_name="SETTLEMENT_PERIOD", value_name="PC")
    pc['SETTLEMENT_PERIOD'] = pc['SETTLEMENT_PERIOD'].astype('int32')

    # Adding TLMs to D0277
    df = ps.merge(df, tlm_group[['Settlement_Date',	'Settlement_Period', 'Off_Taking', 'gsp_area']], how='left', left_on=[
                  'Date', 'STL_PERIOD', 'GSP Area'], right_on=['Settlement_Date', 'Settlement_Period', 'gsp_area'])
    df = df.drop(columns=['Settlement_Date', 'Settlement_Period', 'gsp_area'])
    # Adding GCFs to D0277
    df = ps.merge(df, gcf[['Settlement_Date', 'Settlement_Period', 'GSP_area', 'GCF']], how='left', left_on=[
                  'Date', 'STL_PERIOD', 'GSP Area'], right_on=['Settlement_Date', 'Settlement_Period', 'GSP_area'])
    df = df.drop(columns=['Settlement_Date', 'Settlement_Period', 'GSP_area'])
    # Adding Profile Class Coefficients to D0277
    df = ps.merge(df, pc, how='left',
                  left_on=['Profile Class Id', 'GSP Area', 'Standard Settlement Configuration ID',
                           'Time Pattern Regime', 'Date', 'STL_PERIOD'],
                  right_on=['PROFILE_CLASS_ID', 'GSP_GROUP_ID', 'STD_SETTLEMENT_CONFIG_ID', 'TIME_PATTERN_REGIME_ID', 'SETTLEMENT_DATE', 'SETTLEMENT_PERIOD'])
    df = df.drop(columns=['PROFILE_CLASS_ID', 'GSP_GROUP_ID', 'STD_SETTLEMENT_CONFIG_ID',
                 'TIME_PATTERN_REGIME_ID', 'SETTLEMENT_DATE', 'SETTLEMENT_PERIOD'])

    llf = ps.read_table('elexon_etl.llf')  # From mdd_tables sub-package
    llf.rename(columns={'VALUE': 'LLF'}, inplace=True)

    df = ps.sql('''
    SELECT * 
    FROM {df} df LEFT JOIN {llf} llf
    ON df.Date >= llf.START_SETTLEMENT_DATE
    AND df.Date <= llf.END_SETTLEMENT_DATE
    AND df.STL_PERIOD >= llf.START_SETTLEMENT_PERIOD
    AND df.STL_PERIOD <= llf.END_SETTLEMENT_PERIOD
    AND df.LLF_ID = llf.LLF_CLASS_ID 
    AND df.DNO = llf.MARKET_PARTICIPANT_ID 
    AND df.WD = llf.DAY_TYPE''',
                df=df,
                llf=llf)

    df = df.rename(columns={'LLF_ID': 'Line Loss Factor Class ID',
                   'DNO': 'Distributor ID', 'STL_PERIOD': 'Settlement Period'})
    df = df.drop(columns=['MARKET_PARTICIPANT_ID', 'LLF_CLASS_ID', 'START_SETTLEMENT_DATE',
                 'END_SETTLEMENT_DATE', 'WD', 'DAY_TYPE', 'START_SETTLEMENT_PERIOD', 'END_SETTLEMENT_PERIOD'])
    dic = {
        1: 'Domestic',
        2: 'Domestic',
        3: 'Non-domestic',
        4: 'Non-domestic',
        5: 'Non-domestic',
        6: 'Non-domestic',
        7: 'Non-domestic',
        8: 'Non-domestic'
    }
    pc = ps.DataFrame.from_dict(dic, orient='index')
    pc = pc.reset_index().rename(
        columns={'index': 'Profile_Class_ID', 0: 'Sector'})
    df = ps.merge(df, pc, how='left', left_on=[
                  'Profile Class Id'], right_on=['Profile_Class_ID'])
    df = df.drop(columns='Profile_Class_ID')
    df['PC_corrected'] = df['PC'] * df['GCF'] * df['Off_Taking']
    df = df.drop(columns=['GCF', 'Off_Taking'])

    ps.set_option("compute.ops_on_diff_frames", True)
    df['Peak'] = df['Settlement Period'].transform(peak_func)
    df.dropna().reset_index(drop=True)
    ps.reset_option("compute.ops_on_diff_frames")

    df['EAC [GWh]'] = df['SPM Total EAC Report Value']/1000*df['PC']
    df['ELoss [GWh]'] = df['SPM Total EAC Report Value'] / \
        1000*df['PC']*(df['LLF']-1)
    df['Corrected EAC [GWh]'] = df['SPM Total EAC Report Value'] / \
        1000*df['PC_corrected']
    df['Corrected ELoss [GWh]'] = df['SPM Total EAC Report Value'] / \
        1000*df['PC_corrected']*(df['LLF']-1)

    df['Annualised Advance [GWh]'] = df['SPM Total Annualised Advance Report Value']/1000*df['PC']
    df['Advance Loss [GWh]'] = df['SPM Total Annualised Advance Report Value'] / \
        1000*df['PC']*(df['LLF']-1)
    df['Corrected Annualised Advance [GWh]'] = df['SPM Total Annualised Advance Report Value'] / \
        1000*df['PC_corrected']
    df['Corrected Advance Loss [GWh]'] = df['SPM Total Annualised Advance Report Value'] / \
        1000*df['PC_corrected']*(df['LLF']-1)

    df['Unmetered [GWh]'] = df['SPM Total Unmetered Consumption Report Value']/1000*df['PC']
    df['Unmetered Loss [GWh]'] = df['SPM Total Unmetered Consumption Report Value'] / \
        1000*df['PC']*(df['LLF']-1)
    df['Corrected Unmetered [GWh]'] = df['SPM Total Unmetered Consumption Report Value'] / \
        1000*df['PC_corrected']
    df['Corrected Unmetered Loss [GWh]'] = df['SPM Total Unmetered Consumption Report Value'] / \
        1000*df['PC_corrected']*(df['LLF']-1)

    df = df.rename(columns={'SPM Total EAC MSID Count': 'EAC MSID Count',
                   'SPM Total AA MSID Count': 'AA MSID Count', 'SPM Total Unmetered MSID Count': 'Unmetered MSID Count'})
    df = df.dropna()

    df_final = df.groupby(by=['Date', 'DATA Date', 'Last Run Flag', 'Settlement Run',
                          'Settlement Period', 'Peak', 'GSP Area', 'Profile Class Id', 'Sector']).sum().reset_index()
    df_final = df_final.drop(columns=['LLF'])
    df_final['EAC_Unmetered [GWh]'] = df_final['EAC [GWh]'] + \
        df_final['Unmetered [GWh]']
    df_final['Estimated Line Loss [GWh]'] = df_final['ELoss [GWh]'] + \
        df_final['Unmetered Loss [GWh]']
    df_final['Corrected EAC_Unmetered [GWh]'] = df_final['Corrected EAC [GWh]'] + \
        df_final['Corrected Unmetered [GWh]']
    df_final['Corrected Estimated Line Loss [GWh]'] = df_final['Corrected ELoss [GWh]'] + \
        df_final['Corrected Unmetered Loss [GWh]']
    df_final['EAC_Unmetered MSID Count'] = df_final['EAC MSID Count'] + \
        df_final['Unmetered MSID Count']
    df_final = df_final.reset_index(drop=True)

    df_estimated = df_final[['Date', 'DATA Date', 'Last Run Flag', 'Settlement Run', 'Settlement Period', 'Peak', 'GSP Area', 'Sector', 'Profile Class Id',
                             'EAC_Unmetered [GWh]', 'Estimated Line Loss [GWh]', 'EAC_Unmetered MSID Count', 'Corrected EAC_Unmetered [GWh]', 'Corrected Estimated Line Loss [GWh]']]
    df_estimated = df_estimated.rename(columns={"EAC_Unmetered [GWh]": "Consumption [GWh]", "Estimated Line Loss [GWh]": "Line Loss [GWh]", 'EAC_Unmetered MSID Count': "MSID Count",
                                       'Corrected EAC_Unmetered [GWh]': 'Corrected Consumption [GWh]', 'Corrected Estimated Line Loss [GWh]': 'Corrected Line Loss [GWh]'})
    df_estimated['Data Quality'] = 'Estimate'

    df_advance = df_final[['Date', 'DATA Date', 'Last Run Flag', 'Settlement Run', 'Settlement Period', 'Peak', 'GSP Area', 'Sector', 'Profile Class Id',
                           'Annualised Advance [GWh]', 'Advance Loss [GWh]', 'AA MSID Count', 'Corrected Annualised Advance [GWh]', 'Corrected Advance Loss [GWh]']]
    df_advance = df_advance.rename(columns={"Annualised Advance [GWh]": "Consumption [GWh]", "Advance Loss [GWh]": "Line Loss [GWh]", "AA MSID Count": "MSID Count",
                                   'Corrected Annualised Advance [GWh]': 'Corrected Consumption [GWh]', 'Corrected Advance Loss [GWh]': 'Corrected Line Loss [GWh]'})
    df_advance['Data Quality'] = 'Actual'

    df_hh = ps.concat([df_estimated, df_advance])
    df_hh = df_hh.sort_values('Date')
    df_hh = df_hh.reset_index(drop=True)
    df_hh = df_hh.groupby(by=['Date', 'DATA Date', 'Last Run Flag', 'Settlement Run',
                          'Settlement Period', 'Peak', 'GSP Area', 'Sector', 'Profile Class Id', 'Data Quality']).sum()
    df_hh['Metering'] = 'NHH'
    df_hh = df_hh.drop_duplicates()
    df_hh = df_hh[df_hh['Consumption [GWh]'] != 0]
    df_hh = df_hh.reset_index()

    columns = list(df_hh.columns)
    for i in columns:
        df_hh.rename(columns={i: i.replace(" ", "_").lower()}, inplace=True)
        df_hh.rename(columns={
            'profile_class_id': 'description',
            'data_date': 'file_date',
            'data_quality': 'actual_estimate',
            'metering': 'aggregation_type',
            'consumption_[gwh]': 'uncorrected_consumption_gwh',
            'line_loss_[gwh]': 'uncorrected_line_loss_gwh',
            'corrected_consumption_[gwh]': 'corrected_consumption_gwh',
            'corrected_line_loss_[gwh]': 'corrected_line_loss_gwh',
            'msid_count': 'meters_count'
        }, inplace=True)

    df_hh['total_corrected_consumption_gwh'] = df_hh['corrected_consumption_gwh'] + \
        df_hh['corrected_line_loss_gwh']
    df_hh['total_uncorrected_consumption_gwh'] = df_hh['uncorrected_consumption_gwh'] + \
        df_hh['uncorrected_line_loss_gwh']

    df_hh['actual_estimate'] = df_hh['actual_estimate'].str.slice(stop=1)
    df_hh['aggregation_type'] = df_hh['aggregation_type'].str.slice(stop=1)

    # Half-Hourly data, all runs:
    save_path = "/tmp/delta/277_all_runs"
    sdf_hh = df_hh.to_spark()
    sdf_hh.write.format("delta").save(save_path)
    # DATABRICKS: Saved as elexon_t.d277_hh_all_runs

    # Half-hourly data, final run.
    df_hh = df_hh[df_hh['last_run_flag'] == True]
    df_hh = df_hh.drop(columns=['last_run_flag', 'file_date'])
    save_path = "/tmp/delta/277_hh"
    sdf_hh = df_hh.to_spark()
    sdf_hh.write.format("delta").save(save_path)
    # DATABRICKS: Saved as elexon_t.d277_hh

    del df_hh
    del sdf_hh

    # Daily Last Run
    df_daily = ps.sql("""
        SELECT 
        date,
        settlement_run, 
        gsp_area,
        aggregation_type,
        description, 
        sector, 
        actual_estimate, 
        SUM(uncorrected_consumption_gwh) as uncorrected_consumption_gwh,
        SUM(uncorrected_line_loss_gwh) as uncorrected_line_loss_gwh,
        SUM(corrected_consumption_gwh) as corrected_consumption_gwh,
        SUM(corrected_line_loss_gwh) as corrected_line_loss_gwh,
        MEAN(meters_count) as meters_count,
        SUM(total_corrected_consumption_gwh) as total_corrected_consumption_gwh,
        SUM(total_uncorrected_consumption_gwh) as total_uncorrected_consumption_gwh

        FROM elexon_t.d277_hh D277

        GROUP BY
        date,
        settlement_run, 
        gsp_area,
        aggregation_type,
        description, 
        sector, 
        actual_estimate
        """)
    save_path = "/tmp/delta/277_daily"
    df_daily.to_spark().write.format("delta").option(
        'OverwriteSchema', True).mode("overwrite").save(save_path)
    # DATABRICKS: Saved as elexon_t.d277_daily

    del df_daily

    # Daily Peak Last Run
    df_peak = ps.sql("""
        SELECT 
        date,
        settlement_run, 
        gsp_area,
        aggregation_type,
        description, 
        sector, 
        actual_estimate, 
        SUM(uncorrected_consumption_gwh) as uncorrected_consumption_gwh,
        SUM(uncorrected_line_loss_gwh) as uncorrected_line_loss_gwh,
        SUM(corrected_consumption_gwh) as corrected_consumption_gwh,
        SUM(corrected_line_loss_gwh) as corrected_line_loss_gwh,
        MEAN(meters_count) as meters_count,
        SUM(total_corrected_consumption_gwh) as total_corrected_consumption_gwh,
        SUM(total_uncorrected_consumption_gwh) as total_uncorrected_consumption_gwh

        FROM elexon_t.d277_hh D277

        WHERE
        peak = 'P'

        GROUP BY
        date,
        settlement_run, 
        gsp_area,
        aggregation_type,
        description, 
        sector, 
        actual_estimate
        """)

    save_path = "/tmp/delta/277_peak"
    df_peak.to_spark().write.format("delta").option(
        'OverwriteSchema', True).mode("overwrite").save(save_path)
    # DATABRICKS: Saved as elexon_t.d277_peak

    del df_peak
