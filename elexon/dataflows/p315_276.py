from ..utils_others.utils import sr_to_int, ccc_imports_dic, ccc_exports_dic, peak_func_from_sp
import os
import pandas as pd
import numpy as np
import pyspark.pandas as ps
import datetime
import sys
from pyspark.sql.functions import *
from pyspark.sql.types import *
from databricks.sdk.runtime import *


def load_elexon_p315_276():

    # Getting the P315 - 276 Dataflow adding TLMs and transforming:
    df = ps.read_table('elexon.p315')[
        # Check README.
        ['zpd', 'pool', 'hdr', 'gsp', 'ccc', 'spx', 'a', 'h', 'm', 'c', 'ai']]
    df['pool'] = ps.to_datetime(df['pool'].str.slice(stop=8), format='%Y%m%d')
    df['zpd'] = ps.to_datetime(df['zpd'], format='%Y%m%d')
    df['a'] = ps.to_numeric(df['a'])
    df['h'] = ps.to_numeric(df['h'])
    df['m'] = ps.to_numeric(df['m'])
    df['c'] = ps.to_numeric(df['c'])

    df = df.rename(columns={"zpd": "Date", "hdr": "Settlement Run", "gsp": "GSP Area", "ccc": "CCC", "spx": "Settlement Period",
                   "a": "Aggregated Supplier Consumption [MWh]", "h": "Aggregated Supplier Line Loss [MWh]", "m": "Corrected Aggregated Supplier Consumption [MWh]", "c": "Corrected Aggregated Supplier Line Loss [MWh]", "ai": "Total CCC MSID (meters) Count", "pool": "File Date"})

    # NO Active Export, NO NHH meters.
    df = df[~((df['CCC'] >= 6) & (df['CCC'] <= 8))]
    df = df[~((df['CCC'] >= 14) & (df['CCC'] <= 22))]
    df = df[~((df['CCC'] >= 32) & (df['CCC'] <= 41))]
    df = df[~((df['CCC'] >= 48) & (df['CCC'] <= 53))]
    df = df[~((df['CCC'] >= 60) & (df['CCC'] <= 65))]

    df = df[df['Settlement Run'].isin(['SF', 'RF', 'DF'])]
    settlement = ps.DataFrame.from_dict(sr_to_int, orient='index')
    settlement = settlement.reset_index().rename(
        columns={'index': 'Settlement Run', 0: 'Settlement Value'})
    df = ps.merge(df, settlement, how='left')
    df = ps.merge(df, df.groupby(['Date', 'Settlement Period']).max().reset_index()[['Date', 'Settlement Period',
                  'Settlement Value']],  how='left', left_on=['Date', 'Settlement Period'], right_on=['Date', 'Settlement Period'])
    df['Last Run Flag'] = (df['Settlement Value_x'] ==
                           df['Settlement Value_y'])
    df = df.drop(columns=['Settlement Value_x', 'Settlement Value_y'])
    df = df.reset_index(drop=True)
    df['Data Aggregation Type'] = 'H'
    df = df.drop_duplicates()

    tlm_total = ps.read_table('elexon_etl.tlm')  # From mdd_tables sub-package
    tlm_total['Settlement_Date'] = tlm_total['Settlement_Date'].astype(
        'datetime64[ns]')
    df = ps.merge(df, tlm_total,  how='left', left_on=['Date', 'GSP Area', 'Settlement Period'], right_on=[
                  'Settlement_Date', 'gsp_area', 'Settlement_Period'])
    df = df.drop(columns=['Settlement_Date',
                 'Settlement_Run_Type', 'gsp_area'])
    df = df.reset_index(drop=True)
    df['Corrected Aggregated Supplier Consumption [MWh]'] = df['Corrected Aggregated Supplier Consumption [MWh]']*df['Off_Taking']/1000
    df['Corrected Aggregated Supplier Line Loss [MWh]'] = df['Corrected Aggregated Supplier Line Loss [MWh]']*df['Off_Taking']/1000
    df['Aggregated Supplier Consumption [MWh]'] = df['Aggregated Supplier Consumption [MWh]']/1000
    df['Aggregated Supplier Line Loss [MWh]'] = df['Aggregated Supplier Line Loss [MWh]']/1000
    df['total_corrected_consumption_[gwh]'] = df['Corrected Aggregated Supplier Consumption [MWh]'].fillna(
        0) + df['Corrected Aggregated Supplier Line Loss [MWh]'].fillna(0)
    df['total_uncorrected_consumption_[gwh]'] = df['Aggregated Supplier Consumption [MWh]'].fillna(
        0) + df['Aggregated Supplier Line Loss [MWh]'].fillna(0)

    ccc = ps.DataFrame.from_dict(ccc_imports_dic, orient='index')
    ccc = ccc.reset_index().rename(columns={
        'index': 'CCC', 0: 'Metering Description', 1: 'Sector', 2: 'Actual/Estimated Indicator'})
    df = ps.merge(df, ccc, how='left')
    df = df.drop(columns=['CCC'])

    df = df.rename(columns={
        'Metering Description': 'description',
        'File Date': 'file_date',
        'Actual/Estimated Indicator': 'actual/estimate',
        'Data Aggregation Type': 'aggregation_type',
        'Aggregated Supplier Consumption [MWh]': 'uncorrected_consumption_[gwh]',
        'Aggregated Supplier Line Loss [MWh]': 'uncorrected_line_loss_[gwh]',
        'Corrected Aggregated Supplier Consumption [MWh]': 'corrected_consumption_[gwh]',
        'Corrected Aggregated Supplier Line Loss [MWh]': 'corrected_line_loss_[gwh]',
        'Total CCC MSID (meters) Count': 'meters_count'
    })

    df_hh = df.drop(columns=['Settlement_Period', 'Off_Taking', 'Delivering'])

    df_hh['Peak'] = df_hh['Settlement Period'].apply(peak_func_from_sp)

    columns = list(df_hh.columns)

    for i in columns:
        df_hh.rename(columns={i: i.replace(" ", "_").lower()}, inplace=True)
        df_hh.rename(columns={i: i.replace("/", "_")}, inplace=True)

    df_hh.rename(columns={
        'uncorrected_consumption_[gwh]': 'uncorrected_consumption_gwh',
        'uncorrected_line_loss_[gwh]': 'uncorrected_line_loss_gwh',
        'corrected_consumption_[gwh]': 'corrected_consumption_gwh',
        'corrected_line_loss_[gwh]': 'corrected_line_loss_gwh',
        'total_corrected_consumption_[gwh]': 'total_corrected_consumption_gwh',
        'total_uncorrected_consumption_[gwh]': 'total_uncorrected_consumption_gwh'
    }, inplace=True)

    df_hh = df_hh.groupby(by=['date', 'file_date', 'last_run_flag', 'settlement_run', 'gsp_area',
                          'settlement_period', 'aggregation_type', 'description', 'sector', 'actual_estimate', 'peak']).sum()
    df_hh = df_hh.reset_index()

    # Half-hourly data, all runs.
    save_path = "/tmp/delta/276_all_runs"
    sdf_hh = df_hh.to_spark()
    sdf_hh.write.format("delta").save(save_path)
    # DATABRICKS: Saved as elexon_t.d276_hh_all_runs

    # Half-hourly data, final run.
    df_hh = df_hh[df_hh['last_run_flag'] == True]
    df_hh = df_hh.drop(columns=['last_run_flag', 'file_date'])
    save_path = "/tmp/delta/276_hh"
    sdf_hh = df_hh.to_spark()
    sdf_hh.write.format("delta").save(save_path)
    # DATABRICKS: Saved as elexon_t.d276_hh

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

        FROM elexon_t.d276_hh D276

        GROUP BY
        date,
        settlement_run, 
        gsp_area,
        aggregation_type,
        description, 
        sector, 
        actual_estimate
       """)
    save_path = "/tmp/delta/276_daily"
    df_daily.to_spark().write.format("delta").option(
        'OverwriteSchema', True).mode("overwrite").save(save_path)
    # DATABRICKS: Saved as elexon_t.d276_daily

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

        FROM elexon_t.d276_hh D276

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
    save_path = "/tmp/delta/276_peak"
    df_peak.to_spark().write.format("delta").option(
        'OverwriteSchema', True).mode("overwrite").save(save_path)
    # DATABRICKS: Saved as elexon_t.d276_peak

    del df_peak


def load_elexon_p315_276_exports():
    df = ps.read_table('elexon.p315')[  # Check README.
        ['zpd', 'pool', 'hdr', 'gsp', 'ccc', 'spx', 'a', 'h', 'm', 'c', 'ai']]
    df['pool'] = ps.to_datetime(df['pool'].str.slice(stop=8), format='%Y%m%d')
    df['zpd'] = ps.to_datetime(df['zpd'], format='%Y%m%d')
    df['a'] = ps.to_numeric(df['a'])
    df['h'] = ps.to_numeric(df['h'])
    df['m'] = ps.to_numeric(df['m'])
    df['c'] = ps.to_numeric(df['c'])

    df = df.rename(columns={"zpd": "Date", "hdr": "Settlement Run", "gsp": "GSP Area", "ccc": "CCC", "spx": "Settlement Period",
                   "a": "Aggregated Supplier Consumption [MWh]", "h": "Aggregated Supplier Line Loss [MWh]", "m": "Corrected Aggregated Supplier Consumption [MWh]", "c": "Corrected Aggregated Supplier Line Loss [MWh]", "ai": "Total CCC MSID (meters) Count", "pool": "File Date"})
    # Embedded Exports
    df = df[~((df['CCC'] >= 1) & (df['CCC'] <= 5))]
    df = df[~((df['CCC'] >= 9) & (df['CCC'] <= 13))]
    df = df[~((df['CCC'] >= 17) & (df['CCC'] <= 31))]
    df = df[~((df['CCC'] >= 42) & (df['CCC'] <= 47))]
    df = df[~((df['CCC'] >= 54) & (df['CCC'] <= 59))]
    settlement = ps.DataFrame.from_dict(sr_to_int, orient='index')
    settlement = settlement.reset_index().rename(
        columns={'index': 'Settlement Run', 0: 'Settlement Value'})
    df = ps.merge(df, settlement, how='left')
    df = ps.merge(df, df.groupby(['Date', 'Settlement Period']).max().reset_index()[['Date', 'Settlement Period',
                  'Settlement Value']],  how='left', left_on=['Date', 'Settlement Period'], right_on=['Date', 'Settlement Period'])
    df['Last Run Flag'] = (df['Settlement Value_x'] ==
                           df['Settlement Value_y'])
    df = df.drop(columns=['Settlement Value_x', 'Settlement Value_y'])
    df = df[df['Last Run Flag'] == True]
    df = df.drop(columns=['Last Run Flag'])
    df = df.reset_index(drop=True)
    df = df.drop_duplicates()
    tlm_total = ps.read_table('elexon_etl.tlm')  # From mdd_tables sub-package
    tlm_total['Settlement_Date'] = tlm_total['Settlement_Date'].astype(
        'datetime64[ns]')
    df = ps.merge(df, tlm_total,  how='left', left_on=['Date', 'GSP Area', 'Settlement Period'], right_on=[
                  'Settlement_Date', 'gsp_area', 'Settlement_Period'])
    df = df.drop(columns=['Settlement_Date',
                 'Settlement_Run_Type', 'gsp_area'])
    df = df.reset_index(drop=True)
    df['Corrected Aggregated Supplier Consumption [MWh]'] = df['Corrected Aggregated Supplier Consumption [MWh]']/1000
    df['Corrected Aggregated Supplier Line Loss [MWh]'] = df['Corrected Aggregated Supplier Line Loss [MWh]']/1000
    df['Aggregated Supplier Consumption [MWh]'] = df['Aggregated Supplier Consumption [MWh]']/1000
    df['Aggregated Supplier Line Loss [MWh]'] = df['Aggregated Supplier Line Loss [MWh]']/1000
    df['total_corrected_consumption_[gwh]'] = df['Corrected Aggregated Supplier Consumption [MWh]'].fillna(
        0) + df['Corrected Aggregated Supplier Line Loss [MWh]'].fillna(0)
    df['total_uncorrected_consumption_[gwh]'] = df['Aggregated Supplier Consumption [MWh]'].fillna(
        0) + df['Aggregated Supplier Line Loss [MWh]'].fillna(0)
    df['total_tlm_corrected_consumption_gwh'] = (df['Corrected Aggregated Supplier Consumption [MWh]'].fillna(
        0) + df['Corrected Aggregated Supplier Line Loss [MWh]'].fillna(0))*df['Off_Taking']
    ccc = ps.DataFrame.from_dict(ccc_exports_dic, orient='index')
    ccc = ccc.reset_index().rename(columns={
        'index': 'CCC', 0: 'Metering Description', 1: 'Sector', 2: 'Actual/Estimated Indicator'})
    df = ps.merge(df, ccc, how='left')
    df = df.drop(columns=['File Date'])
    df = df.rename(columns={
        'Metering Description': 'description',
        'Actual/Estimated Indicator': 'actual/estimate',
        'Aggregated Supplier Consumption [MWh]': 'uncorrected_consumption_[gwh]',
        'Aggregated Supplier Line Loss [MWh]': 'uncorrected_line_loss_[gwh]',
        'Corrected Aggregated Supplier Consumption [MWh]': 'corrected_consumption_[gwh]',
        'Corrected Aggregated Supplier Line Loss [MWh]': 'corrected_line_loss_[gwh]',
        'Total CCC MSID (meters) Count': 'meters_count',
        'CCC': 'ccc'
    })
    df_hh = df.drop(columns=['Settlement_Period', 'Off_Taking', 'Delivering'])
    df_hh['Peak'] = df_hh['Settlement Period'].apply(peak_func_from_sp)
    columns = list(df_hh.columns)
    for i in columns:
        df_hh.rename(columns={i: i.replace(" ", "_").lower()}, inplace=True)
        df_hh.rename(columns={i: i.replace("/", "_")}, inplace=True)
    df_hh.rename(columns={
        'uncorrected_consumption_[gwh]': 'uncorrected_consumption_gwh',
        'uncorrected_line_loss_[gwh]': 'uncorrected_line_loss_gwh',
        'corrected_consumption_[gwh]': 'corrected_consumption_gwh',
        'corrected_line_loss_[gwh]': 'corrected_line_loss_gwh',
        'total_corrected_consumption_[gwh]': 'total_corrected_consumption_gwh',
        'total_uncorrected_consumption_[gwh]': 'total_uncorrected_consumption_gwh'
    }, inplace=True)
    df_hh = df_hh.groupby(by=['date', 'settlement_run', 'gsp_area', 'settlement_period',
                          'description', 'sector', 'actual_estimate', 'peak', 'ccc']).sum()
    df_hh = df_hh.reset_index()
    save_path = "/tmp/delta/276_exports_hh"
    sdf_hh = df_hh.to_spark()
    sdf_hh.write.format("delta").save(save_path)
    # DATABRICKS: Saved as elexon_f.p315_embedded_sva

    del df_hh
    del sdf_hh
