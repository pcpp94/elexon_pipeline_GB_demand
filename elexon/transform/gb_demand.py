import os
import pandas as pd
import numpy as np
import pyspark.pandas as ps
import datetime
import sys
from pyspark.sql.functions import *
from pyspark.sql.types import *
from databricks.sdk.runtime import *


def transform_elexon_p315_total():

    save_path = "/tmp/delta/315_all_runs"
    # From dataflows sub-package
    df_276 = ps.read_table('elexon_t.d276_hh_all_runs')
    # From dataflows sub-package
    df_277 = ps.read_table('elexon_t.d277_hh_all_runs')
    df_315 = ps.concat([df_276, df_277])
    df_315 = df_315[df_315['date'] >= '2015-01-01']
    sdf_315 = df_315.to_spark()
    sdf_315.write.format("delta").save(save_path)
    # DATABRICKS: Saved as Table: elexon_f.p315_hh_all_runs

    save_path = "/tmp/delta/315_hh"
    df_276 = ps.read_table('elexon_t.d276_hh')  # From dataflows sub-package
    df_277 = ps.read_table('elexon_t.d277_hh')  # From dataflows sub-package
    df_315_hh = ps.concat([df_276, df_277])
    df_315_hh = df_315_hh[df_315_hh['date'] >= '2015-01-01']
    sdf_315_hh = df_315_hh.to_spark()
    sdf_315_hh.write.format("delta").save(save_path)
    # DATABRICKS: Saved as Table: elexon_f.p315_hh

    save_path = "/tmp/delta/315_daily"
    df_276 = ps.read_table('elexon_t.d276_daily')  # From dataflows sub-package
    df_277 = ps.read_table('elexon_t.d277_daily')  # From dataflows sub-package
    df_315_daily = ps.concat([df_276, df_277])
    df_315_daily = df_315_daily[df_315_daily['date'] >= '2015-01-01']
    sdf_315_daily = df_315_daily.to_spark()
    sdf_315_daily.write.format("delta").save(save_path)
    # DATABRICKS: Saved as Table: elexon_f.p315_daily

    save_path = "/tmp/delta/315_peak"
    df_276 = ps.read_table('elexon_t.d276_peak')  # From dataflows sub-package
    df_277 = ps.read_table('elexon_t.d277_peak')  # From dataflows sub-package
    df_315_peak = ps.concat([df_276, df_277])
    df_315_peak = df_315_peak[df_315_peak['date'] >= '2015-01-01']
    sdf_315_peak = df_315_peak.to_spark()
    sdf_315_peak.write.format("delta").save(save_path)
    # DATABRICKS: Saved as Table: elexon_f.p315_peak


def transform_elexon_gross_demand():

    save_path = "/tmp/delta/gross_demand_hh/"

    p315 = ps.read_table('elexon_f.p315_hh')  # From this script
    t_demand = ps.read_table(
        # From dataflows sub-package
        'elexon_f.p114_big_demand_lr').drop(columns='details')
    start_date = p315['date'].min()
    end_date = p315['date'].max()
    t_demand = t_demand[(t_demand['date'] >= start_date)
                        & (t_demand['date'] <= end_date)]
    gross_demand_hh = ps.concat([p315, t_demand])
    gross_demand_hh.to_spark().write.format("delta").save(save_path)
    # The T-connected demand is missing from the 277 and 276 dataflows so we add it here.
    # DATABRICKS: Saved as Table: elexon_f.gross_demand_hh

    gross_demand_daily = ps.sql("""
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
        SUM(meters_count) as meters_count,
        SUM(total_corrected_consumption_gwh) as total_corrected_consumption_gwh,
        SUM(total_uncorrected_consumption_gwh) as total_uncorrected_consumption_gwh

        FROM elexon_f.gross_demand_hh

        GROUP BY
        date,
        settlement_run, 
        gsp_area,
        aggregation_type,
        description, 
        sector, 
        actual_estimate
        """)

    save_path = "/tmp/delta/gross_demand_daily/"
    gross_demand_daily.write.format("delta").save(save_path)
    # DATABRICKS: Saved as Table: elexon_f.gross_demand_daily
