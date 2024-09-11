
from ..utils_others.utils import ro_fit_sr_dic, RO_dic, FIT_dic
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, month, dayofyear, dayofmonth, dayofweek, when, lit, to_date, udf, concat, substring, concat_ws, date_format, monotonically_increasing_id
from pyspark.sql.types import IntegerType, StringType
from datetime import timedelta, datetime
import pyspark.pandas as ps
import sys
from pyspark.pandas.config import set_option, get_option
from databricks.sdk.runtime import *


def transform_ro_fit():

    # Reading data as SQL
    df = spark.sql(""" SELECT date, settlement_run, gsp_area, settlement_period, aggregation_type,
    description, sector, actual_estimate, peak, uncorrected_consumption_gwh,
            uncorrected_line_loss_gwh, total_uncorrected_consumption_gwh
      FROM elexon_t.d276_hh_all_runs
      WHERE settlement_run = 'SF'
  """)  # From dataflows sub-package
    # Date to Date
    df = df.withColumn('date', to_date(df['date']))
    # Adding new columns 'last_run_ro' and 'last_run_fit' with value True
    df = df.withColumn('last_run_ro', lit(True)).withColumn(
        'last_run_fit', lit(True))
    # Min and Max Date
    min_1 = df.select("date").orderBy("date", ascending=True).first()[0]
    max_1 = df.select("date").orderBy("date", ascending=False).first()[0]

    # Running the SQL query to get the data into DataFrame df_t
    df_t = spark.sql("""
      SELECT date, settlement_run, gsp_area, settlement_period, aggregation_type,
            description, sector, actual_estimate, peak, uncorrected_consumption_gwh,
            uncorrected_line_loss_gwh, total_uncorrected_consumption_gwh
      FROM elexon_t.big_demand_all
      WHERE settlement_run IN ('II', 'SF', 'R1', 'R2', 'R3')
  """)  # From dataflows sub-package
    # Date to Date
    df_t = df_t.withColumn('date', to_date(df_t['date']))
    # Min and Max Date
    min_aux_1 = df_t.select("date").orderBy("date", ascending=True).first()[0]
    max_aux_1 = df_t.select("date").orderBy("date", ascending=False).first()[0]

    # Running the SQL query to get the data into DataFrame df_2
    df_2 = spark.sql("""
      SELECT date, settlement_run, gsp_area, settlement_period, aggregation_type,
            description, sector, actual_estimate, peak, uncorrected_consumption_gwh,
            uncorrected_line_loss_gwh, total_uncorrected_consumption_gwh
      FROM elexon_t.d277_hh_all_runs
      WHERE settlement_run IN ('II', 'SF', 'R1', 'R2', 'R3')
  """)  # From dataflows sub-package
    # Date to Date
    df_2 = df_2.withColumn('date', to_date(df_2['date']))
    # Min and Max Date
    min_aux_2 = df_2.select("date").orderBy("date", ascending=True).first()[0]
    max_aux_2 = df_2.select("date").orderBy("date", ascending=False).first()[0]
    # Getting the Minimum and Maximum of both Aux DataFrames
    min_aux_ttl = max(min_aux_1, min_aux_2)
    max_aux_ttl = min(max_aux_1, max_aux_2)
    # Filter df_t and df_2 based on the date column
    df_t = df_t.filter((col("date") >= min_aux_ttl) &
                       (col("date") <= max_aux_ttl))
    df_2 = df_2.filter((col("date") >= min_aux_ttl) &
                       (col("date") <= max_aux_ttl))
    # Concatenate df_2 and df_t
    # In PySpark, you can use the union method to concatenate two DataFrames
    df_2 = df_2.union(df_t)
    # Getting the settlements dictionary into a DataFrame
    # Converting dictionary to list of tuples
    data = [(k, v) for k, v in ro_fit_sr_dic.items()]
    # Creating DataFrame from list of tuples
    settlement = spark.createDataFrame(
        data, ["Settlement Run", "settlement_value"])
    # Merge df_2 and settlement on settlement_run
    df_2 = df_2.join(settlement, df_2['settlement_run'] ==
                     settlement['Settlement Run'], 'left').drop('Settlement Run')
    # Get minimum and maximum date
    min_2 = df_2.agg({"date": "min"}).collect()[0][0]
    max_2 = df_2.agg({"date": "max"}).collect()[0][0]

    # User defined functions to compute 'ro' and 'fit' columns
    def compute_ro(month, value):
        return 1 if value <= RO_dic.get(month, 0) else 0

    def compute_fit(month, value):
        return 1 if value <= FIT_dic.get(month, 0) else 0

    udf_compute_ro = udf(compute_ro, IntegerType())
    udf_compute_fit = udf(compute_fit, IntegerType())
    # Create 'code' column and compute 'ro' and 'fit' columns
    df_2 = df_2.withColumn("ro", udf_compute_ro(
        month(col("date")), col("settlement_value")))
    df_2 = df_2.withColumn("fit", udf_compute_fit(
        month(col("date")), col("settlement_value")))
    # Create a new column 'flag' as the sum of 'ro' and 'fit'
    df_2 = df_2.withColumn("flag", col("ro") + col("fit"))
    # Drop the column 'code'
    df_2 = df_2.drop("code")
    # Filter rows where 'flag' is greater than or equal to 1
    df_2 = df_2.filter(col("flag") >= 1)
    # Create two new DataFrames df_2_ro and df_2_fit
    df_2_ro = df_2.filter(col("ro") == 1)
    df_2_fit = df_2.filter(col("fit") == 1)
    # Columns to group by
    groupby_columns = ['date', 'gsp_area', 'settlement_period',
                       'aggregation_type', 'description', 'actual_estimate']
    # Aggregating to find maximum settlement_value
    agg_df_2_ro = df_2_ro.groupBy(groupby_columns).agg(
        F.max("settlement_value").alias("max_settlement_value"))
    agg_df_2_fit = df_2_fit.groupBy(groupby_columns).agg(
        F.max("settlement_value").alias("max_settlement_value"))
    # Joining the aggregated data back to the original DataFrames
    df_2_ro = df_2_ro.join(agg_df_2_ro, on=groupby_columns, how='left')
    df_2_fit = df_2_fit.join(agg_df_2_fit, on=groupby_columns, how='left')
    # For df_2_ro
    df_2_ro = df_2_ro.withColumn("last_run_ro", F.col(
        "settlement_value") == F.col("max_settlement_value"))
    df_2_ro = df_2_ro.filter(F.col("last_run_ro"))
    # For df_2_fit
    df_2_fit = df_2_fit.withColumn("last_run_fit", F.col(
        "settlement_value") == F.col("max_settlement_value"))
    df_2_fit = df_2_fit.filter(F.col("last_run_fit"))
    # Merge df_2_ro with df_2 on specified columns to get 'last_run_ro' column
    df_2 = df_2.join(df_2_ro.select(['date', 'settlement_run', 'gsp_area', 'settlement_period', 'aggregation_type', 'description', 'actual_estimate', 'last_run_ro']),
                     on=['date', 'settlement_run', 'gsp_area', 'settlement_period',
                         'aggregation_type', 'description', 'actual_estimate'],
                     how='left')
    # Fill NaN values in 'last_run_ro' column with False
    df_2 = df_2.withColumn("last_run_ro", F.when(
        F.col("last_run_ro").isNull(), False).otherwise(F.col("last_run_ro")))
    # Merge df_2_fit with df_2 on specified columns to get 'last_run_fit' column
    df_2 = df_2.join(df_2_fit.select(['date', 'settlement_run', 'gsp_area', 'settlement_period', 'aggregation_type', 'description', 'actual_estimate', 'last_run_fit']),
                     on=['date', 'settlement_run', 'gsp_area', 'settlement_period',
                         'aggregation_type', 'description', 'actual_estimate'],
                     how='left')
    # Fill NaN values in 'last_run_fit' column with False
    df_2 = df_2.withColumn("last_run_fit", F.when(
        F.col("last_run_fit").isNull(), False).otherwise(F.col("last_run_fit")))
    # Create a new column 'last_run_ttl' which is True if either 'last_run_ro' or 'last_run_fit' is True
    df_2 = df_2.withColumn("last_run_ttl", F.col(
        "last_run_ro") | F.col("last_run_fit"))
    # Filter the rows where 'last_run_ttl' is True
    df_2 = df_2.filter(F.col("last_run_ttl"))
    # Drop 'last_run_ttl' column
    df_2 = df_2.drop('last_run_ttl')
    min_date = max(min_1, min_2)
    max_date = min(max_1, max_2)
    # Filter df and df_2 by date range
    df = df.filter((F.col("date") >= min_date) & (F.col("date") <= max_date))
    df_2 = df_2.filter((F.col("date") >= min_date) &
                       (F.col("date") <= max_date))
    # Drop unnecessary columns from df_2
    df_2 = df_2.drop('ro', 'fit', 'flag', 'settlement_value')
    # Concatenate df and df_2
    df_final = df.unionByName(df_2)
    # Financial Year
    df_final = df_final.withColumn("fy",
                                   when(month(df_final.date) >= 4, concat(
                                       year(df_final.date), lit("-"), (year(df_final.date)+1) % 100))
                                   .otherwise(concat(year(df_final.date)-1, lit("-"), year(df_final.date) % 100)))
    # Month Year
    df_final = df_final.withColumn(
        "month-year",
        to_date(concat_ws("-", lit(1), month(col("date")), year(col("date"))), 'd-M-y'))

    from pyspark.sql.functions import year as year_func, month as month_func, dayofmonth, date_add, date_sub, max, format_string, dayofyear
    df_final = df_final.withColumn("month", month_func(
        col("date"))).withColumn("year", year_func(col("date")))
    # ytd filter:
    latest_day = df_final.select("date").orderBy(
        "date", ascending=False).first()[0].timetuple().tm_yday
    df_final = df_final.withColumn("ytd",
                                   when(dayofyear(df_final.date)
                                        <= latest_day, 1)
                                   .otherwise(0))
    # Financial year date
    df_final = df_final.withColumn('fy_date', date_sub(df_final["date"], 90))
    # Financial Year to Date
    latest_day = df_final.select("fy_date").orderBy(
        "fy_date", ascending=False).first()[0].timetuple().tm_yday
    df_final = df_final.withColumn("fytd",
                                   when(dayofyear(df_final.fy_date)
                                        <= latest_day, 1)
                                   .otherwise(0))
    # Drop the column
    df_final = df_final.drop('fy_date')
    df_final = df_final.withColumn("date", F.to_utc_timestamp(F.col("date"), "UTC")).withColumn(
        "month-year", F.to_utc_timestamp(F.col("month-year"), "UTC"))

    save_path = "/tmp/delta/gross_ro_fit"
    df_final.write.format("delta").save(save_path)
    # DATABRICKS: Saved as Table: elexon_f.ro_fit
