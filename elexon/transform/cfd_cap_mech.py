from ..utils_others.utils import get_holidays_calendar
from pyspark.sql import functions as F
from pyspark.sql.functions import col, year, month, dayofweek, when, lit, to_date, concat, concat_ws


def transform_cfd_cap_mech():

    # From transform sub-package
    df = spark.read.table('elexon_f.gross_demand_hh')
    df = df.withColumn('date', to_date(df['date']))

    df = df.withColumn("fy",
                       when(month(df.date) >= 4, concat(
                           year(df.date), lit("-"), (year(df.date)+1) % 100))
                       .otherwise(concat(year(df.date)-1, lit("-"), year(df.date) % 100)))
    calendar = spark.createDataFrame(get_holidays_calendar())
    df = df.join(calendar.select('WD_Date', 'WD'),
                 df.date == calendar.WD_Date, how='left')
    df = df.fillna({'WD': 'W'})
    df = df.withColumnRenamed('WD', 'WD_BH')
    df = df.drop('WD_Date')
    df = df.withColumn("WD_BH",
                       when(df.WD_BH == "W", 0)
                       .otherwise(1))
    df = df.withColumn('WD_AUX', when(
        dayofweek(df.date).between(2, 6), 0).otherwise(1))
    df = df.withColumn("WD", df["WD_BH"] + df["WD_AUX"])
    df = df.drop("WD_BH", "WD_AUX")
    df = df.withColumn("WD",
                       when(df.WD == 0, "wd")
                       .otherwise("nwd"))
    df = df.withColumnRenamed('WD', 'wd')

    from pyspark.sql.functions import month as month_func, year as year_func
    # CapMec Flag
    df = df.withColumn(
        "flag_cm",
        F.when((F.month(F.col("date")).isin([11, 12, 1, 2])) & (F.col("wd") == 'wd'), 1).otherwise(0))
    # Month Year
    df = df.withColumn(
        "month-year",
        to_date(concat_ws("-", lit(1), month(col("date")), year(col("date"))), 'd-M-y'))

    from pyspark.sql.functions import year as year_func, month as month_func, dayofmonth, date_add, date_sub, max, format_string, dayofyear
    df = df.withColumn("month", month_func(col("date"))).withColumn(
        "year", year_func(col("date")))
    # ytd filter:
    latest_day = df.select("date").orderBy(
        "date", ascending=False).first()[0].timetuple().tm_yday
    df = df.withColumn("ytd",
                       when(dayofyear(df.date) <= latest_day, 1)
                       .otherwise(0))
    # Financial year date
    df = df.withColumn('fy_date', date_sub(df["date"], 90))
    # Financial Year to Date
    latest_day = df.select("fy_date").orderBy(
        "fy_date", ascending=False).first()[0].timetuple().tm_yday
    df = df.withColumn("fytd",
                       when(dayofyear(df.fy_date) <= latest_day, 1)
                       .otherwise(0))
    # Drop the column
    df = df.drop('fy_date')
    df = df.withColumn("date", F.to_utc_timestamp(F.col("date"), "UTC")).withColumn(
        "month-year", F.to_utc_timestamp(F.col("month-year"), "UTC"))

    save_path = "/tmp/delta/gross_capmec"
    df.write.format("delta").save(save_path)
    # DATABRICKS: Saved as Table: elexon_f.cfd_capmech
