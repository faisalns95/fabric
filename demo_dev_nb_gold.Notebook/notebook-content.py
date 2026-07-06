# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "45e6a8c0-2d6c-4286-bfd0-214e4ee8bc2d",
# META       "default_lakehouse_name": "Demo_Dev",
# META       "default_lakehouse_workspace_id": "441f0c73-adde-4fbd-bf3e-e8222bed79ab",
# META       "known_lakehouses": [
# META         {
# META           "id": "45e6a8c0-2d6c-4286-bfd0-214e4ee8bc2d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
# =====================================================================================
# GOLD LAYER — UK HM Land Registry Price Paid Data
# Workspace: Demo_Dev | Lakehouse: Demo_Dev | Source: silver_land_registry_complete
# Silver -> Gold: dimensional model (star schema) + pre-aggregated mart
# Lakehouse Demo_Dev is already attached/pinned as default in this notebook
# =====================================================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# -------------------------------------------------------------------------------------
# 0. CONFIG
# -------------------------------------------------------------------------------------
SILVER_TABLE = "silver_land_registry_complete"

DIM_DATE_TABLE      = "gold_dim_date"
DIM_GEOGRAPHY_TABLE = "gold_dim_geography"
DIM_PROPERTY_TABLE  = "gold_dim_property_type"
FACT_TABLE          = "gold_fact_price_paid"
MART_TABLE          = "gold_mart_avg_price_by_district_year"

silver_df = spark.table(SILVER_TABLE)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


silver_df.filter((F.col("price_amt")<10000) & (F.col("duration")=="F")).count()
silver_df = silver_df.withColumn("low_price_check",F.when(F.col("price_amt") < 10000, "y").otherwise("n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -------------------------------------------------------------------------------------
# 1. DIM_DATE — one row per distinct transfer_date in the silver set
# -------------------------------------------------------------------------------------
dim_date_df = (
    silver_df.select("transfer_date").distinct()
    .withColumn("date_key", F.date_format("transfer_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("transfer_date"))
    .withColumn("quarter", F.quarter("transfer_date"))
    .withColumn("month", F.month("transfer_date"))
    .withColumn("month_name", F.date_format("transfer_date", "MMMM"))
    .withColumn("day", F.dayofmonth("transfer_date"))
    .withColumn("day_of_week", F.date_format("transfer_date", "EEEE"))
    .select("date_key", "transfer_date", "year", "quarter", "month",
            "month_name", "day", "day_of_week")
)

(dim_date_df.write.format("delta").mode("overwrite").saveAsTable(DIM_DATE_TABLE))
print(f"{DIM_DATE_TABLE}: {dim_date_df.count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -------------------------------------------------------------------------------------
# 2. DIM_GEOGRAPHY — distinct district/county/town combinations, surrogate key
# -------------------------------------------------------------------------------------
dim_geo_raw = (
    silver_df.select("district", "county_clean", "town_city_clean", "postcode_clean")
    .withColumnRenamed("county_clean", "county")
    .withColumnRenamed("town_city_clean", "town_city")
    .withColumnRenamed("postcode_clean", "postcode")
    .withColumn("postcode_area", F.regexp_extract("postcode", r"^([A-Z]{1,2})", 1))
    .distinct()
)

dim_geography_df = dim_geo_raw.withColumn(
    "geography_key",
    F.row_number().over(Window.orderBy("district", "county", "town_city", "postcode"))
)

(dim_geography_df.write.format("delta").mode("overwrite").saveAsTable(DIM_GEOGRAPHY_TABLE))
print(f"{DIM_GEOGRAPHY_TABLE}: {dim_geography_df.count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -------------------------------------------------------------------------------------
# 3. DIM_PROPERTY_TYPE — small static-ish dimension
# -------------------------------------------------------------------------------------
dim_property_df = spark.createDataFrame([
    ("D", "Detached"),
    ("S", "Semi-Detached"),
    ("T", "Terraced"),
    ("F", "Flat/Maisonette"),
    ("O", "Other"),
], ["property_type", "property_type_desc"]) \
    .withColumn("property_type_key", F.monotonically_increasing_id())

(dim_property_df.write.format("delta").mode("overwrite").saveAsTable(DIM_PROPERTY_TABLE))

# -------------------------------------------------------------------------------------
# 4. FACT_PRICE_PAID — grain: one row per transaction, joined to dims by natural keys
#    (kept as broadcast joins — dims are small relative to the fact)
# -------------------------------------------------------------------------------------
dim_geo_lookup = dim_geography_df.select(
    "geography_key", "district", "county", "town_city", "postcode"
)
dim_property_lookup = dim_property_df.select("property_type_key", "property_type")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



#FACT



fact_df = (
    silver_df
    .withColumn("date_key", F.date_format("transfer_date", "yyyyMMdd").cast("int"))
    .withColumnRenamed("county_clean", "county")
    .withColumnRenamed("town_city_clean", "town_city")
    .withColumnRenamed("postcode_clean", "postcode")
    .join(F.broadcast(dim_geo_lookup), ["district", "county", "town_city", "postcode"], "left")
    .join(F.broadcast(dim_property_lookup), "property_type", "left")
    .withColumn("price_band",
        F.when(F.col("price_amt") < 150000, "Under 150k")
         .when(F.col("price_amt") < 300000, "150k-300k")
         .when(F.col("price_amt") < 500000, "300k-500k")
         .when(F.col("price_amt") < 1000000, "500k-1M")
         .otherwise("1M+"))
    .select(
        "transaction_id", "date_key", "geography_key", "property_type_key",
        "price_amt", "price_band", "is_new_build", "duration", "duration_desc",
        "paon", "saon", "street_clean", "ppd_category_type", "transfer_year"
    )
)

if not spark.catalog.tableExists(FACT_TABLE):
    (fact_df.write.format("delta").mode("overwrite")
        .partitionBy("transfer_year")
        .saveAsTable(FACT_TABLE))
else:
    fact_tbl = DeltaTable.forName(spark, FACT_TABLE)
    (fact_tbl.alias("t")
        .merge(fact_df.alias("s"), "t.transaction_id = s.transaction_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

print(f"{FACT_TABLE}: {fact_df.count():,} rows")

# -------------------------------------------------------------------------------------
# 5. PRE-AGGREGATED MART — avg price by district/year (fast Power BI import/DirectLake)
# -------------------------------------------------------------------------------------
fact_current = spark.table(FACT_TABLE)
geo_current = spark.table(DIM_GEOGRAPHY_TABLE)

mart_df = (
    fact_current
    .join(geo_current, "geography_key", "left")
    .groupBy("district", "transfer_year")
    .agg(
        F.count("*").alias("transaction_count"),
        F.round(F.avg("price_amt"), 2).alias("avg_price"),
        F.round(F.expr("percentile_approx(price_amt, 0.5)"), 2).alias("median_price"),
        F.min("price_amt").alias("min_price"),
        F.max("price_amt").alias("max_price"),
    )
)

(mart_df.write.format("delta").mode("overwrite").saveAsTable(MART_TABLE))
print(f"{MART_TABLE}: {mart_df.count():,} rows")

# -------------------------------------------------------------------------------------
# 6. MAINTENANCE — OPTIMIZE + VACUUM on the fact table (run periodically, not every batch)
# -------------------------------------------------------------------------------------
# spark.sql(f"OPTIMIZE {FACT_TABLE} ZORDER BY (geography_key, date_key)")
# spark.sql(f"VACUUM {FACT_TABLE} RETAIN 168 HOURS")

print("Gold layer run complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
