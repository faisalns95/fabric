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
# META     },
# META     "warehouse": {
# META       "default_warehouse": "3474c0a6-e138-4cf4-b3f8-c7933f4767e6",
# META       "known_warehouses": [
# META         {
# META           "id": "3474c0a6-e138-4cf4-b3f8-c7933f4767e6",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# =====================================================================================
# SILVER LAYER — UK HM Land Registry Price Paid Data
# Workspace: Demo_Dev | Lakehouse: Demo_Dev | Notebook: demo_dev_nb_silver
# Bronze -> Silver: schema enforcement, validation/quarantine, standardization,
# dedup, CDC (record_status) handling, partitioned Delta MERGE
# Lakehouse Demo_Dev is already attached/pinned as default, so relative Files/ paths
# and saveAsTable() resolve against it automatically — no need to prefix with abfss://
# =====================================================================================

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType
)
from delta.tables import DeltaTable

# -------------------------------------------------------------------------------------
# 0. CONFIG
# -------------------------------------------------------------------------------------
BRONZE_PATH   = "Files/bronze/demo-land_registry_complete.csv"   # confirmed from lakehouse browser, 5.10 GB
SILVER_TABLE  = "silver_land_registry_complete"
QUARANTINE_TABLE = "silver_land_registry_complete_quarantine"
AUDIT_TABLE   = "silver_audit_log"

RUN_TS = F.current_timestamp()

# -------------------------------------------------------------------------------------
# 1. SCHEMA — official 16 HMLR columns, in source order (no header in file)
# -------------------------------------------------------------------------------------
bronze_schema = StructType([
    StructField("transaction_id",   StringType(), True),
    StructField("price",            StringType(), True),   # cast after validation
    StructField("date_of_transfer", StringType(), True),   # cast after validation
    StructField("postcode",         StringType(), True),
    StructField("property_type",    StringType(), True),
    StructField("old_new",          StringType(), True),
    StructField("duration",         StringType(), True),
    StructField("paon",             StringType(), True),
    StructField("saon",             StringType(), True),
    StructField("street",           StringType(), True),
    StructField("locality",         StringType(), True),
    StructField("town_city",        StringType(), True),
    StructField("district",         StringType(), True),
    StructField("county",           StringType(), True),
    StructField("ppd_category_type",StringType(), True),
    StructField("record_status",    StringType(), True),   # A / C / D
])

# -------------------------------------------------------------------------------------
# 2. READ BRONZE
# -------------------------------------------------------------------------------------
bronze_df = (
    spark.read
    .option("header", "false")
    .schema(bronze_schema)
    .csv(BRONZE_PATH)
)

print(f"Bronze row count: {bronze_df.count():,}")

# -------------------------------------------------------------------------------------
# 3. TYPE CASTING (raw strings -> typed columns, nulls on bad parse — caught in validation)
# -------------------------------------------------------------------------------------
typed_df = (
    bronze_df
    .withColumn("price_amt", F.col("price").cast(DoubleType()))
    .withColumn("transfer_date", F.to_date("date_of_transfer", "yyyy-MM-dd HH:mm"))
    .withColumn("postcode_clean", F.upper(F.trim(F.col("postcode"))))
    .withColumn("ingestion_ts", RUN_TS)
)

# -------------------------------------------------------------------------------------
# 4. VALIDATION RULES
#    Each rule adds a boolean flag column. Rows failing ANY rule -> quarantine.
# -------------------------------------------------------------------------------------
validated_df = (
    typed_df
    .withColumn("chk_transaction_id_present", F.col("transaction_id").isNotNull() & (F.length("transaction_id") > 0))
    .withColumn("chk_price_valid", F.col("price_amt").isNotNull() & (F.col("price_amt") > 0))
    .withColumn("chk_date_valid", F.col("transfer_date").isNotNull())
    .withColumn("chk_postcode_format",
                F.col("postcode_clean").rlike(r"^[A-Z]{1,2}[0-9R][0-9A-Z]? [0-9][A-Z]{2}$"))
    .withColumn("chk_property_type_valid",
                F.col("property_type").isin("D", "S", "T", "F", "O"))
    .withColumn("chk_old_new_valid", F.col("old_new").isin("Y", "N"))
    .withColumn("chk_duration_valid", F.col("duration").isin("F", "L"))
    .withColumn("chk_record_status_valid", F.col("record_status").isin("A", "C", "D"))
    .withColumn("chk_no_future_date", F.col("transfer_date") <= F.current_date())
)

rule_cols = [c for c in validated_df.columns if c.startswith("chk_")]

validated_df = validated_df.withColumn(
    "is_valid",
    F.array_min(F.array(*[F.col(c).cast("int") for c in rule_cols])) == 1
).withColumn(
    "failed_rules",
    F.concat_ws(",", *[F.when(~F.col(c), F.lit(c)) for c in rule_cols])
)

good_df = validated_df.filter("is_valid = true")
bad_df  = validated_df.filter("is_valid = false")

good_count = good_df.count()
bad_count  = bad_df.count()
print(f"Valid rows: {good_count:,} | Quarantined rows: {bad_count:,}")

# -------------------------------------------------------------------------------------
# 5. STANDARDIZATION (on the valid set only)
# -------------------------------------------------------------------------------------
standardized_df = (
    good_df
    .withColumn("property_type_desc", F.when(F.col("property_type") == "D", "Detached")
                                        .when(F.col("property_type") == "S", "Semi-Detached")
                                        .when(F.col("property_type") == "T", "Terraced")
                                        .when(F.col("property_type") == "F", "Flat/Maisonette")
                                        .otherwise("Other"))
    .withColumn("duration_desc", F.when(F.col("duration") == "F", "Freehold").otherwise("Leasehold"))
    .withColumn("is_new_build", F.col("old_new") == "Y")
    .withColumn("transfer_year", F.year("transfer_date"))
    .withColumn("street_clean", F.initcap(F.trim("street")))
    .withColumn("town_city_clean", F.initcap(F.trim("town_city")))
    .withColumn("county_clean", F.initcap(F.trim("county")))
)

# -------------------------------------------------------------------------------------
# 6. DEDUPLICATION (keep latest ingestion per transaction_id)
# -------------------------------------------------------------------------------------
from pyspark.sql.window import Window

dedup_window = Window.partitionBy("transaction_id").orderBy(F.col("ingestion_ts").desc())

deduped_df = (
    standardized_df
    .withColumn("row_num", F.row_number().over(dedup_window))
    .filter("row_num = 1")
    .drop("row_num")
)

# -------------------------------------------------------------------------------------
# 7. CDC / RECORD STATUS HANDLING (A = add, C = change, D = delete)
#    Splits into upserts and deletes for the MERGE step below.
# -------------------------------------------------------------------------------------
upserts_df = deduped_df.filter(F.col("record_status").isin("A", "C"))
deletes_df = deduped_df.filter(F.col("record_status") == "D").select("transaction_id")

final_cols = [
    "transaction_id", "price_amt", "transfer_date", "transfer_year",
    "postcode_clean", "property_type", "property_type_desc",
    "is_new_build", "duration", "duration_desc",
    "paon", "saon", "street_clean", "locality",
    "town_city_clean", "district", "county_clean",
    "ppd_category_type", "record_status", "ingestion_ts"
]
upserts_final = upserts_df.select(*final_cols)

# -------------------------------------------------------------------------------------
# 8. WRITE / MERGE TO SILVER DELTA TABLE (partitioned by transfer_year)
# -------------------------------------------------------------------------------------
if not spark.catalog.tableExists(SILVER_TABLE):
    (upserts_final.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("transfer_year")
        .saveAsTable(SILVER_TABLE))
    print(f"Created {SILVER_TABLE} with {upserts_final.count():,} rows")
else:
    silver_tbl = DeltaTable.forName(spark, SILVER_TABLE)

    # Upsert adds/changes
    (silver_tbl.alias("t")
        .merge(upserts_final.alias("s"), "t.transaction_id = s.transaction_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

    # Apply deletes
    if deletes_df.count() > 0:
        (silver_tbl.alias("t")
            .merge(deletes_df.alias("d"), "t.transaction_id = d.transaction_id")
            .whenMatchedDelete()
            .execute())

    print(f"Merged into {SILVER_TABLE}")

# -------------------------------------------------------------------------------------
# 9. WRITE QUARANTINE TABLE (for review/reprocessing)
# -------------------------------------------------------------------------------------
if bad_count > 0:
    (bad_df.select(*[c for c in bad_df.columns if not c.startswith("chk_")])
        .write.format("delta").mode("append")
        .saveAsTable(QUARANTINE_TABLE))

# -------------------------------------------------------------------------------------
# 10. AUDIT LOG
# -------------------------------------------------------------------------------------
audit_row = spark.createDataFrame([{
    "run_ts": None,  # filled via current_timestamp() below
    "bronze_count": bronze_df.count(),
    "valid_count": good_count,
    "quarantined_count": bad_count,
    "final_silver_upserts": upserts_final.count(),
    "deletes_applied": deletes_df.count(),
}]).withColumn("run_ts", RUN_TS)

(audit_row.write.format("delta").mode("append").saveAsTable(AUDIT_TABLE))

print("Silver layer run complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SHOW TABLES").show(truncate=False)
df1=spark.sql("Select * from Demo_Dev.Demo_Dev.dbo.silver_land_registry_complete limit 10")
display(df1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
