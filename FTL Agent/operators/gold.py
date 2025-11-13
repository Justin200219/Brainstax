from pyspark.sql import functions as F

def scd2_dimension(spark, source, target, pk, tracked_cols="auto"):
    src = spark.table(source)
    pkc = pk[0] if isinstance(pk, list) else pk
    cols = [c for c in src.columns if c not in ("valid_from","valid_to","is_current","_row_hash")]
    if tracked_cols == "auto":
        tracked_cols = [c for c in cols if c != pkc]
    row_hash = F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in tracked_cols]), 256)
    src = (src
           .withColumn("_row_hash", row_hash)
           .withColumn("valid_from", F.current_timestamp())
           .withColumn("valid_to", F.lit(None).cast("timestamp"))
           .withColumn("is_current", F.lit(True)))
    src.createOrReplaceTempView("_scd_src")

    spark.sql(f"CREATE TABLE IF NOT EXISTS {target} USING DELTA AS SELECT * FROM _scd_src WHERE 1=0")

    # Close changed rows
    spark.sql(f"""
      MERGE INTO {target} t
      USING _scd_src s
      ON t.{pkc} = s.{pkc} AND t.is_current = true
      WHEN MATCHED AND t._row_hash <> s._row_hash THEN
        UPDATE SET t.valid_to = current_timestamp(), t.is_current = false
    """)
    # Insert new/current
    spark.sql(f"""
      MERGE INTO {target} t
      USING _scd_src s
      ON t.{pkc} = s.{pkc} AND t.is_current = true
      WHEN NOT MATCHED THEN INSERT *
    """)

def fact_passthrough(spark, source, target):
    df = spark.table(source)
    df.write.format("delta").mode("overwrite").saveAsTable(target)
