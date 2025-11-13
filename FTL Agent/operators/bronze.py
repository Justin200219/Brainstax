from pyspark.sql import functions as F, Window

def ingest(spark, source, target, pk, incremental_cols=None):
    df = spark.table(source)
    df = (df
          .withColumn("_ingestion_timestamp", F.current_timestamp())
          .withColumn("_source_table", F.lit(source)))

    # Idempotent: if incremental cols exist, keep latest per PK
    if incremental_cols:
        ts = incremental_cols[0]
        w = Window.partitionBy(*pk).orderBy(F.col(ts).desc_nulls_last())
        df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn=1").drop("_rn")

    # Always dedupe PK to be safe
    df = df.dropDuplicates(pk)

    # Upsert into Bronze (idempotent MERGE)
    df.createOrReplaceTempView("_bronze_src")
    keys = " AND ".join([f"t.{c}=s.{c}" for c in pk])
    sets = ", ".join([f"{c}=s.{c}" for c in df.columns])
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target} USING DELTA AS
        SELECT * FROM _bronze_src WHERE 1=0
    """)
    spark.sql(f"""
        MERGE INTO {target} t
        USING _bronze_src s
        ON {keys}
        WHEN MATCHED THEN UPDATE SET {sets}
        WHEN NOT MATCHED THEN INSERT *
    """)
