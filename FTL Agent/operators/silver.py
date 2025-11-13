from pyspark.sql import functions as F
import re

def cleanse(spark, source, target, dq, pii_cols_guess, pk):
    df = spark.table(source)

    # Standardize strings; cast timestamps if they look like *_at
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, F.trim(F.col(c)))
        if re.search(r"_at$", c) and c in df.columns:
            df = df.withColumn(c, F.to_timestamp(F.col(c)))

    # Email normalize + hash any guessed PII (keep *_hash, drop raw where makes sense)
    for c in pii_cols_guess or []:
        if c in df.columns:
            if re.search("email", c, re.I):
                df = df.withColumn(c, F.lower(F.col(c)))
            df = df.withColumn(f"{c}_hash", F.sha2(F.col(c).cast("string"), 256))

    # Apply basic DQ
    for c in (dq or {}).get("not_null", []):
        if c in df.columns:
            df = df.filter(F.col(c).isNotNull())
    if "unique" in (dq or {}):
        df = df.dropDuplicates(dq["unique"])

    # Remove raw PII (optional, keep only hashes)
    for c in pii_cols_guess or []:
        if c in df.columns and f"{c}_hash" in df.columns:
            df = df.drop(c)

    # Idempotent MERGE into Silver
    df.createOrReplaceTempView("_silver_src")
    keys = " AND ".join([f"t.{c}=s.{c}" for c in pk])
    sets = ", ".join([f"{c}=s.{c}" for c in df.columns])
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target} USING DELTA AS
        SELECT * FROM _silver_src WHERE 1=0
    """)
    spark.sql(f"""
        MERGE INTO {target} t
        USING _silver_src s
        ON {keys}
        WHEN MATCHED THEN UPDATE SET {sets}
        WHEN NOT MATCHED THEN INSERT *
    """)
