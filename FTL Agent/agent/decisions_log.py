def log_decision(spark, run_id, item):
    spark.sql("""
      CREATE TABLE IF NOT EXISTS dev.meta.agent_decisions
      (run_id string, table_name string, decision string)
      USING DELTA
    """)
    spark.createDataFrame([(run_id, item["name"], str(item))], "run_id string, table_name string, decision string")\
         .write.mode("append").saveAsTable("dev.meta.agent_decisions")
