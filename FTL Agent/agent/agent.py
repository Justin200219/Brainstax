# COMMAND ----------
# MAGIC %pip install pyyaml

# COMMAND ----------
import yaml, uuid
from pyspark.sql.utils import AnalysisException
from operators.bronze import ingest as bronze_ingest
from operators.silver import cleanse as silver_cleanse
from operators.gold import scd2_dimension, fact_passthrough
from agent.decisions_log import log_decision

RUN_ID = str(uuid.uuid4())

# Load config
with open("/Workspace/Repos/<your-repo-path>/medallion-agent/configs/table_config.yaml", "r") as f:
    config = yaml.safe_load(f)["tables"]

def detect_incremental_cols(df, hints):
    cols = [c for c,_ in df.dtypes]
    hints = [h for h in (hints or []) if h in cols]
    return hints or None

for tcfg in config:
    name   = tcfg["name"]
    source = tcfg["source"]
    pk     = tcfg["pk"]
    scd    = str(tcfg.get("scd_type","none")).lower()
    dq     = tcfg.get("dq", {})
    pii    = tcfg.get("pii_guess", [])
    print(f"=== Processing {name} ===")

    # 1) inspect source
    df = spark.table(source).limit(10000)  # sample
    incr_cols = detect_incremental_cols(df, tcfg.get("incremental_hints"))

    # 2) Bronze
    bronze_target = f"dev.bronze.{name}_raw"
    bronze_ingest(spark, source, bronze_target, pk, incr_cols)

    # 3) Silver
    silver_target = f"dev.silver.{name}_clean"
    silver_cleanse(spark, bronze_target, silver_target, dq, pii, pk)

    # 4) Gold
    if scd in ("1","2","3"):
        gold_target = f"dev.gold.{name}_dim"
        if scd == "2":
            scd2_dimension(spark, silver_target, gold_target, pk)
        else:
            # type 1 overwrite
            fact_passthrough(spark, silver_target, gold_target)
    elif scd == "fct":
        gold_target = f"dev.gold.{name}_fct"
        fact_passthrough(spark, silver_target, gold_target)
    else:
        gold_target = None

    # 5) Log decision
    decision = {
      "name": name,
      "source": source,
      "pk": pk,
      "incremental_cols": incr_cols,
      "silver_target": silver_target,
      "gold_target": gold_target,
      "dq": dq,
      "pii": pii,
      "scd_type": scd
    }
    log_decision(spark, RUN_ID, decision)

print(f"Run complete: {RUN_ID}")
display(spark.table("dev.meta.agent_decisions").filter(f"run_id = '{RUN_ID}'"))
