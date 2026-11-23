"""Demonstrate Spark partitioning, parallel aggregations, and driver monitoring."""
import argparse
import os
import sys
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import psutil
import os
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

from pyspark.sql import SparkSession, functions as F
from pyspark.ml.evaluation import BinaryClassificationEvaluator

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from src.spark.ml_pipeline import build_model_pipeline, prepare_for_model


_LAST_PROCESS_SAMPLE = None

def process_snapshot(label):
    """Sample the Python driver and visible local JVM descendants."""
    global _LAST_PROCESS_SAMPLE
    root = psutil.Process(os.getpid())
    processes = [root]
    try:
        processes.extend(root.children(recursive=True))
    except psutil.Error:
        pass
    live = []
    for process in processes:
        try:
            process.cpu_times()
            live.append(process)
        except psutil.Error:
            continue
    python_rss = 0
    java_rss = 0
    cpu_seconds = 0.0
    for process in live:
        try:
            rss = process.memory_info().rss
            cpu = process.cpu_times()
            cpu_seconds += cpu.user + cpu.system
            if "java" in process.name().lower():
                java_rss += rss
            else:
                python_rss += rss
        except psutil.Error:
            continue
    now = time.monotonic()
    rate = None
    if _LAST_PROCESS_SAMPLE is not None:
        prior_time, prior_cpu = _LAST_PROCESS_SAMPLE
        elapsed = max(now - prior_time, 1e-9)
        rate = max(0.0, (cpu_seconds - prior_cpu) / elapsed * 100.0)
    _LAST_PROCESS_SAMPLE = (now, cpu_seconds)
    cpu_text = "warming_up" if rate is None else f"{rate:.1f}"
    print(f"{label}: python_driver_rss_mb={python_rss / 1024**2:.1f}, "
          f"local_jvm_rss_mb={java_rss / 1024**2:.1f}, "
          f"python_plus_jvm_cpu_pct_of_one_core={cpu_text}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="data/bank.csv")
    parser.add_argument("--partitions", type=int, default=4)
    args = parser.parse_args()

    scheduler_file = str(REPO_ROOT / "src/spark/fairscheduler.xml")
    spark = (SparkSession.builder.appName("BankingDataParallelism")
             .config("spark.scheduler.mode", "FAIR")
             .config("spark.scheduler.allocation.file", scheduler_file).getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    df = spark.read.option("header", True).option("inferSchema", True).csv(args.data)
    print("input_partitions", df.rdd.getNumPartitions())
    partitioned = df.repartition(args.partitions, "job").cache()
    print("repartitioned_partitions", partitioned.rdd.getNumPartitions())
    process_snapshot("before_aggregations")

    jobs = partitioned.groupBy("job").agg(F.avg("balance").alias("average_balance"))
    print("average_balance_by_job (Spark shuffles and combines partition aggregates)")
    jobs.orderBy(F.desc("average_balance")).show(20, truncate=False)

    # The dataset has no loan-amount field. Balance is explicitly used as a proxy
    # only for personal-loan holders, not reported as an actual loan amount.
    age_band = F.concat((F.floor(F.col("age") / 10) * 10).cast("int"), F.lit("-"),
                        (F.floor(F.col("age") / 10) * 10 + 9).cast("int"))
    loan_proxy = (partitioned.where(F.col("loan") == "yes")
                  .withColumn("age_band", age_band)
                  .groupBy("age_band")
                  .agg(F.count("*").alias("personal_loan_clients"),
                       F.sum("balance").alias("total_yearly_balance_proxy"),
                       F.avg("balance").alias("mean_yearly_balance_proxy"))
                  .orderBy(F.desc("total_yearly_balance_proxy")))
    print("top age bands by average-yearly-balance proxy among personal-loan clients")
    loan_proxy.show(5, truncate=False)

    # Train the same Logistic Regression pipeline over partitioned training data.
    prepared = prepare_for_model(partitioned).withColumn(
        "_row_id", F.monotonically_increasing_id()).cache()
    train = prepared.sampleBy("label", {0.0: 0.8, 1.0: 0.8}, seed=42).cache()
    train_ids = train.select("_row_id")
    test = prepared.join(train_ids, on="_row_id", how="left_anti")
    distributed_train = train.repartition(args.partitions, "job")
    print("training_partitions", distributed_train.rdd.getNumPartitions())
    distributed_model = build_model_pipeline().fit(distributed_train)
    distributed_predictions = distributed_model.transform(test)
    auc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    print("partitioned_logistic_test_AUROC", f"{auc.evaluate(distributed_predictions):.4f}")
    print("Spark distributes feature preparation and logistic-regression work over input partitions; "
          "the small Colab dataset is for demonstration, not a speed benchmark.")

    # Two independent aggregation jobs are submitted through Spark's fair scheduler.
    sc = spark.sparkContext
    def run_in_pool(group, action):
        sc.setLocalProperty("spark.scheduler.pool", group)
        sc.setJobGroup(group, f"Independent preprocessing: {group}")
        try:
            return action().collect()
        finally:
            sc.setLocalProperty("spark.jobGroup.id", None)
            sc.setLocalProperty("spark.job.description", None)
            sc.setLocalProperty("spark.scheduler.pool", None)

    with ThreadPoolExecutor(max_workers=2) as pool:
        job_future = pool.submit(run_in_pool, "job_summary", lambda: jobs)
        education_future = pool.submit(
            run_in_pool, "education_summary",
            lambda: partitioned.groupBy("education").count())
        print("concurrent_job_summary_rows", len(job_future.result()))
        print("concurrent_education_summary_rows", len(education_future.result()))
    process_snapshot("after_aggregations")
    print("Resource readings are driver-process snapshots in local Colab, not cluster-wide metrics.")
    partitioned.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
