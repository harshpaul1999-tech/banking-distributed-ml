"""Run job aggregates, event-time windows, and micro-batch model predictions."""
import argparse
import sys
from pathlib import Path

import os
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession, functions as F, types as T

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
from src.spark.ml_pipeline import prepare_for_model


def stream_schema():
    return T.StructType([
        T.StructField("age", T.IntegerType()),
        T.StructField("job", T.StringType()),
        T.StructField("marital", T.StringType()),
        T.StructField("education", T.StringType()),
        T.StructField("default", T.StringType()),
        T.StructField("balance", T.DoubleType()),
        T.StructField("housing", T.StringType()),
        T.StructField("loan", T.StringType()),
        T.StructField("contact", T.StringType()),
        T.StructField("day", T.IntegerType()),
        T.StructField("month", T.StringType()),
        T.StructField("duration", T.IntegerType()),
        T.StructField("campaign", T.IntegerType()),
        T.StructField("pdays", T.IntegerType()),
        T.StructField("previous", T.IntegerType()),
        T.StructField("poutcome", T.StringType()),
        T.StructField("y", T.StringType()),
        T.StructField("event_id", T.StringType()),
        T.StructField("event_time", T.StringType()),
    ])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--inbox", default="/tmp/bank_stream_inbox")
    parser.add_argument("--model", default="outputs/model/bank_subscription_logistic")
    parser.add_argument("--checkpoint-dir", default="outputs/streaming/checkpoints")
    parser.add_argument("--watermark", default="1 minute")
    parser.add_argument("--available-now", action="store_true",
                        help="process all files currently present, then stop")
    args = parser.parse_args()
    Path(args.checkpoint_dir).mkdir(parents=True, exist_ok=True)

    spark = (SparkSession.builder.appName("BankingStructuredStreaming")
             .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    raw = (spark.readStream.schema(stream_schema()).option("header", True)
           .option("maxFilesPerTrigger", 1).csv(args.inbox))
    events = raw.withColumn("event_time", F.to_timestamp("event_time"))
    watermark_events = events.withWatermark("event_time", args.watermark)

    job_means = events.groupBy("job").agg(
        F.count("*").alias("transactions"),
        F.round(F.avg("balance"), 2).alias("average_balance"),
        F.round(F.avg("duration"), 2).alias("average_duration_seconds"),
    )
    def start_query(writer):
        if args.available_now:
            return writer.trigger(availableNow=True).start()
        return writer.trigger(processingTime="2 seconds").start()

    jobs_query = start_query(job_means.writeStream.outputMode("complete").format("console")
                             .option("truncate", False)
                             .option("checkpointLocation", f"{args.checkpoint_dir}/jobs"))

    window_10s = (watermark_events.groupBy(F.window("event_time", "10 seconds"))
                  .agg(F.count("*").alias("transactions"),
                       F.round(F.avg("balance"), 2).alias("average_balance")))
    def print_window_batch(batch, batch_id, label):
        print(f"{label} microbatch={batch_id}", flush=True)
        batch.show(20, truncate=False)

    window_10_query = start_query(window_10s.writeStream.outputMode("update")
                                  .foreachBatch(lambda batch, batch_id:
                                                print_window_batch(batch, batch_id, "10-second windows"))
                                  .option("checkpointLocation", f"{args.checkpoint_dir}/window10s"))

    window_1m = (watermark_events.groupBy(F.window("event_time", "1 minute"))
                 .agg(F.count("*").alias("transactions"),
                      F.round(F.avg("balance"), 2).alias("average_balance")))
    window_1m_query = start_query(window_1m.writeStream.outputMode("update")
                                  .foreachBatch(lambda batch, batch_id:
                                                print_window_batch(batch, batch_id, "1-minute windows"))
                                  .option("checkpointLocation", f"{args.checkpoint_dir}/window1m"))

    model = PipelineModel.load(args.model)
    def predict_microbatch(batch, batch_id):
        scored = model.transform(prepare_for_model(batch))
        print(f"microbatch={batch_id}: predictions", flush=True)
        display_cols = ["event_id", "event_time", "job", "balance", "duration"]
        if "y" in scored.columns:
            display_cols.append("y")
        scored.select(*display_cols, "probability", "prediction").show(20, truncate=False)
    prediction_query = start_query(events.writeStream.foreachBatch(predict_microbatch)
                                   .option("checkpointLocation", f"{args.checkpoint_dir}/predictions"))
    mode = "available-now replay" if args.available_now else "paced live replay"
    print(f"Streaming from {args.inbox}; watermark={args.watermark}; mode={mode}.")
    try:
        for query in [jobs_query, window_10_query, window_1m_query, prediction_query]:
            query.awaitTermination()
    finally:
        for query in [jobs_query, window_10_query, window_1m_query, prediction_query]:
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
