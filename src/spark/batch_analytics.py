"""Run the Spark EDA questions against the supplied bank.csv."""
import argparse
from pathlib import Path

import os
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

from pyspark.sql import SparkSession, functions as F, types as T

MONTH_QUARTER = {
    "jan": 1, "feb": 1, "mar": 1,
    "apr": 2, "may": 2, "jun": 2,
    "jul": 3, "aug": 3, "sep": 3,
    "oct": 4, "nov": 4, "dec": 4,
}


def age_group(age):
    if age is None:
        return "unknown"
    if age < 30:
        return "<30"
    if age <= 60:
        return "30-60"
    return ">60"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="data/bank.csv")
    parser.add_argument("--plot", default="outputs/job_counts.png")
    parser.add_argument("--default-plot", default="outputs/default_counts.png")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("BankingSparkEDA").getOrCreate()
    df = (spark.read.option("header", True).option("inferSchema", True)
          .csv(args.data).cache())
    print(f"rows={df.count()} columns={len(df.columns)}")
    df.show(5, truncate=False)
    df.printSchema()
    df.describe("age", "balance", "day", "duration", "campaign", "pdays", "previous").show()
    print("nulls by column")
    df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()
    print("duplicate_rows", df.count() - df.dropDuplicates().count())

    quarter_expr = F.when(F.col("month").isin("jan", "feb", "mar"), 1) \
        .when(F.col("month").isin("apr", "may", "jun"), 2) \
        .when(F.col("month").isin("jul", "aug", "sep"), 3) \
        .when(F.col("month").isin("oct", "nov", "dec"), 4)
    engineered = (df.withColumn("quarter", quarter_expr)
                  .withColumn("age_group", F.udf(age_group, T.StringType())("age"))
                  .withColumn("job_marital", F.concat_ws("_", "job", "marital"))
                  .withColumn("contact_upper", F.upper("contact")))

    print("balance > 1000")
    df.where(F.col("balance") > 1000).select("age", "job", "balance").show(10)
    print("average balance and median age by job")
    df.groupBy("job").agg(F.avg("balance").alias("avg_balance"),
                           F.expr("percentile_approx(age, 0.5)").alias("median_age"),
                           F.count("*").alias("clients")) \
      .orderBy(F.desc("avg_balance")).show(20, truncate=False)
    print("subscribed clients by marital status")
    df.where(F.col("y") == "yes").groupBy("marital").count().show()
    print("age groups")
    engineered.groupBy("age_group").count().orderBy("age_group").show()
    print("subscription rate by education")
    df.groupBy("education").agg(
        F.count("*").alias("clients"),
        (F.avg(F.when(F.col("y") == "yes", 1.0).otherwise(0.0)) * 100)
        .alias("subscription_pct"),
    ).orderBy(F.desc("subscription_pct")).show()
    print("top 3 professions by credit-default rate")
    df.groupBy("job").agg(
        F.count("*").alias("clients"),
        (F.avg(F.when(F.col("default") == "yes", 1.0).otherwise(0.0)) * 100)
        .alias("default_pct"),
    ).orderBy(F.desc("default_pct")).limit(3).show(truncate=False)
    engineered.select("job_marital", "contact_upper", "quarter").show(5, truncate=False)

    print("monthly contact counts and subscription rates")
    df.groupBy("month").agg(
        F.count("*").alias("contacts"),
        (F.avg(F.when(F.col("y") == "yes", 1.0).otherwise(0.0)) * 100)
        .alias("subscription_pct"),
    ).orderBy(F.desc("contacts")).show(12)
    print("mean and median duration by subscription outcome")
    df.groupBy("y").agg(F.avg("duration").alias("mean_seconds"),
                         F.expr("percentile_approx(duration, 0.5)").alias("median_seconds")).show()
    print("age_balance_correlation", df.stat.corr("age", "balance"))
    print("credit-default composition")
    df.groupBy("default").count().show()
    print("subscription rate by contact method")
    df.groupBy("contact").agg(
        F.count("*").alias("clients"),
        (F.avg(F.when(F.col("y") == "yes", 1.0).otherwise(0.0)) * 100)
        .alias("subscription_pct"),
    ).orderBy(F.desc("subscription_pct")).show()

    engineered.createOrReplaceTempView("bank_clients")
    print("Spark SQL average balance by age group")
    spark.sql("""SELECT age_group, COUNT(*) AS clients,
                       ROUND(AVG(balance), 2) AS average_balance
                FROM bank_clients GROUP BY age_group ORDER BY age_group""").show()
    print("Spark SQL most common jobs")
    spark.sql("SELECT job, COUNT(*) AS clients FROM bank_clients GROUP BY job ORDER BY clients DESC LIMIT 5").show()

    # Collect only the small aggregate to Pandas for plotting.
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import pandas as pd
    counts = df.groupBy("job").count().orderBy(F.desc("count")).toPandas()
    Path(args.plot).parent.mkdir(parents=True, exist_ok=True)
    ax = counts.set_index("job")["count"].plot(kind="bar", figsize=(10, 5), color="#2563eb")
    ax.set(title="Clients by job category", xlabel="Job", ylabel="Clients")
    plt.tight_layout()
    plt.savefig(args.plot, dpi=160)
    plt.close()
    default_counts = df.groupBy("default").count().orderBy("default").toPandas()
    ax = default_counts.set_index("default")["count"].plot(
        kind="bar", figsize=(6, 4), color=["#2563eb", "#dc2626"])
    ax.set(title="Clients by credit-default status", xlabel="Credit default", ylabel="Clients")
    plt.tight_layout()
    Path(args.default_plot).parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(args.default_plot, dpi=160)
    plt.close()
    print("saved_plot", args.plot)
    print("saved_plot", args.default_plot)
    df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
