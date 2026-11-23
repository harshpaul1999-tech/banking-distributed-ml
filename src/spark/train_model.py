"""Fit and evaluate a cross-validated Spark ML subscription classifier."""
import argparse
import sys
from pathlib import Path

import os
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession, functions as F

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from src.spark.ml_pipeline import build_model_pipeline, prepare_for_model


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="data/bank.csv")
    parser.add_argument("--model", default="outputs/model/bank_subscription_logistic")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("BankingSubscriptionModel").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    raw = spark.read.option("header", True).option("inferSchema", True).csv(args.data)
    model_data = prepare_for_model(raw)
    print("target counts")
    model_data.groupBy("y").count().show()
    print("balance IQR diagnostics (RobustScaler is fit inside each CV fold)")
    q1, q3 = raw.approxQuantile("balance", [0.25, 0.75], 0.01)
    iqr = q3 - q1
    lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    print(f"balance_q1={q1:.2f} balance_q3={q3:.2f} "
          f"IQR_bounds=({lower:.2f},{upper:.2f}) "
          f"outside_iqr={raw.where((F.col('balance') < lower) | (F.col('balance') > upper)).count()}")
    print("pdays=-1 is a structural sentinel and is represented with a separate flag.")
    print("duration is excluded from features because it is observed during/after a call.")

    # sampleBy gives each outcome its own train fraction; a generated ID lets us
    # build the complementary holdout without using outcome labels as features.
    indexed = model_data.withColumn("_row_id", F.monotonically_increasing_id()).cache()
    train = indexed.sampleBy("label", {0.0: 0.8, 1.0: 0.8}, seed=42).cache()
    train_ids = train.select("_row_id")
    test = indexed.join(train_ids, on="_row_id", how="left_anti").cache()
    pipeline = build_model_pipeline()
    lr = pipeline.getStages()[-1]
    grid = (ParamGridBuilder()
            .addGrid(lr.regParam, [0.01, 0.1])
            .addGrid(lr.elasticNetParam, [0.0, 0.5])
            .build())
    evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction",
                                               metricName="areaUnderPR")
    cv = CrossValidator(estimator=pipeline, estimatorParamMaps=grid,
                        evaluator=evaluator, numFolds=3, parallelism=2,
                        seed=42, collectSubModels=False)
    fitted = cv.fit(train)
    predictions = fitted.transform(test).cache()

    print(f"test_rows={test.count()} best_cv_areaUnderPR={max(fitted.avgMetrics):.4f}")
    for metric in ["areaUnderPR", "areaUnderROC"]:
        print(f"test_{metric}={BinaryClassificationEvaluator(labelCol='label', metricName=metric).evaluate(predictions):.4f}")
    for metric in ["accuracy", "f1", "weightedPrecision", "weightedRecall"]:
        score = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName=metric
        ).evaluate(predictions)
        print(f"test_{metric}={score:.4f}")
    print("default_threshold_0.50 confusion_matrix (rows=actual, columns=predicted)")
    predictions.groupBy("label", "prediction").count().orderBy("label", "prediction").show()
    print("baseline_accuracy_all_no", f"{1 - test.agg(F.avg('label')).first()[0]:.4f}")

    # Show a predeclared 0.20 operating threshold as a sensitivity example; it
    # is not chosen by optimizing on the held-out test set.
    print("threshold_sensitivity (illustrative; threshold is not tuned on test data)")
    probability_yes = vector_to_array(F.col("probability"))[1]
    for threshold in [0.10, 0.20, 0.50]:
        scored_at_threshold = predictions.withColumn(
            "threshold_prediction",
            F.when(probability_yes >= threshold, 1.0).otherwise(0.0),
        )
        counts = scored_at_threshold.agg(
            F.sum(F.when((F.col("label") == 1.0) & (F.col("threshold_prediction") == 1.0), 1).otherwise(0)).alias("tp"),
            F.sum(F.when((F.col("label") == 0.0) & (F.col("threshold_prediction") == 1.0), 1).otherwise(0)).alias("fp"),
            F.sum(F.when((F.col("label") == 1.0) & (F.col("threshold_prediction") == 0.0), 1).otherwise(0)).alias("fn"),
        ).first()
        tp, fp, fn = int(counts["tp"] or 0), int(counts["fp"] or 0), int(counts["fn"] or 0)
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        print(f"threshold={threshold:.2f} precision_yes={precision:.4f} "
              f"recall_yes={recall:.4f} f1_yes={f1:.4f} predicted_yes={tp + fp}")
    print("threshold_0.20 confusion_matrix (rows=actual, columns=predicted)")
    predictions.withColumn("prediction_0_20", F.when(probability_yes >= 0.20, 1.0).otherwise(0.0)) \
        .groupBy("label", "prediction_0_20").count().orderBy("label", "prediction_0_20").show()

    best_lr = fitted.bestModel.stages[-1]
    transformed_one = fitted.transform(test.limit(1))
    attrs = transformed_one.schema["features"].metadata.get("ml_attr", {}).get("attrs", {})
    names = {}
    for values in attrs.values():
        for item in values:
            names[int(item["idx"])] = item.get("name", f"feature_{item['idx']}")
    coeffs = list(enumerate(best_lr.coefficients.toArray().tolist()))
    top = sorted(coeffs, key=lambda pair: abs(pair[1]), reverse=True)[:15]
    print("largest_absolute_logistic_coefficients (positive raises modeled log-odds)")
    for index, coefficient in top:
        print(f"{names.get(index, f'feature_{index}')}\t{coefficient:.5f}")
    print("Coefficient magnitude is associational, not causal; correlated inputs can share signal.")

    Path(args.model).parent.mkdir(parents=True, exist_ok=True)
    fitted.bestModel.write().overwrite().save(args.model)
    print("saved_model", args.model)
    predictions.unpersist()
    test.unpersist()
    train.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
