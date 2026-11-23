"""Reusable Spark ML pipeline for term-deposit subscription prediction."""
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoder, RobustScaler, StringIndexer, VectorAssembler
from pyspark.sql import functions as F

CATEGORICAL_FEATURES = [
    "job", "marital", "education", "default", "housing", "loan",
    "contact", "month", "poutcome",
]
NUMERIC_FEATURES = [
    "age", "balance", "day", "campaign", "previous",
    "pdays_nonnegative", "was_previously_contacted",
]


def prepare_for_model(df):
    """Create labels and stable feature inputs without using post-call duration."""
    df = df.fillna({c: "unknown" for c in CATEGORICAL_FEATURES})
    df = df.fillna({c: 0 for c in ["age", "balance", "day", "campaign", "previous", "pdays"]})
    if "y" in df.columns:
        df = df.withColumn("label", F.when(F.col("y") == "yes", 1.0).otherwise(0.0))
    df = (df.withColumn("was_previously_contacted",
                      F.when(F.col("pdays") >= 0, 1.0).otherwise(0.0))
          .withColumn("pdays_nonnegative",
                      F.when(F.col("pdays") >= 0, F.col("pdays").cast("double"))
                       .otherwise(0.0)))
    return df


def build_model_pipeline():
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in CATEGORICAL_FEATURES
    ]
    indexed = [f"{c}_idx" for c in CATEGORICAL_FEATURES]
    encoded = [f"{c}_ohe" for c in CATEGORICAL_FEATURES]
    encoder = OneHotEncoder(inputCols=indexed, outputCols=encoded,
                            handleInvalid="keep", dropLast=True)
    numeric_assembler = VectorAssembler(inputCols=NUMERIC_FEATURES,
                                        outputCol="numeric_raw",
                                        handleInvalid="keep")
    # RobustScaler learns median/IQR within each training fold; extremes are
    # down-weighted without dropping legitimate high or negative bank balances.
    robust_scaler = RobustScaler(inputCol="numeric_raw", outputCol="numeric_scaled",
                                 withCentering=False, withScaling=True,
                                 lower=0.25, upper=0.75)
    assembler = VectorAssembler(inputCols=encoded + ["numeric_scaled"],
                                outputCol="features", handleInvalid="keep")
    classifier = LogisticRegression(labelCol="label", featuresCol="features",
                                    maxIter=80, standardization=False)
    return Pipeline(stages=indexers + [encoder, numeric_assembler, robust_scaler,
                                       assembler, classifier])

