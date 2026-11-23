# Banking Analytics with Hadoop, Hive and Spark

A Colab-ready project using the supplied bank.csv to demonstrate five distributed-data tasks: Hadoop MapReduce and Hive SQL, Spark EDA, Spark ML classification, Structured Streaming, and data parallelism.

[Open the notebook in Colab](https://colab.research.google.com/github/harshpaul1999-tech/banking-distributed-ml/blob/main/notebooks/Banking_Distributed_Analytics_Colab.ipynb) · [View the notebook on GitHub](https://github.com/harshpaul1999-tech/banking-distributed-ml/blob/main/notebooks/Banking_Distributed_Analytics_Colab.ipynb) · [Browse the repository](https://github.com/harshpaul1999-tech/banking-distributed-ml)

## Project contents

- notebooks/Banking_Distributed_Analytics_Colab.ipynb — the main guided notebook covering all five project parts.
- notebooks/Financial_Forecasting_Frontier_Distributed_ML.ipynb — a compact Colab companion aligned with the linked reference notebook, using this repository’s dataset path.
- data/bank.csv — the supplied 4,521-row dataset.
- data/stream_sample/ — four small replay chunks with synthetic event IDs and event times.
- src/hadoop/ — Hadoop Streaming mappers, reducers, and HDFS run commands.
- sql/banking_queries.hql — Hive database/table definition and analysis queries.
- src/spark/ — batch EDA, ML pipeline, and parallel processing scripts.
- src/streaming/ — chunk generator, paced file replay, and Structured Streaming job.
- docs/results_and_interpretation.md — data-grounded findings and limitations.
- docs/video_presentation_script.md — the original project presentation script.
- docs/reference_colab_video_script.md — two recording scripts covering the reference PDF and PySpark notebook.
- docs/reflection_summary.md — a reflection draft to personalize after running the notebook.

## Run the Colab notebooks

- [Main five-part project notebook](https://colab.research.google.com/github/harshpaul1999-tech/banking-distributed-ml/blob/main/notebooks/Banking_Distributed_Analytics_Colab.ipynb)
- [Reference-aligned Financial Forecasting Frontier companion](https://colab.research.google.com/github/harshpaul1999-tech/banking-distributed-ml/blob/main/notebooks/Financial_Forecasting_Frontier_Distributed_ML.ipynb)

1. Open either notebook in Colab and choose Runtime → Run all. The companion opens the repository’s `bank.csv` rather than depending on the original notebook’s Google Drive folder path.
2. The main notebook installs the pinned PySpark dependency and runs local Hadoop mapper/reducer replays, Spark batch analysis, Spark ML, a bounded streaming replay, and partitioned Spark examples.
3. Hadoop HDFS and Hive commands are included for a configured cluster. Colab does not start those services automatically; the local mapper/reducer replay checks the aggregation logic without claiming a distributed Hadoop run. The companion also has an optional Gradio cell.

## Run from a local environment

Use Python 3.9+ and Java 17. From the repository root:

~~~sh
python -m pip install -r requirements.txt
python src/spark/batch_analytics.py --data data/bank.csv
python src/spark/train_model.py --data data/bank.csv
python src/spark/data_parallelism.py --data data/bank.csv --partitions 4
~~~

For a streaming demo, first create a model and chunks, then run these in separate terminals:

~~~sh
python src/streaming/create_stream_chunks.py --input data/bank.csv --output-dir /tmp/bank-chunks --rows 120 --chunk-size 30
python src/streaming/run_streaming_demo.py --model outputs/model/bank_subscription_logistic --inbox /tmp/bank-stream-inbox
python src/streaming/replay_chunks.py --source /tmp/bank-chunks --inbox /tmp/bank-stream-inbox --interval 2
~~~

For a one-shot bounded replay, copy prepared chunks into an inbox and add --available-now to the streaming command. This lets the job consume the files and exit.

## Data notes and interpretation

The target is term-deposit subscription (y). The executed Spark ML pipeline reached test PR-AUC 0.3554 and ROC-AUC 0.7592. Its default 0.50 threshold had 89.06% accuracy versus an 89.17% all-no baseline and only 1.08% subscriber recall; the notebook includes a 0.10/0.20/0.50 threshold sensitivity table. The supplied data has no direct customer identifier, event timestamp, loan principal, or year field. To avoid making unsupported claims:

- Streaming event_time is synthetic replay metadata; it is not present in bank.csv.
- The parallel age-band question asks about loan amounts, but the dataset only has a personal-loan yes/no flag and a balance field. The source describes balance as an average yearly balance in euros; the project reports that balance among personal-loan holders as an explicitly labelled proxy, not as loan principal.
- The Hive year-over-year contact request cannot be computed from a single campaign snapshot with no year/date. The project reports contact counts by month instead. The yearly-balance field can be compared across education groups, but it is not a multi-year time series.
- duration is excluded from model features because it is observed during/after the call and can leak information into a pre-contact prediction. It remains in the EDA and streaming summaries.
- This is a small local-mode demonstration. Partition count and elapsed time do not show a cluster speed-up on 4,521 rows.

## Reference material

The supplied telecom churn notebooks were reviewed for their EDA and ML walkthrough structure. Telecom-specific data, columns, outputs, and conclusions are not reused. This repository includes a compact [Colab companion](notebooks/Financial_Forecasting_Frontier_Distributed_ML.ipynb) based on the linked [Financial Forecasting Frontier notebook](https://github.com/amol004/Financial-Forecasting-Frontier-Distributed-ML/blob/main/Financial_Forecasting_Frontier_Distributed_ML.ipynb) and the associated [Hadoop, MapReduce, Hive, and PySpark PDF](https://github.com/amol004/Financial-Forecasting-Frontier-Distributed-ML/blob/main/Banking%20Data%20Analysis%20with%20Hadoop%2CMapreduce%2C%20Apache%20Hive%20and%20PySpark.pdf). The companion uses this repository's dataset and documents where its workflow differs from the source notebook. The [two-part video script](docs/reference_colab_video_script.md) covers both references.

The referenced ZIP download was not present at the supplied path, so the deliverables use the available bank.csv and the two notebooks in the Demo Projects folder.

## Dataset attribution

The supplied 4,521-row file matches the 17-feature sample variant of the UCI Bank Marketing dataset. Attribute it to S. Moro, P. Rita, and P. Cortez, “Bank Marketing,” UCI Machine Learning Repository, DOI: 10.24432/C5K306. UCI lists the dataset under CC BY 4.0; the repository includes this attribution. See [the UCI dataset record](https://archive.ics.uci.edu/dataset/222/bank%2Bmarketing) and the [CC BY 4.0 license](https://creativecommons.org/licenses/by/4.0/).
