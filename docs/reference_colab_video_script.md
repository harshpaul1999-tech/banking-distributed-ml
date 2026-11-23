# Video scripts: the reference PDF and Colab notebook

This file contains two short video scripts. Video 1 follows the PDF's GCP, HDFS, MapReduce, and Hive walkthrough. Video 2 follows the Financial Forecasting Frontier notebook and the runnable companion notebook in this repository.

## Video 1 — Banking data analysis with Hadoop, MapReduce, Hive, and PySpark

**Target runtime:** 8–9 minutes  
**Screen materials:** the linked PDF, then Part 1 of `Banking_Distributed_Analytics_Colab.ipynb` for the Colab boundary.

### 00:00–00:40 — Opening

**On screen:** PDF cover, then title.

**Narration:**

Hello. This video explains the banking analytics workflow in the PDF, which combines Hadoop storage, MapReduce jobs written with mrjob, Apache Hive queries, and PySpark on Google Cloud's managed Spark service. The walkthrough in the PDF is centered on a configured GCP cluster. I will also show how the repository's Colab notebook demonstrates the same ideas locally and which parts still need cloud services.

### 00:40–01:35 — Project architecture

**On screen:** PDF page 1; point to the four deliverables.

**Narration:**

The project begins with the bank marketing CSV. HDFS provides distributed file storage. MapReduce groups records and calculates summaries. Hive gives SQL access to the data in cloud storage. PySpark is the interactive analysis layer on the managed Spark cluster. The flow is: prepare the cluster, make the dataset available, run distributed aggregations, and query the results through Hive.

### 01:35–02:35 — Managed Spark cluster

**On screen:** PDF pages 1–3; show the Dataproc cluster settings table on page 2.

**Narration:**

The PDF first provisions a managed Apache Spark cluster, formerly called Dataproc. It lists a cluster name, region and zone, image version, one master and two workers, machine types, and optional components such as Jupyter and Hive WebHCat. These settings define the environment for the later jobs. They are the PDF's example configuration; use the settings approved for the cloud account and project where you run the work.

### 02:35–03:20 — HDFS and dataset staging

**On screen:** PDF pages 3–4; show the SSH and filesystem checks.

**Narration:**

Next, the project stages `bank.csv` for distributed processing. The PDF directs the reader to connect to the cluster's master node over SSH and verify the machine and file environment. Its screenshots show shell checks such as listing files and checking disk usage. The important idea is to confirm that the cluster and input data are available before starting the jobs.

### 03:20–05:35 — MapReduce jobs

**On screen:** PDF pages 5–8; highlight one mapper and reducer, then the remaining job descriptions.

**Narration:**

The MapReduce section uses four Python mrjob programs. A mapper reads a CSV row and emits a key-value pair. Hadoop groups values by key, and the reducer combines them. For example, a mapper can emit a job category and a subscription count; the reducer adds the counts for each category.

The four analyses count term-deposit subscriptions by job, calculate monthly conversion rates, calculate average balances by marital status, and calculate average balances by education. Each job follows the same pattern: parse the row, skip the header, choose the grouping key, emit the needed values, and reduce them into a summary. This is what makes MapReduce useful for work that can be split into groups and combined again.

### 05:35–06:25 — Reading the MapReduce outputs

**On screen:** PDF pages 9–13; trace each job name to its output path and show the corresponding shell output.

**Narration:**

The output table maps each analysis to a storage path and a command for reading its result. When reviewing an output, check that it belongs to the expected job and that the reducer's grouping and aggregation match the question. The output is a summary of this dataset, not a statement about every banking customer or a guarantee about a future campaign.

### 06:25–07:40 — Hive table and SQL

**On screen:** PDF pages 14–18; show the external-table DDL and the three sample queries.

**Narration:**

Hive adds a table definition over the CSV files in Google Cloud Storage. The external table declares the seventeen banking columns, their types, comma-separated fields, and the location of the source files. The header-line property tells Hive to skip the CSV header. Because the table is external, deleting the table definition does not delete the underlying file.

The PDF then demonstrates three Hive queries: average balance by job, term-deposit subscription counts by education, and monthly counts of successful subscriptions. These SQL statements show how the same stored banking data can be queried with familiar group-by operations.

### 07:40–08:35 — What Colab can reproduce

**On screen:** repository notebook Part 1 and its README note about local replay versus a cluster.

**Narration:**

In this repository, the Colab notebook includes the Hadoop Streaming commands and Hive SQL, and it locally replays the mapper and reducer logic so the aggregation can be inspected without a cluster. That local replay is not a Hadoop cluster run. Colab also does not start HDFS or Hive automatically. To demonstrate those services end to end, connect the commands to a configured GCP environment. The PDF explains that cloud path; the notebook gives a reproducible local companion.

### 08:35–08:50 — Close

**On screen:** repository README with links to both notebooks and the PDF.

**Narration:**

That completes the storage, MapReduce, and Hive walkthrough. The next video follows the PySpark EDA, machine-learning, and streaming notebook. The repository links the Colab notebooks, source files, and the reference materials together.

## Video 2 — Financial Forecasting Frontier: PySpark Colab walkthrough

**Target runtime:** 12–14 minutes  
**Screen materials:** the reference notebook on GitHub for its original flow, then `Financial_Forecasting_Frontier_Distributed_ML.ipynb` in Colab for the runnable companion.

### 00:00–00:45 — Opening and scope

**On screen:** notebook title and Colab badge.

**Narration:**

Welcome. This video walks through the Financial Forecasting Frontier banking notebook. It uses PySpark to inspect the bank dataset, explore segments, build a Random Forest classifier, and demonstrate event-time streaming. The repository includes a compact Colab companion that reads the project's `bank.csv` directly. The linked reference notebook and PDF are included as source material, with clear notes where the runnable project uses a different setup.

### 00:45–01:35 — Runtime and data loading

**On screen:** Colab setup cell; show repository clone, Java/PySpark setup, and the first five rows.

**Narration:**

The setup cell opens the public repository, locates `data/bank.csv`, and prepares a local Spark session. This avoids a personal Drive path and makes the notebook portable for viewers who can access the repository. Spark reads the header and infers the schema. The dataset has 4,521 rows and 17 columns, with customer profile, campaign, and subscription fields.

### 01:35–02:35 — Data quality and target

**On screen:** schema, null-count output, duplicate count, and the `y` value counts.

**Narration:**

The first checks print the types, exact duplicate count, missing-value counts, numeric summaries, and counts for the target. The target `y` records whether the customer subscribed to a term deposit. There are substantially more `no` outcomes than `yes` outcomes, so later model evaluation should not rely on accuracy alone. The `pdays` value of minus one means there was no previous contact; it should be read as a special marker.

### 02:35–03:40 — Spark aggregation and window logic

**On screen:** job summary table and running-balance window output.

**Narration:**

The notebook groups records by job and calculates customer counts, mean balance, subscriptions, and the observed subscription rate. It then uses a Spark window partitioned by job and ordered by age to build a running balance. A grouped summary and a window calculation answer different questions: one compares job categories, and the other calculates cumulative values within an ordered group.

### 03:40–04:55 — Spark SQL segments

**On screen:** three SQL cells and their outputs.

**Narration:**

The DataFrame is registered as a temporary SQL view. The first query examines high-balance clients by marital status. The second compares contact volume and subscription rate by education. The third summarizes age bands among subscribers. These are descriptive summaries of the sample. A higher conversion rate alone does not prove that a campaign aimed at that group will perform better next time, so the denominators and context matter.

### 04:55–06:10 — Charts and interpretation

**On screen:** age-decade outcome bars, job-rate bars, and month summary table.

**Narration:**

The notebook collects only small aggregated tables into Pandas for plotting. The age chart bins customers by decade and separates the yes and no outcomes. The job chart compares observed rates, and the monthly table shows both contact volume and subscription rate. The original reference notebook contains ten chart experiments; this compact companion shows three representative views. In the longer reference, the same explanation pattern applies: why a chart was chosen, what it shows, and how cautiously to translate it into business action.

### 06:10–07:20 — Statistical checks

**On screen:** test code and printed p-values.

**Narration:**

The companion uses Welch's t-test to compare call durations for subscribers and non-subscribers, then chi-square tests for age band and previous campaign outcome against subscription. The reference notebook uses ANOVA for some comparisons. Here, the tests are matched to the variable types: a mean comparison for numeric duration and association tests for categorical groups. These are exploratory checks. A small p-value is not proof of causation or a measure of business value.

### 07:20–09:15 — Random Forest model

**On screen:** label encoding, indexers, one-hot encoding, vector assembler, Random Forest, and evaluation output.

**Narration:**

The target is encoded explicitly so `no` is zero and `yes` is one. StringIndexer and OneHotEncoder prepare categorical fields, VectorAssembler combines the predictors, and Spark ML fits a Random Forest on a training split. The notebook prints ROC-AUC, PR-AUC, and accuracy beside the all-no baseline. The source notebook's narrative reports an AUC near 0.88 for its own run; this companion computes metrics on its own split, so its values may differ.

This reference-style model includes call duration to match the source workflow. Duration is recorded during or after the call, so it can leak information into a prediction meant to be made before contact. Treat this as an educational demonstration. For pre-contact prediction, switch to the main project notebook's Logistic Regression pipeline, which excludes duration and reports minority-class metrics.

### 09:15–10:40 — Event-time streaming

**On screen:** four sample chunks, the 10-second aggregation, and the per-record prediction output.

**Narration:**

The source CSV does not contain transaction timestamps. The repository's stream sample adds synthetic event times and IDs and splits a small set of records into files. Structured Streaming reads those files as micro-batches, calculates transaction counts and average balances in ten-second windows, and applies the fitted model to incoming rows. The example uses a one-minute watermark to bound how long Spark retains state for late events. Events later than that bound are not guaranteed to revise earlier aggregates.

This is a finite replay of historical rows, not a live transaction feed. The timestamps illustrate the mechanics of event-time processing and should not be presented as observed customer activity.

### 10:40–11:40 — Gradio interaction

**On screen:** optional Gradio cell, JSON sample input, and prediction response.

**Narration:**

The optional Gradio cell wraps the fitted model in a small form. A sample customer record is converted to a Spark DataFrame and scored by the same pipeline. The result is a demonstration of how a model can be called from an interface. It is not a production banking service: the cell does not provide authentication, customer-data controls, monitoring, or an approved decision process.

### 11:40–12:30 — Close and limitations

**On screen:** companion notebook closing links and main project notebook.

**Narration:**

The notebook moves from data checks to Spark summaries, SQL, statistical comparisons, a classifier, and a bounded stream replay. The data limits remain important: there are no real transaction timestamps, no loan principal, and no year field. The stream uses synthetic time, and the model's duration feature is retrospective. The repository's main notebook provides the complete five-part assignment workflow, including Hadoop and Hive instructions and a leakage-aware pre-contact model. Thank you for watching.

## Recording checklist

- Show the output cells after running them; do not read code for the entire video.
- Do not describe the local MapReduce replay as a cluster run.
- Use the generated metric values from the recording session, and label the reference notebook's reported AUC as its own result.
- Keep the synthetic nature of stream timestamps visible when discussing the windows.
- End on the repository README, with links to the PDF, both notebooks, and source scripts.
