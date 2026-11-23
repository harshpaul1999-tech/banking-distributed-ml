# Video presentation script: Banking Analytics with Distributed ML

**Target runtime:** about 18–20 minutes at a clear pace of roughly 120–135 words per minute, including short pauses to show the notebook tables. The five rubric sections are all covered. The model figures below match the executed Colab notebook.

## 00:00–00:55 — Opening and objective

Hello, and welcome to my project presentation on banking data analysis with Hadoop, Hive, Apache Spark, Spark ML, and Structured Streaming. The project uses the supplied bank marketing dataset to show how a bank can move from storing records, to exploring them, to predicting a campaign outcome, and finally to processing new records in small batches.

I have organized the work into the five requested parts. Part one covers Hadoop MapReduce and Hive. Part two is Spark data processing and exploratory analysis. Part three builds and evaluates a classification model in Spark ML. Part four simulates a live stream and analyzes windows and late events. Part five demonstrates data parallelism, resource observations, and scheduling. The implementation is in a Colab-ready notebook, with the Python, SQL, and replay scripts beside it in the repository.

A key point throughout the presentation is that distributed tools do not create information that the data does not contain. I call out the limits of this dataset when a requested question needs a timestamp or loan amount that is not present.

## 00:55–02:10 — Dataset and preparation

The source file has 4,521 records and 17 columns. Each record describes a client and one marketing contact. The fields include age, job, marital status, education, credit default, account balance, housing and personal-loan flags, contact method, month, contact duration, campaign counts, days since an earlier contact, the previous campaign outcome, and the target y. The target is yes when a client subscribed to a term deposit and no otherwise.

I first checked the header, schema, row count, missing values, and exact duplicate rows. The file has no missing cells and no exact duplicate records. There are 521 yes outcomes and 4,000 no outcomes, so the positive class is about 11.52 percent. That imbalance matters later: an accuracy figure alone can look high even if a model misses most subscribers.

The balance field is the client's average yearly balance, not a personal-loan principal. There is no year or event timestamp in the file. For the streaming demonstration, I add synthetic event IDs and timestamps so Spark can order sample events. I keep those synthetic values clearly separate from observations in the original data.

## 02:10–04:45 — Part one: Hadoop and MapReduce

The Hadoop part begins with creating an HDFS directory and copying bank.csv into it. The provided shell commands show the ingestion step. The Python mapper and reducer programs use Hadoop Streaming's line interface: mappers emit a key and a compact value, Hadoop sorts and groups keys, and reducers combine values for the final answer.

The first job groups balance totals and row counts by job category. The reducer divides each total by its count to return average account balance by profession. The next job counts people with and without a housing loan in each education category. It uses a combined education and housing key, so each group is reduced independently.

A third job groups the month and subscription result, which gives both the number of contacts and the number of subscriptions per month. The fourth computes average contact duration for each previous campaign outcome. The fifth job emits age and balance pairs and combines their sums to calculate a Pearson correlation. This is a compact example of a map-side summary followed by a reduce-side calculation.

I also tested the mapper and reducer logic locally by feeding the CSV through each mapper, sorting the emitted keys, and running the reducers. That checks the scripts' transformations. It does not claim that a Hadoop cluster ran in this environment. The repository contains the HDFS commands so the same jobs can run on a configured cluster with the Hadoop Streaming JAR and Python installed on worker nodes.

The age and balance correlation is approximately 0.0838, which is a very weak positive linear relationship. This is a descriptive association, not evidence that age causes account balances to change.

## 04:45–06:55 — Part one continued: Hive

Hive provides a table layer over files stored in HDFS. I create the banking_data database and an external client_info table with integer age and contact counts, a large integer account balance, and string columns for categories. The OpenCSVSerde settings handle comma-separated rows and the table property skips the header line.

The Hive queries cover the requested questions: total clients and sample rows; married clients with personal loans; the ten largest balances; average age by job; and credit-default counts by education. The more analytical queries rank job categories by average balance and report each group's term-deposit subscription percentage. Another query identifies the busiest campaign month and computes that month's conversion rate.

For this dataset, May is the busiest month with 1,398 contacts. Ninety-three clients subscribed, giving a 6.65 percent rate. October has a much higher rate, 46.25 percent, but only 80 contacts. The difference is useful: a campaign team should not confuse a high rate in a small group with the largest number of subscriptions.

The Hive file also calculates average yearly balance by education and compares group means with the overall distribution. Tertiary is highest at about 1,775 euros and secondary lowest at about 1,197 euros; these differences are modest compared with the variation between individual clients. It also calculates previous-outcome subscription rates and average durations, and a cross-sectional correlation between age and balance. Two requests need a careful limitation. There is no year column, so a year-over-year contact trend cannot be calculated. The source describes balance as an average yearly balance per client, so comparing it by education is possible; the file still does not provide a multi-year series. Those are data limits, not missing SQL syntax.

## 06:55–09:35 — Part two: Spark EDA and data processing

Spark loads the CSV into a DataFrame with a header and inferred numeric types. I show the first five rows, print the schema, summarize the numeric columns, and check nulls and duplicates. The notebook then works through the requested transformations.

First, it filters clients whose account balance is above 1,000; there are 1,481 such rows. The month abbreviation is mapped to a quarter: January through March are quarter one, April through June quarter two, July through September quarter three, and October through December quarter four. This is a month-to-quarter mapping because the source contains month names but no full date.

For group summaries, the notebook calculates average balance and median age by job. Retired clients have the highest mean balance at about 2,319, followed by housemaids at about 2,084. Management has the largest group among these examples, with 969 clients and a mean balance near 1,767. Counts are shown with averages because a small group can have a high mean from a few large balances.

A user-defined function puts clients into under-30, 30-to-60, and over-60 age groups. I also concatenate job with marital status, convert contact method to uppercase, calculate education subscription rates, and rank professions by default rate. Entrepreneurs have the highest default rate in this file at 4.17 percent, but that is only seven defaults among 168 clients. Small denominators mean the ranking should not be treated as a stable estimate of risk.

For visualization, the notebook collects only an aggregate count by job into Pandas and draws a bar chart. That is appropriate for this small result table; collecting all rows from a large Spark dataset would not scale. The remaining questions compare duration for subscribers and non-subscribers, calculate the age-balance correlation, show default counts, compare subscription rates by contact method, and run a Spark SQL query over a temporary view.

Mean contact duration is about 553 seconds for subscribers and 226 seconds for non-subscribers. Duration is strongly associated with the outcome in this campaign file, but it is observed during or after the conversation. It is therefore a risky predictor for a decision that must be made before the call.

## 09:35–12:20 — Part three: Spark ML model

The target is binary, so I use Logistic Regression. It is a useful baseline because it produces a probability and its coefficients can be inspected. Categorical predictors such as job, marital status, education, contact method, and previous outcome are indexed and one-hot encoded. Numeric predictors are combined into a vector and robust-scaled. The Spark ML pipeline fits its transformations inside cross-validation folds.

The source has no missing values, but the pipeline fills missing numeric and categorical values defensively so it is easier to reuse. Outliers are inspected with an interquartile-range diagnostic. Instead of deleting large or negative balances that may be real, the numeric pipeline uses a robust scaler based on medians and interquartile ranges. The special pdays value of minus one means no previous contact, so I make a separate previously-contacted flag and set the no-history numeric value to zero.

The data is split by outcome using Spark's sampleBy operation. This keeps the minority subscription class represented in both training and test samples. The parameter grid searches two regularization strengths and two elastic-net mixes, using three-fold CrossValidator and area under the precision-recall curve to choose among the candidates. That is four parameter settings across three folds.

On the held-out data, the best cross-validation PR-AUC is 0.3411. Test PR-AUC is 0.3554 and ROC-AUC is 0.7592. At the default threshold of 0.50, accuracy is 89.06 percent, slightly below the 89.17 percent all-no baseline, and subscriber recall is only 1.08 percent. This is why the notebook also prints a threshold sensitivity table. At a fixed illustrative threshold of 0.20, subscriber precision is 45.45 percent and recall is 37.63 percent, with F1 of 0.4118. The confusion matrix shows 35 true positives, 42 false positives, and 58 missed subscribers. At 0.10, recall rises to 79.57 percent while precision falls to 16.48 percent. These thresholds are a sensitivity display, not values selected by optimizing the held-out test set. The comparison should focus on whether subscribers are identified, not just overall accuracy.

The model excludes duration because including post-contact call length can leak information about the outcome. The largest positive and negative coefficients are printed with feature names when available. They describe associations in this sample, not causal effects. This educational model is not a lending or eligibility system and should not be used to make decisions about individual customers.

## 12:20–15:05 — Part four: Structured Streaming

The streaming example uses Spark's file source. The generator takes a small subset of historical rows, adds an event ID and a synthetic event time, and writes four CSV chunks. The replay script can copy one chunk at a time into a watched inbox. The standalone program then reads new files as micro-batches.

For each job category, the stream updates the transaction count, average account balance, and average contact duration. A second aggregation groups events into ten-second windows, and a third uses one-minute windows. The shorter window reacts quickly and is more variable. With sample events spaced five seconds apart, a typical ten-second window contains about two rows, while a one-minute window contains roughly twelve. The longer window smooths short changes and makes the overall level easier to read.

For predictions, I load the model trained on historical rows and call it from foreachBatch. This is important because each micro-batch is an ordinary batch DataFrame that the fitted Spark ML pipeline can score. Each printed prediction includes the event ID, timestamp, job, balance, duration, actual y label from the historical replay, estimated probability, and predicted class. A live application would usually receive an unlabeled event; the y column is retained here only so the demonstration can compare predictions with known historical outcomes.

The file has no timestamps, so the event times are synthetic. Every seventeenth event is assigned a time 20 seconds earlier than its sequence would suggest. The event-time window aggregations use a one-minute watermark, allowing bounded late updates and eventually removing old window state. A watermark is an operational bound: events later than the configured delay are not guaranteed to be included. It improves state management while allowing ordinary out-of-order arrivals inside the delay.

## 15:05–17:10 — Part five: data parallelism and resource management

The data-parallelism script repartitions rows by job category into four partitions, then calculates average balance by job. Spark can execute work for those partitions in parallel and combine partial aggregates after a shuffle. For the model task, the same Logistic Regression pipeline is fitted on a partitioned training split and evaluated on a holdout set. The notebook reports the number of partitions and a test AUROC.

The exercise also asks for the five age groups with the largest loan amounts. This dataset has a personal-loan yes/no flag, not loan principal. Its balance field is an average yearly balance per client. I therefore calculate total yearly balance among personal-loan holders in ten-year age bands and label it a proxy. The leading bands are ages 30–39 with 206,687 total balance across 271 clients, ages 50–59 with 204,379 across 160 clients, ages 40–49 with 155,559 across 184 clients, ages 20–29 with 62,879 across 68 clients, and ages 60–69 with 4,256 across eight clients. These are average-yearly-balance totals, not outstanding debt amounts.

For resource monitoring, psutil samples the Python driver and its visible local JVM child before and after several actions. These local process readings do not describe all executors in a distributed cluster. The Spark job scheduler is set to FAIR, and two independent group summaries are submitted through separate threads in different scheduler pools. The scheduler can interleave tasks, but the local runtime still has limited CPU and memory. This small dataset is meant to demonstrate parallel execution, not to prove a speedup.

A practical challenge is that data parallelism only helps when each task can be partitioned and the combined work is worth the shuffle and coordination cost. Another is keeping resource readings in context: one local process is not a full cluster monitor. The partition count, output, and CPU/RSS snapshots are included so the choices can be inspected.

## 17:10–18:00 — Reflection and close

The strongest learning from this project is to line up the question, the available fields, and the method before interpreting a result. Hadoop Streaming shows map, shuffle, and reduce. Hive provides SQL over HDFS files. Spark DataFrames make repeated transformations and aggregations readable. Spark ML pipelines keep categorical encoding and model fitting in one reproducible flow. Structured Streaming adds event-time windows and bounded state to incoming micro-batches.

The data also taught me to state limits precisely. There is no event timestamp, year, or loan amount, so synthetic event times and account-balance proxies must be labeled. A model can appear to perform well if the target is imbalanced or if a post-outcome feature is included, so I report minority-class metrics and exclude call duration from pre-contact features.

Thank you for watching. The repository contains the notebook, scripts, SQL, sample stream files, findings, and this presentation script. The notebook can be opened in Google Colab from the repository and rerun to reproduce the Spark outputs.
