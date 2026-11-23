# Results and interpretation

These figures were calculated from the supplied bank.csv as a ground-truth check for the Spark/Hive code. The Spark cells print their own outputs when run in Colab. The local Hadoop Streaming replay runs the included mapper and reducer programs through a local sort; it demonstrates their logic, not execution on an HDFS cluster.

## Dataset profile

| Check | Result |
| --- | ---: |
| Rows × columns | 4,521 × 17 |
| Subscribed (yes) | 521 (11.52%) |
| Not subscribed (no) | 4,000 (88.48%) |
| Missing cells | 0 |
| Exact duplicate rows | 0 |
| Age range / median | 19–87 / 39 |
| Balance mean / median | 1,422.66 / 444 |
| Balance range | −3,313 to 71,188 |

The target is imbalanced, so accuracy alone is not enough to assess the classifier. Read area under the precision-recall curve, recall, precision, and a confusion matrix together with the majority-class baseline.

## Spark EDA and Hive-equivalent findings

**Average account balance and median age by job** (top five by mean balance):

| Job | Clients | Mean balance | Median age |
| --- | ---: | ---: | ---: |
| retired | 230 | 2,319.19 | 59 |
| housemaid | 112 | 2,083.80 | 49 |
| management | 969 | 1,766.93 | 38 |
| entrepreneur | 168 | 1,645.13 | 41 |
| student | 84 | 1,543.82 | 26 |

Other checks:

- 1,481 clients have balance above 1,000.
- Age-group mean balances are 1,058.10 for under 30 (482 clients), 1,426.88 for ages 30–60 (3,912), and 2,676.35 for over 60 (127).
- Average yearly balance by education is 1,411.54 (primary), 1,196.81 (secondary), 1,775.42 (tertiary), and 1,701.25 (unknown). Tertiary is highest and secondary lowest, but their means are within 0.12 overall standard deviations of the full-sample mean; this is not a strong anomaly signal.
- Subscriptions by marital status: married 277, single 167, divorced 77.
- Subscription rates by education: tertiary 14.30%, secondary 10.62%, unknown 10.16%, primary 9.44%.
- Highest credit-default rates by job: entrepreneur 4.17% (7/168), unemployed 2.34% (3/128), self-employed 2.19% (4/183). The denominators are small for some jobs, so rates should be interpreted carefully.
- May is the busiest contact month: 1,398 clients and 93 subscriptions, a 6.65% subscription rate. October has a much higher rate (46.25%) but only 80 contacts; volume and rate answer different questions.
- Mean contact duration is 552.74 seconds for subscribers and 226.35 seconds for non-subscribers; medians are 442 and 167 seconds.
- Pearson correlation between age and account balance is 0.0838, a very weak positive linear relationship.
- Credit default: 76 yes (1.68%) and 4,445 no (98.32%).
- Contact-method subscription rates: telephone 14.62% (44/301), cellular 14.36% (416/2,896), unknown 4.61% (61/1,324). The telephone sample is much smaller than cellular.
- Previous outcome rates: success 64.34% (83/129), other 19.29% (38/197), failure 12.86% (63/490), unknown 9.10% (337/3,705).

## Hadoop and Hive

The mappers/reducers cover: average balance per job, housing-loan counts by education, monthly counts split by subscription result, mean duration by prior campaign outcome, and age/balance correlation. The SQL file covers the requested table setup, filters, sorts, aggregates, contact-month analysis, correlation, balance anomalies by education, and previous-outcome analysis.

No Hadoop or Hive service was installed in the authoring environment. The Colab notebook locally replays the Streaming mapper/reducer functions and prints the SQL and cluster commands; a real HDFS/Hive execution requires a configured cluster. The year-over-year contact trend cannot be computed because the CSV has no year field. Its balance variable is an average yearly balance per client, so cross-sectional comparisons by education are possible.

## Machine learning

The Spark ML pipeline indexes categorical predictors, one-hot encodes them, robust-scales numeric predictors, assembles a feature vector, and tunes Logistic Regression with three-fold CrossValidator search over four parameter settings. It uses an outcome-stratified sample split. Missing values are filled defensively (the supplied file has none). Robust scaling is fit inside each CV fold so legitimate extreme balances are not deleted. The −1 pdays marker is represented with a separate prior-contact flag.

On the 859-row test split (93 subscribers), the best cross-validation PR-AUC was 0.3411; test PR-AUC was 0.3554 and ROC-AUC was 0.7592. The default 0.50 cutoff had 89.06% accuracy versus an 89.17% all-no baseline, and subscriber recall was only 1.08%. A fixed 0.20 threshold sensitivity example produced 45.45% subscriber precision, 37.63% recall, and 0.4118 F1 (35 true positives, 42 false positives, and 58 false negatives). A 0.10 threshold raised recall to 79.57% but precision fell to 16.48%. These examples show the precision/recall tradeoff; the test set was not used to tune or select an operating threshold.

The model excludes duration because it is known only during or after a call. Treating it as an input to a pre-contact decision could inflate performance through outcome leakage. The modest ranking metrics and near-baseline default-cutoff accuracy should be reported plainly. Coefficients describe associations in this dataset, not causal effects or a responsible lending decision.

## Streaming and data parallelism

The sample stream generator adds event IDs and synthetic event times, emits 120 rows in four files, and shifts every 17th event 20 seconds earlier. The streaming program predicts in foreachBatch micro-batches, updates balance and duration means by job, and aggregates 10-second and 1-minute windows with a 1-minute watermark. The watermark retains event-time state for expected lateness, but events later than the threshold are not guaranteed to be included. The synthetic timestamps demonstrate mechanics only; they do not represent observed transaction timing.

The parallelism script repartitions by job, aggregates mean balance, trains the same Logistic Regression pipeline over four partitions, and submits independent group summaries through Spark's fair scheduler. The requested top-five age-group “loan amounts” are represented by total average yearly balance among personal-loan holders because loan principal is absent. CPU and RSS are sampled for the local Python driver and its visible JVM child process; this is not executor-wide or cluster telemetry.
