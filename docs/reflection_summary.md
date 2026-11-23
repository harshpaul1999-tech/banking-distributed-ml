# Reflection summary (draft to personalize)

This project helped me connect five parts of a distributed banking analytics workflow: file ingestion, SQL analysis, Spark transformations, predictive modeling, and stream processing. I practiced how Hadoop Streaming mappers emit key/value pairs and how reducers calculate grouped totals and averages. I also wrote Hive DDL and queries to analyze the HDFS data using a table-oriented interface.

The Spark EDA work reinforced the value of checking schema, missingness, duplicates, and class balance before modeling. The input contained 4,521 rows, no missing cells, and an 11.52% subscription rate. That imbalance made it important to report precision-recall measures and the confusion matrix alongside accuracy. I also learned to keep feature processing in the Spark ML pipeline and to inspect coefficient direction without interpreting it as causation.

A few requested questions exposed limits in the source file. There is no event timestamp, so the streaming timestamp had to be synthetic. The file has a personal-loan indicator but no loan principal, so account balance was used only as a clearly marked proxy for the age-band exercise. There is no year field, so year-over-year contact trends could not be calculated. The dataset does provide an average yearly balance per client, which can be compared across groups but is not a multi-year time series. I learned to explain these limits directly rather than create unsupported conclusions.

The streaming work showed how event-time windows and watermarks balance late-data tolerance against state cleanup. The synthetic replay demonstrates mechanics, but it is not evidence of live banking activity. The parallelism exercise showed how partitioning and scheduling can organize concurrent work, while also making clear that this small local dataset cannot establish a cluster speed-up or provide cluster-wide resource measurements.

Before submission, I will run the Colab notebook in the target runtime, replace this draft with the challenges I personally encountered, record the actual model and streaming outputs, and make sure the presentation script reflects those outputs.
