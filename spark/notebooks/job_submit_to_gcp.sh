
    # "clusterName": "data-processing-cluster-ny-taxi"
 
    # "mainPythonFileUri": "gs://lateral-attic-426206-c4-spark-nytaxi-data/code/spark_gcs_master.py",
    # "args":
    #   "--input_green=gs://lateral-attic-426206-c4-spark-nytaxi-data/pq/green/pq/*/",
    #   "--input_yellow=gs://lateral-attic-426206-c4-spark-nytaxi-data/pq/yellow/pq/*/",
    #   "--output=gs://lateral-attic-426206-c4-spark-nytaxi-data/report/revenue/"

  gcloud dataproc jobs submit pyspark \
    --cluster=data-processing-cluster-ny-taxi \
    --region=europe-west4 \
    gs://lateral-attic-426206-c4-spark-nytaxi-data/code/spark_gcs_master.py \
    -- \
      --input_green=gs://lateral-attic-426206-c4-spark-nytaxi-data/pq/green/pq/*/ \
      --input_yellow=gs://lateral-attic-426206-c4-spark-nytaxi-data/pq/yellow/pq/*/ \
      --output=gs://lateral-attic-426206-c4-spark-nytaxi-data/report/revenue2/