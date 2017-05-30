

# Submitting spark application to a remote cluster using spark-submit 

spark-submit \
  --master spark://master-url \
  --class AggregatePartIDsFromCustomer \
  target/spark-exp-0.0.1-SNAPSHOT.jar