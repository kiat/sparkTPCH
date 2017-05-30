

# Submitting spark application to a remote cluster using spark-submit 

spark-submit   \\ 
    --master spark://cslinux18.cs.rice.edu:7077  --class edu.rice.exp.spark_exp.AggregatePartIDsFromCustomer  \\ 
    --driver-memory 32G \\ 
    ./target/spark-exp-0.0.1-SNAPSHOT.jar