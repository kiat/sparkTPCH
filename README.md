
# Build it 
mvn clean package 

mvn clean compile assembly:single


# Submitting to Spark Cluster using spark-submit 

spark-submit   \\ 
    --master spark://cslinux18.cs.rice.edu:7077  --class edu.rice.exp.spark_exp.AggregatePartIDsFromCustomer_RDD  \\ 
    --driver-memory 2G --executor-memory 3G  \\ 
    ./target/spark-exp-0.0.1-SNAPSHOT-jar-with-dependencies.jar
    
    
    
spark-submit  --master spark://ip-10-150-167-66.ec2.internal:7077   --class edu.rice.exp.spark_exp.AggregatePartIDsFromCustomer_RDD  --deploy-mode cluster   --executor-cores 8  --queue default  /home/ubuntu/sparkTPCH/target/spark-exp-0.0.1-SNAPSHOT-jar-with-dependencies.jar


# Data Path 

data should be placed in folder 


/mnt/data$ ls 
tables_scale_0.1  tables_scale_0.2  tables_scale_0.3  tables_scale_0.5  tables_scale_1 
    