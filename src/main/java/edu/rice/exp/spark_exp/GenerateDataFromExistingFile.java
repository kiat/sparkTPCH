package edu.rice.exp.spark_exp;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import edu.rice.dmodel.Customer;
import edu.rice.dmodel.MyKryoRegistrator;

// used SPARK Submit command like this
// spark-submit --master  spark://10.134.96.100:7077  --class edu.rice.exp.spark_exp.GenerateDataFileOnHDFS  --executor-cores 4  --executor-memory 4G  --driver-memory 10G ./target/spark-exp-0.0.1-SNAPSHOT-jar-with-dependencies.jar  "5,10,15,20"  0.2  10


public class GenerateDataFromExistingFile {

	public static void main(String[] args) throws FileNotFoundException, IOException {

		String hdfsNameNodePath = "hdfs://10.134.96.100:9000/user/kia/customer-";

		// define the number of partitions
		int numPartitions = 8;

		int sourceFactor = 80; // each copy has 2.4 million costumers
								  // since we are using the 80 factor file as source
		
		int factorToCopy = 2; 	  // how many times are we adding the original
								  // dataset (by default = 2, we duplicate)
		
		if (args.length > 0)
			sourceFactor = Integer.parseInt(args[0]);
		
		if (args.length > 1)
			factorToCopy = Integer.parseInt(args[1]);
		

		if (args.length > 2)
			numPartitions = Integer.parseInt(args[2]);

		if (args.length > 3)
			hdfsNameNodePath = args[3];

		SparkConf conf = new SparkConf();

		// PropertyConfigurator.configure("log4j.properties");

		conf.setAppName("GenerateDataFromExistingFile-"+(factorToCopy*sourceFactor));

		// Kryo Serialization
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kryo.registrationRequired", "true");
		conf.set("spark.kryo.registrator", MyKryoRegistrator.class.getName());

		conf.set("spark.io.compression.codec", "lzf"); // snappy, lzf, lz4
		// conf.set("spark.speculation", "true");
		// conf.set("spark.local.dir", "/mnt/sparkdata");

		// hadoop configurations
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		// set the block size to 256 MB
		conf.set("fs.local.block.size", "268435456");

		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Customer> customerRDD_raw = sc.objectFile(hdfsNameNodePath + sourceFactor); 
				
		JavaRDD<Customer> customerRDD=customerRDD_raw.coalesce(numPartitions);
		customerRDD.persist(StorageLevel.MEMORY_ONLY_SER());
		
		System.out.println("Get the number of Customers");

		// force spark to do the job and load data into RDD
		long numberOfCustomers = customerRDD.count();
		
     	System.out.println("Number of Customers in Source RDD: " + numberOfCustomers);

		// Copy the same data multiple times to make it big data
		for (int i = 1; i < factorToCopy; i++) {
			customerRDD = customerRDD.union(customerRDD_raw);

			System.out.println("Appending set-> " + i);
			
			if (i == (factorToCopy-1)) {
				System.out.println("Saving the dataset for " + i);
				// coalesce the RDD based on number of partitions.				
				customerRDD = customerRDD.coalesce(numPartitions);	
				// force spark to do the job and load data into RDD
				numberOfCustomers = customerRDD.count();
				
		     	System.out.println("Number of Customers in Final RDD: " + numberOfCustomers);
				
				customerRDD.saveAsObjectFile(hdfsNameNodePath + (factorToCopy*sourceFactor));
			}
		}

	}
}