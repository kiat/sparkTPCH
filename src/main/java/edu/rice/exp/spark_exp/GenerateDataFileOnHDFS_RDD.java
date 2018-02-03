package edu.rice.exp.spark_exp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.rice.dmodel.Customer;
import edu.rice.dmodel.MyKryoRegistrator;
import edu.rice.generate_data.DataGenerator;

// used SPARL Submit command like this
// spark-submit --master  spark://10.134.96.100:7077  --class edu.rice.exp.spark_exp.GenerateDataFileOnHDFS  --executor-cores 4  --executor-memory 4G  --driver-memory 10G ./target/spark-exp-0.0.1-SNAPSHOT-jar-with-dependencies.jar  "5,10,15,20"  0.2  10


public class GenerateDataFileOnHDFS_RDD {

	public static void main(String[] args) throws FileNotFoundException, IOException {

		String hdfsNameNodePath = "hdfs://ip-172-30-4-47:9000/customer-";

		// define the number of partitions
		int numPartitions = 128;

		int NUMBER_OF_COPIES = 1;// number of Customers multiply X
									// 2^REPLICATION_FACTOR
		String fileScale = "0.2";
		
		// 1=serialize Data, 0=don't serialize data
		// by default it serializes data
		int serializeData = 1;		

		// 1=compress Data, 0=don't compress data
		// by default it compresses data		
		int compressData = 1;		
		
		// if (args.length > 0)
		// NUMBER_OF_COPIES = Integer.parseInt(args[0]);

		// read a set of integer numbers - data sets that we need to save on
		// hdfs.
		// The numbers indicate the replication number of data, on each of these
		// numbers
		// we save the object file on a separate folder to be able to access
		// them for the final experiments
		String s = args[0];
		String[] numberOfCopies_string = s.split(",");
		Set<Integer> numberOfCopies_set = new HashSet<Integer>(numberOfCopies_string.length);

		for (int i = 0; i < numberOfCopies_string.length; i++) {
			int tmp = Integer.parseInt(numberOfCopies_string[i].replaceAll(" ", ""));
			numberOfCopies_set.add(tmp);
			if (i == (numberOfCopies_string.length - 1))
				// the last number is the maximum
				NUMBER_OF_COPIES = tmp;
		}

		if (args.length > 1)
			fileScale = args[1];

		if (args.length > 2)
			numPartitions = Integer.parseInt(args[2]);

                String hdfsInputPath = "";
                if (args.length > 3)
                        hdfsInputPath = args[3];

		if (args.length > 4)
			hdfsNameNodePath = args[4];

		if (args.length > 5)
			serializeData = Integer.parseInt(args[5]);

		if (args.length > 6)
			compressData = Integer.parseInt(args[6]);
		
		SparkConf conf = new SparkConf();

		// PropertyConfigurator.configure("log4j.properties");

		conf.setAppName("GenerateDataFileOnHDFS-"+NUMBER_OF_COPIES);

		String filePrefix = "";
		if (serializeData == 1 && compressData == 1) {			
			// Kryo Serialization
			conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
			conf.set("spark.kryo.registrationRequired", "true");
			conf.set("spark.kryo.registrator", MyKryoRegistrator.class.getName());
			conf.set("spark.io.compression.codec", "lzf"); // snappy, lzf, lz4			
		} else {			
			hdfsNameNodePath = hdfsNameNodePath + "no-compress-no-serial/customer-";				
		}
		
		// hadoop configurations
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		// set the block size to 256 MB
		conf.set("fs.local.block.size", "268435456");
                conf.set("spark.rpc.message.maxSize", "512");
		JavaSparkContext sc = new JavaSparkContext(conf);
                if (hdfsInputPath == "") {
		    JavaRDD<Customer> customerRDD_raw = sc.parallelize(DataGenerator.generateData(fileScale), numPartitions);
		    JavaRDD<Customer> customerRDD = customerRDD_raw;
                    customerRDD_raw.cache();
		    if (NUMBER_OF_COPIES==1){
			System.out.println("Saving the dataset for 1");
			// coalesce the RDD based on number of partitions.				
			customerRDD.saveAsObjectFile(hdfsNameNodePath + (1));			
		    } else {

			// Copy the same data multiple times to make it big data
			for (int i = 1; i < NUMBER_OF_COPIES; i++) {
				customerRDD = customerRDD.union(customerRDD_raw);
				System.out.println("Appending set-> " + i);
				if (numberOfCopies_set.contains((i+1))) {
					System.out.println("Saving the dataset for " + i);
					// coalesce the RDD based on number of partitions.				
                                        customerRDD = customerRDD.coalesce(numPartitions);
					customerRDD.saveAsObjectFile(hdfsNameNodePath + (i+1));
				}
			}
		   }
                } else {
                    JavaRDD<Customer> customerRDD_raw = sc.objectFile(hdfsInputPath, numPartitions);
                    customerRDD_raw.cache();
                    JavaRDD<Customer> customerRDD = customerRDD_raw;
                    if (NUMBER_OF_COPIES==1){
                        System.out.println("Saving the dataset for 1");
                        // coalesce the RDD based on number of partitions.                              
                        customerRDD.saveAsObjectFile(hdfsNameNodePath + (1));
                    } else {

                        // Copy the same data multiple times to make it big data
                        for (int i = 1; i < NUMBER_OF_COPIES; i++) {
                                customerRDD = customerRDD.union(customerRDD_raw);
                                System.out.println("Appending set-> " + i);
                                if (numberOfCopies_set.contains((i+1))) {
                                        System.out.println("Saving the dataset for " + i);
                                        // coalesce the RDD based on number of partitions.                              
                                        customerRDD = customerRDD.coalesce(numPartitions);
                                        customerRDD.saveAsObjectFile(hdfsNameNodePath + (i+1));
                                }
                        }
                   }


                }

	}
}
