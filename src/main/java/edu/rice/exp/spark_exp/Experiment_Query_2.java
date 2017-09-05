package edu.rice.exp.spark_exp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Function;
import scala.Tuple2;
import edu.rice.dmodel.Customer;
import edu.rice.dmodel.LineItem;
import edu.rice.dmodel.MyKryoRegistrator;
import edu.rice.dmodel.Order;
import edu.rice.dmodel.PartIDCount;
import edu.rice.generate_data.DataGenerator;

public class Experiment_Query_2 {

	public static void main(String[] args) throws FileNotFoundException, IOException {


		// Complex Query 2: What are the top-10 parts that are ordered with priority = "1-URGENT"
		//
		// Process is the following.
		//
		// 1. Loop through customer list, get the orders
		// 2. For each customer, loop through orders and get the list of linteitems
		// 3. For each customer and orders, loop through Parts and sum

		// can be overwritten by the fourth command line arg
		String hdfsNameNodePath = "hdfs://10.134.96.100:9000/user/kia/customer-";

		
		long startTime = 0;					// timestamp from the beginning
		long loadRDDTimestamp = 0;			// timestamp after loading RDD
		long countTimestamp = 0;			// timestamp after count
		long finalTimestamp = 0;			// timestamp final		

		double loadRDDTime = 0;				// time to load RDD in memory
		double queryTimeIncludesCount = 0;	// time from load RDD to count		
		double queryTime = 0;				// time to run the query
		double elapsedTotalTime = 0;		// total elapsed time		

		
		// define the number of partitions
		// can be overwritten by the 3rd command line arg
		int numPartitions=8;

		int NUMBER_OF_COPIES = 4;// number of Customers multiply X 2^REPLICATION_FACTOR
		String fileScale = "0.2";

		if (args.length > 0)
			NUMBER_OF_COPIES = Integer.parseInt(args[0]);

		if (args.length > 1)
			fileScale = args[1];
		
		if (args.length > 2)
			numPartitions = Integer.parseInt(args[2]);

		if (args.length > 3)
			hdfsNameNodePath = args[3];
		
		
		SparkConf conf = new SparkConf();
		conf.setAppName("Query-2-" + NUMBER_OF_COPIES);

		// Kryo Serialization
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kryoserializer.buffer", "1024mb");
		conf.set("spark.kryo.registrationRequired", "true");
		conf.set("spark.kryo.registrator", MyKryoRegistrator.class.getName());
		
		conf.set("spark.io.compression.codec", "lzf"); // snappy, lzf, lz4 
//		conf.set("spark.speculation", "true"); 
//		conf.set("spark.local.dir", "/mnt/sparkdata");
		
		
		conf.set("spark.shuffle.spill", "true");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		
		conf.set("fs.local.block.size", "268435456");

		// Get the initial time
		startTime = System.nanoTime();
		

		JavaRDD<Customer> customerRDD = sc.objectFile(hdfsNameNodePath + NUMBER_OF_COPIES); 
		
		
//		JavaRDD<Customer> customerRDD = sc.parallelize(DataGenerator.generateData(fileScale), numPartitions);
//
////		JavaRDD<Customer> customerRDD = customerRDD_raw;
//
//		// Copy the same data multiple times to make it big data
//		for (int i = 0; i < NUMBER_OF_COPIES; i++) {
//			customerRDD = customerRDD.union(customerRDD);
//		}
		
		// Caching made the experiment slower 
//		System.out.println("Cache the data");
		
//		customerRDD.persist(StorageLevel.MEMORY_ONLY_2());

//		customerRDD.persist(StorageLevel.MEMORY_AND_DISK());
		customerRDD.persist(StorageLevel.MEMORY_ONLY_SER());

		// Timestamp the load data step
		loadRDDTimestamp = System.nanoTime();
		
		customerRDD=customerRDD.coalesce(numPartitions);

		
		System.out.println("Get the number of Customers");

		// force spark to do the job and load data into RDD
		long numberOfCustomers = customerRDD.count();
     	System.out.println("Number of Customer: " + numberOfCustomers);
     	
     	
     	// do something else to have the data in memory 		
     	long numberOfDistinctCustomers = customerRDD.distinct().count();
     	System.out.println("Number of Distinct Customer: " + numberOfDistinctCustomers);

     	
     	
		// #############################################
		// #############################################
		// ######### MAIN Experiment #############
		// #############################################
		// #############################################

		// Now is data loaded in RDD, ready for the experiment
		// Start the timer
     	countTimestamp = System.nanoTime();

		JavaPairRDD<Integer, Integer> partIDandOne = customerRDD.flatMapToPair(new PairFlatMapFunction<Customer, Integer, Integer>() {

			private static final long serialVersionUID = -1932241861741271488L;

			@Override
			public Iterator<Tuple2<Integer, Integer>> call(Customer customer) throws Exception {
				List<Order> orders = customer.getOrders();

				List<Tuple2<Integer, Integer>> returnList = new ArrayList<Tuple2<Integer, Integer>>();

				for (Order order : orders) {
					
					// only if the order priority is urgent then we get the list of lineitems and then the parts
					if (order.getOrderpriority().equals("1-URGENT")) {
						List<LineItem> lineItems = order.getLineItems();
						for (LineItem lineItem : lineItems) {
							Tuple2<Integer, Integer> returnValue = new Tuple2<Integer, Integer>(lineItem.getPart().getPartID(), 1);
							returnList.add(returnValue);
						}
					}
				}
				return returnList.iterator();
			}
		});
		
		
		// Reduce it by partID as key 
		JavaPairRDD<Integer, Integer> result=partIDandOne.reduceByKey(new Function2 <Integer, Integer, Integer>(){

			private static final long serialVersionUID = -1924109337454612429L;
			@Override
			public Integer call(Integer a, Integer b) throws Exception {
				return a+b;
			}
		}); 

		// we need to map it to an RDD to be able to get the top 10
		JavaRDD<PartIDCount> changedKeyValueResults = result.map(word -> new PartIDCount(word._1, word._2)); 
		
		// here we get the top 10
		List<PartIDCount> finalResult=changedKeyValueResults.top(10);
		// no need to sort it
//		Collections.sort(finalResult);
		System.out.println(finalResult);


		// Stop the timer
		finalTimestamp = System.nanoTime();
		
		// Calculate elapsed times
		loadRDDTime = (countTimestamp - loadRDDTimestamp) / 1000000000.0;
		queryTimeIncludesCount = (finalTimestamp - loadRDDTimestamp) / 1000000000.0;
		queryTime = (finalTimestamp - countTimestamp) / 1000000000.0;
		elapsedTotalTime = (finalTimestamp - startTime) / 1000000000.0;
		
		// print out the final results
		System.out.println("Result Query 1 Top-10:\nDataset:"+fileScale+"\nNum Copies: "+NUMBER_OF_COPIES+"\nNum Part: "+numPartitions+"\nNum Cust: "+numberOfCustomers+"\nresult count: " +finalResult+"\nLoad RDD time: "+ String.format("%.9f", loadRDDTime)+"\nQuery time: "+ String.format("%.9f", queryTime)+"\nTotal time: "+"\nQuery time includes count: "+ String.format("%.9f", queryTimeIncludesCount)+"\nTotal time: "+ String.format("%.9f", elapsedTotalTime));
		//System.out.println("RDD-TOP10#"+fileScale+"#"+NUMBER_OF_COPIES+"#"+numPartitions+"#"+numberOfCustomers+"#" +finalResult.size()+"#"+ String.format("%.9f", elapsedTotalTime));

	}
}