package edu.rice.exp.spark_exp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import edu.rice.dmodel.Customer;
import edu.rice.dmodel.LineItem;
import edu.rice.dmodel.MyKryoRegistrator;
import edu.rice.dmodel.Order;


public class Experiment_Query_3 {
	

	
	public static void main(String[] args) throws FileNotFoundException, IOException {


// Simple Query 3 : How many customers do not include orders for products provided by suppliers from their own nation.
		
	String hdfsNameNodePath = "hdfs://10.134.96.100:9000/user/kia/customer-";
	
	long startTime = 0;

	double elapsedTotalTime = 0;
	
	// define the number of partitions
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
	conf.setAppName("ComplexObjectManipulation_RDD");

	// Kryo Serialization
	conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
	conf.set("spark.kryoserializer.buffer", "1024mb");
	conf.set("spark.kryo.registrationRequired", "true");
	conf.set("spark.kryo.registrator", MyKryoRegistrator.class.getName());
	
	conf.set("spark.io.compression.codec", "lzf"); // snappy, lzf, lz4 
//	conf.set("spark.speculation", "true"); 
//	conf.set("spark.local.dir", "/mnt/sparkdata");
	
	
	conf.set("spark.shuffle.spill", "true");
	
	JavaSparkContext sc = new JavaSparkContext(conf);
	
	
	conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
	conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
	
	conf.set("fs.local.block.size", "268435456");

	JavaRDD<Customer> customerRDD = sc.objectFile(hdfsNameNodePath + NUMBER_OF_COPIES); 
	
	
//	JavaRDD<Customer> customerRDD = sc.parallelize(DataGenerator.generateData(fileScale), numPartitions);
//
////	JavaRDD<Customer> customerRDD = customerRDD_raw;
//
//	// Copy the same data multiple times to make it big data
//	for (int i = 0; i < NUMBER_OF_COPIES; i++) {
//		customerRDD = customerRDD.union(customerRDD);
//	}
	
	// Caching made the experiment slower 
//	System.out.println("Cache the data");
	customerRDD=customerRDD.coalesce(numPartitions);
	
//	customerRDD.persist(StorageLevel.MEMORY_ONLY_2());

//	customerRDD.persist(StorageLevel.MEMORY_AND_DISK());
	customerRDD.persist(StorageLevel.MEMORY_ONLY_SER());
	
	System.out.println("Get the number of Customers");

	// force spark to do the job and load data into RDD
	long numberOfCustomers = customerRDD.count();
 	System.out.println("Number of Customer: " + numberOfCustomers);
 	
 	
 	// do something else to have the data in memory 		
 	long numberOfDistinctCustomers = customerRDD.distinct().count();
 	System.out.println("Number of Distinct Customer: " + numberOfDistinctCustomers);


 	
     	
     	
		// #############################################
		// #############################################
		// #########      MAIN Experiment  #############
		// #############################################
		// #############################################

		// Now is data loaded in RDD, ready for the experiment
		// Start the timer
		startTime = System.nanoTime();
		
		
		JavaPairRDD<Integer, Integer> customerNations = customerRDD.flatMapToPair(
				new PairFlatMapFunction<Customer, Integer, Integer>(){
			
				private static final long serialVersionUID = -1932241861741271488L;

			@Override
			public Iterator<Tuple2<Integer, Integer>> call(Customer customer) throws Exception {
				List<Order> orders = customer.getOrders();
				int cusotmerNation=customer.getNationkey();
				
				List<Tuple2<Integer, Integer>> returnList = new ArrayList<Tuple2<Integer, Integer>>();
				
				int include=1;
				order_loop: // annotate this loop so that we can break from it
				for (Order order : orders) {
					List<LineItem> lineItems = order.getLineItems();
					for (LineItem lineItem : lineItems) {

						if(cusotmerNation == lineItem.getSupplier().getNationKey()){
							include=0;

							// if there is one lineitem with the nation key this customer includes at least one product from its own nation. 
							 break order_loop;
						}
							
					}
				}
				
				Tuple2<Integer, Integer> returnValue=new Tuple2<Integer, Integer>(1, include); 
				returnList.add(returnValue); 
				
				
				return returnList.iterator();
			}
		});
		
		
		JavaPairRDD<Integer, Integer> result=customerNations.reduceByKey(new Function2 <Integer, Integer, Integer>(){

			private static final long serialVersionUID = -1924109337454612429L;
			@Override
			public Integer call(Integer a, Integer b) throws Exception {
				return a+b;
			}
			
		});
		
		
		List<Tuple2<Integer, Integer>> finalResultCount=result.collect();
//		System.out.println(m);

		
//		long  finalResultCount=customerNations.count();
			
		// Stop the timer
		elapsedTotalTime += (System.nanoTime() - startTime) / 1000000000.0;

		// print out the final results
		System.out.println("RDD#"+fileScale+"#"+NUMBER_OF_COPIES+"#"+numPartitions+"#"+numberOfCustomers+"#" +finalResultCount.get(0)._2+"#"+ String.format("%.9f", elapsedTotalTime));

	}
}