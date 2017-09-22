package edu.rice.exp.spark_exp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
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

/**
 * This is a class for computing a Top-K Similarity Query using
 * JaccardSimilarity between a Query Object and entries in the 
 * dataset.
 * 
 * The objects to compare are as follows: For each Customer 
 * obtain a list of Orders and within each Order, obtain a 
 * list of unique Part ID's (i.e. duplicates are included only
 * once).
 * 
 * For each of these objects we compute Jaccard Similarity,
 * which is calculated as the number of parts that are in
 * both sets (intersection), divided by the total number of 
 * parts in both sets (duplicated counted once).
 *  
 * First, for each customer we obtain a list of unique parts 
 * ordered for all orders.
 * 
 * Second: 
 *
 */

public class JaccardSimilarityQuery {

	public static void main(String[] args) throws FileNotFoundException, IOException {

		// can be overwritten by the fourth command line arg
		String hdfsNameNodePath = "hdfs://10.134.96.100:9000/user/kia/customer-";

		long startTime = 0; // timestamp from the beginning
		long readFileTime = 0; // timestamp after reading from HDFS
		long countTimestamp = 0; // timestamp after count that reads
									// from disk into RDD
		long startQueryTimestamp = 0; // timestamp before query begins
		long finalTimestamp = 0; // timestamp final

		double readsHDFSTime = 0; // time to read from HDFS (not including count + count.distinct)
		double loadRDDTime = 0; // time to load RDD in memory (includes count + count.distinct)
		double countTime = 0; // time to count (includes only count)
		double queryTime = 0; // time to run the query (doesn't include data load)
		double elapsedTotalTime = 0; // total elapsed time

		// define the number of partitions
		// can be overwritten by the 3rd command line arg
		int numPartitions = 8;

		int NUMBER_OF_COPIES = 0;// number of Customers multiply X 2^REPLICATION_FACTOR

		// TODO this is not used and should be removed
		String fileScale = "0.2";

		// can be overwritten by the 4rd command line arg
		// 0 = the query time doesn't include count nor count.distinct
		// (thus calculated time includes reading from HDFS)
		// 1 = the query includes count and count.distinct (default)
		int warmCache = 1;

		if (args.length > 0)
			NUMBER_OF_COPIES = Integer.parseInt(args[0]);

		if (args.length > 1)
			fileScale = args[1];

		if (args.length > 2)
			numPartitions = Integer.parseInt(args[2]);

		if (args.length > 3)
			hdfsNameNodePath = args[3];

		if (args.length > 4)
			warmCache = Integer.parseInt(args[4]);

		long numberOfCustomers = 0;
		long numberOfDistinctCustomers = 0;

		SparkConf conf = new SparkConf();
		conf.setAppName("ComplexObjectManipulation_RDD " + NUMBER_OF_COPIES);
				
		// Kryo Serialization
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kryo.registrationRequired", "true");
		conf.set("spark.kryo.registrator", MyKryoRegistrator.class.getName());

		conf.set("spark.io.compression.codec", "lzf"); // snappy, lzf, lz4
		// conf.set("spark.speculation", "true");

		conf.set("spark.shuffle.spill", "true");

		JavaSparkContext sc = new JavaSparkContext(conf);

		// Print application Id so it can be used via REST API to analyze processing
		// times
		System.out.println("Application Id: " + sc.sc().applicationId());

		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		conf.set("fs.local.block.size", "268435456");
		
		// Creates a List<Integer> with the PartID's I want to use for 
		// computing JaccardSimilarity TODO: read from arg
		List<Integer> queryListOfPartsIds = 
		    new ArrayList<Integer>();		
		
		// Get the initial time
		startTime = System.nanoTime();

		JavaRDD<Customer> customerRDD = sc.objectFile(hdfsNameNodePath + NUMBER_OF_COPIES);
		
		readFileTime = System.nanoTime();

		if (warmCache == 1) {

			// customerRDD=customerRDD.coalesce(numPartitions);
			customerRDD.persist(StorageLevel.MEMORY_ONLY_SER());

			System.out.println("Get the number of Customers");

			// force spark to do the job and load data into RDD
			numberOfCustomers = customerRDD.count();

			countTimestamp = System.nanoTime();

			System.out.println("Number of Customer: " + numberOfCustomers);

			// do something else to have the data in memory
			numberOfDistinctCustomers = customerRDD.distinct().count();
			System.out.println("Number of Distinct Customer: " + numberOfDistinctCustomers);

		}

		// #############################################
		// #############################################
		// #########     MAIN Experiment   #############
		// #############################################
		// #############################################

		// Now is data loaded in RDD, ready for the experiment
		// Start the timer
		startQueryTimestamp = System.nanoTime();
		
		// flatMap to pair <Customer.key, List<PartID>>
		// returns pairs with the customerKey and a list with all partsId for each
		// customer
		JavaPairRDD<Integer, List<Integer>> allPartsIDsPerCustomer = 
			customerRDD.flatMapToPair(new PairFlatMapFunction<Customer, 			// Input Object: A Customer
															  Integer,				// Customer key
															  List<Integer>>() {	// Value returned: A List of all parts Id's
																					// from all orders for each customer
	
				private static final long serialVersionUID = -1932241861741271488L;
	
				@Override
				public Iterator<Tuple2<Integer, List<Integer>>> call(Customer customer) throws Exception {
					List<Order> orders = customer.getOrders();

					// List for storing all partID's for this Customer
					List<Integer> listOfPartsIds = 
					    new ArrayList<Integer>();
					
					// returns a Tuple<Customer, List<Integer>>
					// where the List contains the ID's of all parts
					List<Tuple2<Integer, List<Integer>>> returnTuple = 
						    new ArrayList<Tuple2<Integer, List<Integer>>>();
	
					// iterates over all orders for a customer
					for (Order order : orders) {
						
						List<LineItem> lineItems = order.getLineItems();
						Integer partKey = new Integer(0);
						
						//iterates over the items in an order
						for (LineItem lineItem : lineItems) {
							partKey = lineItem.getPart().getPartID();
							
							// now adds the partID only if is not already on the list
							// TODO see if there's a more efficient way of doing this
							if (listOfPartsIds.contains(partKey) == false)
								listOfPartsIds.add(partKey);
						}
						// sorts partId's
						Collections.sort(listOfPartsIds, (a, b) -> b.compareTo(a));
						// adds the parts for this Order
						returnTuple.add(new Tuple2<Integer, List<Integer>>(new Integer(customer.getCustkey()), listOfPartsIds));
					}
					
					return returnTuple.iterator();
				}
			});
		
		
		// Now, let's compute Jaccard Similarity

		// returns the customerKey and a tuple <SimilarityScore, and the list of PartID's>
		// for each customer
		
//		flatMapToPair(PairFlatMapFunction<T,K2,V2> f) 
		JavaPairRDD<Integer, Tuple2<Double, List<Integer>>> jaccardSimilarityScore = 
				allPartsIDsPerCustomer.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, List<Integer>>,
													Integer,
													Tuple2<Double, List<Integer>>>() {	// Value returned: A List of all parts Id's
												// from all orders for each customer
		
					private static final long serialVersionUID = -1932241861741271488L;
		
					@Override
					public Iterator<Tuple2<Integer, Tuple2<Double, List<Integer>>>> call(Tuple2<Integer, List<Integer>> value) throws Exception {

						
						// PartID's for this customer TODO: get the values
						List<Integer> customerListOfPartsIds = 
						    new ArrayList<Integer>();
																		
						// sort both lists to speed up lookups
						// TODO: this still can be optimized further
						Collections.sort(customerListOfPartsIds);						
						Collections.sort(queryListOfPartsIds);
						
						// will store the common PartID's
						List<Integer> inCommon = 
							    new ArrayList<Integer>();

						// will store all PartID's (repeated counts only one)
						List<Integer> totalUniquePartsID = 
							    new ArrayList<Integer>();	
						
						Collections.sort(queryListOfPartsIds);						
						Collections.sort(customerListOfPartsIds);
						
						int countFirst = 0;
						int countSecond = 0;
												
						while(countFirst < queryListOfPartsIds.size() && countSecond < customerListOfPartsIds.size()){
							
							// if the first entry is larger than the second
							if (queryListOfPartsIds.get(countFirst) > customerListOfPartsIds.get(countSecond)){
								
								totalUniquePartsID.add(customerListOfPartsIds.get(countSecond));
								countSecond++;
								
							} else {
								if (queryListOfPartsIds.get(countFirst) == customerListOfPartsIds.get(countSecond)){
									
									inCommon.add(queryListOfPartsIds.get(countFirst));
									totalUniquePartsID.add(queryListOfPartsIds.get(countFirst));
									
									countSecond++;
									countFirst++;
									
								} else {
									
									totalUniquePartsID.add(queryListOfPartsIds.get(countFirst));
									countFirst++;	
									
								}
								
							}
							
						}		
						
						// now add the not in common entries for the largest list
						if (queryListOfPartsIds.size() > customerListOfPartsIds.size()){
							
						    for (int i=countFirst ; i< queryListOfPartsIds.size(); i++)
						    	totalUniquePartsID.add(queryListOfPartsIds.get(i));
						    
						} else{
							
						    for (int i=countSecond ; i< customerListOfPartsIds.size(); i++)
						    	totalUniquePartsID.add(customerListOfPartsIds.get(i));
						    
						}					
						
						Double similarityValue = new Double((double)(inCommon.size() / totalUniquePartsID.size()));
						
						// adds the similarity along with part ID's purchased by this Customer
						Tuple2<Double, List<Integer>> innerTuple = 
								new Tuple2<Double, List<Integer>>(similarityValue, customerListOfPartsIds);
						// adds the Customer.key
						Tuple2<Integer, Tuple2<Double, List<Integer>>> outerTuple = 
								new Tuple2<Integer, Tuple2<Double, List<Integer>>>(new Integer(0), innerTuple);
						
						return(outerTuple);
						
					}
				});
		
		// Sort by key, where the key is the Similarity value
		// Take the top-10 Review this b/c it sorts each
		// partition and we have to make sure it gets a global 
		// one.
		jaccardSimilarityScore.sortByKey().take(10);		
		
		int finalResultCount=0;
		
		// Stop the timer
		finalTimestamp = System.nanoTime();

		// Calculate elapsed times
		// time to load data from hdfs into RDD
		loadRDDTime = (startQueryTimestamp - startTime) / 1000000000.0;
		// reads file from HDFS time
		readsHDFSTime = (readFileTime - startTime) / 1000000000.0;
		// query time including loading RDD into memory
		countTime = (startQueryTimestamp - countTimestamp) / 1000000000.0;
		// query time not including loading RDD into memory
		queryTime = (finalTimestamp - startQueryTimestamp) / 1000000000.0;
		// total elapsed time
		elapsedTotalTime = (finalTimestamp - startTime) / 1000000000.0;

		// print out the final results
		if (warmCache == 1)
			System.out.println("Result Query 1:\nDataset Factor: " + NUMBER_OF_COPIES + "\nNum Part: " + numPartitions + "\nNum Cust: " + numberOfCustomers
					+ "\nResult count: " + finalResultCount + "\nReads HDFS time: " + readsHDFSTime + "\nLoad RDD time: " + String.format("%.9f", loadRDDTime)
					+ "\nTime to count: " + String.format("%.9f", countTime) + "\nQuery time: " + String.format("%.9f", queryTime) + "\nTotal time: "
					+ String.format("%.9f", elapsedTotalTime) + "\n");
		else
			System.out.println("Result Query 1:\nDataset Factor: " + NUMBER_OF_COPIES + "\nNum Part: " + numPartitions + "\nNum Cust: " + numberOfCustomers
					+ "\nResult count: " + finalResultCount + "\nReads HDFS time: " + readsHDFSTime + "\nLoad RDD time: " + String.format("%.9f", loadRDDTime)
					+ "\nQuery time: " + String.format("%.9f", queryTime) + "\nTotal time: " + String.format("%.9f", elapsedTotalTime) + "\n");

		// Finally stop the Spark context once all is completed
		sc.stop();

	}
}

