package edu.rice.exp.spark_exp;

import java.io.BufferedReader;
import java.util.Comparator;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
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
import edu.rice.dmodel.Part;
import edu.rice.dmodel.Wrapper;

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
 * ordered for all orders, returning a pair of:
 * 
 * Tuple2< customer.key, List<partId's> >
 * 
 * Second, for each customer we calculate the Jaccard Similarity
 * against the list of parts from the query, returning a pair of:
 * 
 * Tuple2< similarity.value, List<partId's>>, similarity.value
 * closer to 1 the  lists are more similar, closer to 0, the lists
 * are less similar.
 * 
 * Third, we create a priority queue sorting by similarity.value
 * and return the top 10 of the list.
 *
 */

public class JaccardSimilarityQuery implements Serializable {
	
	private static final long serialVersionUID = 3045541819871271488L;	

	public static void main(String[] args) throws FileNotFoundException, IOException {

		// can be overwritten by the fourth command line arg
		String hdfsNameNodePath = "hdfs://10.134.96.100:9000/user/kia/customer-";

		long startTime = 0; 			// timestamp from the beginning
		long readFileTime = 0; 			// timestamp after reading from HDFS
		long countTimestamp = 0; 		// timestamp after count that reads
										// from disk into RDD
		long startQueryTimestamp = 0; 	// timestamp before query begins
		long finalTimestamp = 0; 		// timestamp final

		double readsHDFSTime = 0; 		// time to read from HDFS (not including count + count.distinct)
		double loadRDDTime = 0; 		// time to load RDD in memory (includes count + count.distinct)
		double countTime = 0; 			// time to count (includes only count)
		double queryTime = 0; 			// time to run the query (doesn't include data load)
		double elapsedTotalTime = 0; 	// total elapsed time

		// what top K elements to include in query
		// can be overwritten by the 3rd command line arg
		int topKValue = 10;

		int NUMBER_OF_COPIES = 0;// number of Customers multiply X 2^REPLICATION_FACTOR

		// TODO this is not used and should be removed
		String fileScale = "0.2";
		
		// Default name of file with query data
		String inputQueryFile = "jaccardInput";

		// can be overwritten by the 4rd command line arg
		// 0 = the query time doesn't include count nor count.distinct
		// (thus calculated time includes reading from HDFS)
		// 1 = the query includes count and count.distinct (default)
		int warmCache = 1;

		if (args.length > 0)
			NUMBER_OF_COPIES = Integer.parseInt(args[0]);

		if (args.length > 1)		
		    inputQueryFile = args[1];

		String[] listOfParts = null;
		
		try (BufferedReader br = new BufferedReader(new FileReader(inputQueryFile))) {
			String line;
			while ((line = br.readLine()) != null) {
				listOfParts = line.split(",");
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}		

		// Creates a List<Integer> with the PartID's to be used for 
		// the query
		List<Integer> queryListOfPartsIds = 
		    new ArrayList<Integer>(listOfParts.length);		

		for (int i = 0; i < listOfParts.length; i++) {
			int tmp = Integer.parseInt(listOfParts[i]);
			queryListOfPartsIds.add(tmp);
		}
		
		if (args.length > 2)
			topKValue = Integer.parseInt(args[2]);

		if (args.length > 3)
			hdfsNameNodePath = args[3];

		if (args.length > 4)
			warmCache = Integer.parseInt(args[4]);

		long numberOfCustomers = 0;
		long numberOfDistinctCustomers = 0;

		SparkConf conf = new SparkConf();
		conf.setAppName("JaccardSimilarity- " + NUMBER_OF_COPIES);
				
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
						
		// Get the initial time
		startTime = System.nanoTime();

		JavaRDD<Customer> customerRDD = sc.objectFile(hdfsNameNodePath + NUMBER_OF_COPIES);

		customerRDD.persist(StorageLevel.MEMORY_ONLY_SER());
		
		readFileTime = System.nanoTime();

		if (warmCache == 1) {

			// customerRDD=customerRDD.coalesce(numPartitions);

			System.out.println("Get the number of Customers");

			// force spark to do the job and load data into RDD
			numberOfCustomers = customerRDD.count();

			countTimestamp = System.nanoTime();

			System.out.println("Number of Customer: " + numberOfCustomers);

			// do something else to have the data in memory
			numberOfDistinctCustomers = customerRDD.distinct().count();
			System.out.println("Number of Distinct Customer: " + numberOfDistinctCustomers);

		}
		
		System.out.println("The query is----> " + queryListOfPartsIds.toString());

		// #############################################
		// #############################################
		// #########     MAIN Experiment   #############
		// #############################################
		// #############################################

		// Now is data loaded in RDD, ready for the experiment
		// Start the timer
		startQueryTimestamp = System.nanoTime();

//		System.out.println("Number of Original Customers in RDD: " + customerRDD.count());
		
		// flatMap to pair <Customer.key, List<PartID>>
		// returns pairs with the customerKey and a list with all partsId for each
		// customer
		JavaPairRDD<Integer, List<Integer>> allPartsIDsPerCustomer = 
			customerRDD.flatMapToPair(new PairFlatMapFunction<Customer, 			// Type of Input Object: A Customer
															  Integer,				// Key: Customer
															  List<Integer>>() {	// Value: A List of all parts Id's
																					// from all orders for each customer
	
				private static final long serialVersionUID = 1932241819871271488L;
	
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
						System.out.println("Size of entries " + listOfPartsIds.size());
						// creates the tuple to be returned, with the Customer.key and a List<PartsId>
						returnTuple.add(new Tuple2<Integer, List<Integer>>(new Integer(customer.getCustkey()), listOfPartsIds));
						
					}
					
					return returnTuple.iterator();
				}
			});
		
//		System.out.println("Total customers after 1st map " + allPartsIDsPerCustomer.count());
		
		// Now, let's compute Jaccard Similarity
		// returns the SimilarityScore and a tuple <Similarity score, and the list of PartID's>
		// for each customer
		JavaPairRDD<Double, Tuple2<Integer, List<Integer>>> jaccardSimilarityScore = 
				allPartsIDsPerCustomer.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, List<Integer>>,	// Type of input RDD
													Double,													    // Key: Similarity value
													Tuple2<Integer, List<Integer>>>() {							// Value: Customer.key and List<PartsId>
		
			private static final long serialVersionUID = 2009241861741271488L;

			@Override
			public Iterator<Tuple2<Double, Tuple2<Integer, List<Integer>>>> call(Tuple2<Integer, List<Integer>> item) throws Exception {
				
				List<Integer> customerListOfPartsIds = item._2; // retrieves the list of parts for this customer
																
				// sort both lists to speed up lookups
				Collections.sort(customerListOfPartsIds);						
				Collections.sort(queryListOfPartsIds);
				
				// will store the common PartID's in this List
				List<Integer> inCommon = 
					    new ArrayList<Integer>();

				// will store all PartID's (repeated counts only one) in this List
				List<Integer> totalUniquePartsID = 
					    new ArrayList<Integer>();	
								
				int indexQueryList = 0;
				int indexCustoList = 0;
				System.out.println("In calc Size of entries " + customerListOfPartsIds.size());
				
                System.out.println("query list" + queryListOfPartsIds.toString());
                System.out.println("this list" + queryListOfPartsIds.toString());
                
				// iterates until the end of the shortest list is reached
				while(indexQueryList < queryListOfPartsIds.size() && 
					  indexCustoList < customerListOfPartsIds.size()){
					
					// if the value in the current entry in Query List is greater 
					// than the one in the Customer List, this is a unique partId
					if (queryListOfPartsIds.get(indexQueryList) > customerListOfPartsIds.get(indexCustoList)){
						
						totalUniquePartsID.add(customerListOfPartsIds.get(indexCustoList));
						// move index in Customer List to the next entry
						indexCustoList++;
						
					} else {
						// if both values in the current Query List and Customer List 
						// are equal, this is a common partId					
						if (queryListOfPartsIds.get(indexQueryList) == customerListOfPartsIds.get(indexCustoList)){
							
							inCommon.add(queryListOfPartsIds.get(indexQueryList));
							// but a common part is also unique, so put it in the 
							// corresponding list
							totalUniquePartsID.add(queryListOfPartsIds.get(indexQueryList));
							
							// move index in both Lists to the next entry
							indexCustoList++;
							indexQueryList++;
							
						} else {
							// if the value in the current Query List is less than the 
							// one in the Customer List, this is not a common part 
							totalUniquePartsID.add(queryListOfPartsIds.get(indexQueryList));
							// move index in Query List to the next entry
							indexQueryList++;	
							
						}					
					}				
				}		
				
				// we will iterate from the last index in the shortest List
				// until the end of the largest List, all entries are unique
				// partID's
				if (queryListOfPartsIds.size() > customerListOfPartsIds.size()){
					
				    for (int i=indexQueryList; i< queryListOfPartsIds.size(); i++)
				    	totalUniquePartsID.add(queryListOfPartsIds.get(i));
				    
				} else{
					
				    for (int i=indexCustoList; i< customerListOfPartsIds.size(); i++)
				    	totalUniquePartsID.add(customerListOfPartsIds.get(i));
				    
				}						
				
//				Double similarityValue = new Double(0.0);

				// Prevents divided by zero errors
//				if (totalUniquePartsID.size()!=0){
				//double calculate = ((double)inCommon.size() / (double)totalUniquePartsID.size());
				Double similarityValue = new Double(((double)inCommon.size() / (double)totalUniquePartsID.size()));
//				if (inCommon.size()>0) 
//					System.out.println("; query size: " +queryListOfPartsIds.size() + " | Common: "+ inCommon.size() + " | score " + similarityValue + " | " + similarityValue.toString());
//				}
				
				// adds the similarity along with part ID's purchased by this Customer
				Tuple2<Integer, List<Integer>> innerTuple = 
						new Tuple2<Integer, List<Integer>>(item._1, customerListOfPartsIds);
				// adds the Customer.key
				Tuple2<Double, Tuple2<Integer, List<Integer>>> outerTuple = 
						new Tuple2<Double, Tuple2<Integer, List<Integer>>>(similarityValue, innerTuple);
				
				List<Tuple2<Double, Tuple2<Integer, List<Integer>>>> returnTuple = 
					    new ArrayList<Tuple2<Double, Tuple2<Integer, List<Integer>>>>();
				
				returnTuple.add(outerTuple);
								
				return returnTuple.iterator();
				
			}
		});
		
//		System.out.println("Total after 2nd map " +jaccardSimilarityScore.count());
		
		// Comparator class for comparing similarity results, to be used in top and
		// prevent serialization error by implementing Serializable
		class TupleComparator implements Comparator<Tuple2<Double, Tuple2<Integer, List<Integer>>>>, Serializable {

			private static final long serialVersionUID = 1972211969496211511L;

			@Override
		    public int compare(Tuple2<Double, Tuple2<Integer,List<Integer>>> score_1, 
		    				   Tuple2<Double, Tuple2<Integer,List<Integer>>> score_2) {
		        return Double.compare(score_1._1, score_2._1);
		    }
		}	

		// Return the top 10 entries in the RDD, the key is the 
		// similarity score.
		
		List<Tuple2<Double, Tuple2<Integer, List<Integer>>>> topKResults = 
				jaccardSimilarityScore.top(topKValue, new TupleComparator ());
		
		System.out.println("Total after topK " + topKValue + " is: " +topKResults.size());
				
		// print the topK entries
		for (Tuple2<Double, Tuple2<Integer, List<Integer>>>  resultItem : topKResults) {
	        System.out.println("Customer key: "+ resultItem._2._1 + 
	        				   " Similarity Score: " + resultItem._1.toString() + 
	        				   "\nParts:" + resultItem._2._2);	
		}
				
	    
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
			System.out.println("Result Query 1:\nDataset Factor: " + NUMBER_OF_COPIES + "\ntopKValue: " + topKValue + "\nNum Cust: " + numberOfCustomers
					+ "\nResult count: " + finalResultCount + "\nReads HDFS time: " + readsHDFSTime + "\nLoad RDD time: " + String.format("%.9f", loadRDDTime)
					+ "\nTime to count: " + String.format("%.9f", countTime) + "\nQuery time: " + String.format("%.9f", queryTime) + "\nTotal time: "
					+ String.format("%.9f", elapsedTotalTime) + "\n");
		else
			System.out.println("Result Query 1:\nDataset Factor: " + NUMBER_OF_COPIES + "\ntopKValue: " + topKValue + "\nNum Cust: " + numberOfCustomers
					+ "\nResult count: " + finalResultCount + "\nReads HDFS time: " + readsHDFSTime + "\nLoad RDD time: " + String.format("%.9f", loadRDDTime)
					+ "\nQuery time: " + String.format("%.9f", queryTime) + "\nTotal time: " + String.format("%.9f", elapsedTotalTime) + "\n");

		// Finally stop the Spark context once all is completed
		sc.stop();

	}
	
}

