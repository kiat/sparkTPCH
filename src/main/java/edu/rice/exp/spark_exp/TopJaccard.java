package edu.rice.exp.spark_exp;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import edu.rice.dmodel.Customer;
import edu.rice.dmodel.LineItem;
import edu.rice.dmodel.MyKryoRegistrator;
import edu.rice.dmodel.Order;
import edu.rice.dmodel.Wrapper;
import edu.rice.generate_data.DataGenerator;

public class TopJaccard {

	// the capacity of priority queue is 10
	public static int numUniqueInQuery = 0;

	public static void main(String[] args) throws FileNotFoundException, IOException {

		// this is our query
		Integer[] myQuery = {90, 342, 528, 678, 957, 1001, 1950, 2022, 2045, 2345, 3238, 4456, 5218, 5301, 5798, 6001, 6119, 6120, 6153, 6670, 6715, 6896, 7000,
				7109, 7400, 7542, 8000, 10024, 10030, 10316, 10400, 10534, 11000, 11635, 11700, 11884, 11900, 12413, 14511, 15000, 15594, 15700, 15760, 16000,
				16976, 17000, 17002, 17003, 17035, 18437, 19000, 20848, 21000, 22004, 22202, 22203, 22339, 22400, 23984, 24000, 24180, 25000, 26284, 27000,
				27182, 28000, 28268, 28500, 28530, 29000, 31060, 31500, 32388, 32400, 32428, 32774, 33000, 33023, 34000, 34055, 34300, 34385, 36745, 37000,
				37232, 37500, 37990, 38000, 3982};


		int m_index = 0;

		while (true) {
			if (m_index == myQuery.length)
				break;
			
			// loop to the last repeated value
			while (m_index + 1 < myQuery.length && myQuery[m_index].intValue() == myQuery[m_index + 1].intValue())
				m_index++;
		

			// saw another unique
			numUniqueInQuery++;
			m_index++;
		}
		

		System.out.println("numUniqueInQuery=" + numUniqueInQuery);

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
		
		// Default name of file with query data represented as
		// a comma separated text file, e.g. 222,543,22,56,23
		String inputQueryFile = "jaccardInput";	
		
		JavaRDD<Customer> customerRDD = null;	
		
		// can be overwritten by the 4rd command line arg
		// 0 = the query time doesn't include count nor count.distinct
		// (thus calculated time includes reading from HDFS)
		// 1 = the query includes count and count.distinct (default)
		int warmCache = 1;

		if (args.length > 0)
			NUMBER_OF_COPIES = Integer.parseInt(args[0]);
		
		long numberOfCustomers = 0;
		long numberOfDistinctCustomers = 0;

		SparkConf conf = new SparkConf();
		conf.setAppName("TopJaccard-" + NUMBER_OF_COPIES);

		// TODO Remove when it is run on Chluser
		// these are obly for running local
//		PropertyConfigurator.configure("log4j.properties");
//		conf.setMaster("local[*]");

		// Kryo Serialization
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kryo.registrationRequired", "true");
		conf.set("spark.kryo.registrator", MyKryoRegistrator.class.getName());

		conf.set("spark.io.compression.codec", "lzf"); // snappy, lzf, lz4
		// conf.set("spark.speculation", "true");

		conf.set("spark.shuffle.spill", "true");

		JavaSparkContext sc = new JavaSparkContext(conf);		

		if (args.length > 1)
			fileScale = args[1];

		if (args.length > 2)
			numPartitions = Integer.parseInt(args[2]);

		// if third arg is provided use that and read from hdfs
		if (args.length > 3){
			hdfsNameNodePath = args[3];
			customerRDD = sc.objectFile(hdfsNameNodePath + NUMBER_OF_COPIES);
			
		} else {
			// otherwise, generate from files
			customerRDD = sc.parallelize(DataGenerator.generateData(fileScale), numPartitions);			
		}

		if (args.length > 4)
			warmCache = Integer.parseInt(args[4]);
		
		// if a 6th arg is provided it contains the name of 
		// a comma separated file with the part Id's to be
		// used by the query.
		// This assumes well-formed numbers!!!!
		// If this arg is not provided, it uses a default
		// hard-coded list stored in the variable myQuery
		if (args.length > 5) {		
		    inputQueryFile = args[5];

			String[] listOfParts = null;
			
			try (BufferedReader br = new BufferedReader(new FileReader(inputQueryFile))) {
				String line;
				int numItems = 0;
				while ((line = br.readLine()) != null) {
					listOfParts = line.split(",");
					for(int i=numItems;i < listOfParts.length;i++) {
						numItems++;
						myQuery[numItems] = new Integer(listOfParts[numItems]);
					}
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}			
		}

		// Print application Id so it can be used via REST API to analyze processing
		// times
		System.out.println("Application Id: " + sc.sc().applicationId());

		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		conf.set("fs.local.block.size", "268435456");

		// Get the initial time
		startTime = System.nanoTime();

		readFileTime = System.nanoTime();

		// if (warmCache == 1) {
		//
		// // customerRDD=customerRDD.coalesce(numPartitions);
		// customerRDD.persist(StorageLevel.MEMORY_ONLY_SER());
		//
		// System.out.println("Get the number of Customers");
		//
		// // force spark to do the job and load data into RDD
		// numberOfCustomers = customerRDD.count();
		//
		// countTimestamp = System.nanoTime();
		//
		// System.out.println("Number of Customer: " + numberOfCustomers);
		//
		// // do something else to have the data in memory
		// numberOfDistinctCustomers = customerRDD.distinct().count();
		// System.out.println("Number of Distinct Customer: " + numberOfDistinctCustomers);
		//
		// }

		// #############################################
		// #############################################
		// ######### MAIN Experiment #############
		// #############################################
		// #############################################

		// Now is data loaded in RDD, ready for the experiment
		// Start the timer
		startQueryTimestamp = System.nanoTime();

		// We are doing one big map of each Customer object to a Wrapper Object that contains the results of Jaccard Similarity of the PartID set ordered by
		// Customer to a specific Query( an order list of partIDs )
		JavaRDD<Wrapper> myMappedData = customerRDD.map(new Function<Customer, Wrapper>() {

			private static final long serialVersionUID = -7744382435853610996L;

			@Override
			public Wrapper call(Customer m_Customer) throws Exception {
				// Process the Customer, implementation extracted from here to be able to do unit testing.
				return processCustomer(m_Customer, myQuery);
			}
		});

		// Then we get the top 10 Results
		List<Wrapper> results = myMappedData.top(10);

		// We print out the results
		for (Wrapper wrapper : results) {
			System.out.println(wrapper);
		}

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

		// // print out the final results
		// if (warmCache == 1)
		// System.out.println("Result Query 1:\nDataset Factor: " + NUMBER_OF_COPIES + "\nNum Part: " + numPartitions + "\nNum Cust: " + numberOfCustomers
		// + "\nResult count: " + finalResultCount + "\nReads HDFS time: " + readsHDFSTime + "\nLoad RDD time: " + String.format("%.9f", loadRDDTime)
		// + "\nTime to count: " + String.format("%.9f", countTime) + "\nQuery time: " + String.format("%.9f", queryTime) + "\nTotal time: "
		// + String.format("%.9f", elapsedTotalTime) + "\n");
		// else
		// System.out.println("Result Query 1:\nDataset Factor: " + NUMBER_OF_COPIES + "\nNum Part: " + numPartitions + "\nNum Cust: " + numberOfCustomers
		// + "\nResult count: " + finalResultCount + "\nReads HDFS time: " + readsHDFSTime + "\nLoad RDD time: " + String.format("%.9f", loadRDDTime)
		// + "\nQuery time: " + String.format("%.9f", queryTime) + "\nTotal time: " + String.format("%.9f", elapsedTotalTime) + "\n");

		// Finally stop the Spark context once all is completed

		sc.stop();

	}

	public static Wrapper processCustomer(Customer m_Customer, Integer[] origList) {

		List<Order> orders = m_Customer.getOrders();

		// we do nothing if
		if (orders.size() == 0 || orders == null) {
			return new Wrapper(m_Customer.getCustkey(), null, 0);
		}

		// We collect the list of all partIDs ordered by the Customer.
		// Sorting HashSet using List
		// List<Integer> allLines = new ArrayList<Integer>(orders.size());

		// new, we figure out how many parts there are in this customer object
		int totParts = 0;

		for (Order order : orders) {
			totParts += order.getLineItems().size();
		}

		Integer[] allLines = new Integer[totParts];

		int m_index=0;
		// iterates over all orders for a customer
		for (Order order : orders) {
			List<LineItem> lineItems = order.getLineItems();

			// iterates over the items in an order
			for (LineItem lineItem : lineItems) {
				allLines[m_index] =new Integer(lineItem.getPart().getPartID());
				m_index++;
			}
		}

		// ######################
		// ### QUERY Processing
		// ######################
		// now we run the query on top of that

		// sort the list
//		Arrays.sort(allLines, Collections.reverseOrder());
		Arrays.sort(allLines);

		
		// will store the common PartID's
		List<Integer> inCommon = new ArrayList<Integer>();
		int posInOrig = 0;
		int posInThis = 0;

		while (true) {

			// if we got to the end of either, break
			if (posInThis == allLines.length || posInOrig == origList.length)
				break;

			// first, loop to the last repeated value
			while (posInThis + 1 < allLines.length && allLines[posInThis].intValue() == allLines[posInThis+1].intValue())
				posInThis++;
			

			// next, see if the two are the same
			if (allLines[posInThis].intValue() == origList[posInOrig].intValue()) {

				inCommon.add(allLines[posInThis]);
				
				posInThis++;
				posInOrig++;
				
				// otherwise, advance the smaller one
			} else if (allLines[posInThis].intValue() < origList[posInOrig].intValue()) {
				posInThis++;
				
			} else {
				
				posInOrig++;
			}
		}

		// and get the number of unique items in the list of parts
		int numUnique = 0;
		posInThis = 0;
		
		while (true) {

			if (posInThis == allLines.length)
				break;

			// loop to the last repeated value
			while (posInThis + 1 < allLines.length && allLines[posInThis].intValue() == allLines[posInThis + 1].intValue())
				posInThis++;

			// saw another unique
			numUnique++;
			posInThis++;
		}

		double similarityValue = ((double) inCommon.size()) / (double) (numUnique + numUniqueInQuery - inCommon.size());

		// make a new wrapper object and return
		return new Wrapper(m_Customer.getCustkey(), inCommon, similarityValue);
	}

}