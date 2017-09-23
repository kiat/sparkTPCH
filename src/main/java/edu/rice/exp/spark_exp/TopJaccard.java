package edu.rice.exp.spark_exp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

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
	public static PriorityQueue<Wrapper> topKQueue;

	public static List<Integer> myQuery;
	public static int numUniqueInQuery = 0;

	public static void main(String[] args) throws FileNotFoundException, IOException {
		int[] thisIsAnIntArray = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 20, 24, 27 };

		myQuery = new ArrayList<Integer>();
		topKQueue = new PriorityQueue<Wrapper>(10);

		for (int i : thisIsAnIntArray) {
			TopJaccard.myQuery.add(i);
		}

		System.out.println(myQuery);

		int m_index = 0;
		while (true) {
			if (m_index == TopJaccard.myQuery.size())
				break;
			// loop to the last repeated value
			while (m_index + 1 < myQuery.size() && myQuery.get(m_index) == myQuery.get(m_index + 1)) {
				m_index++;
			}

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
		String fileScale = "0.1";

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

		// TODO Remove when it is run on Chluser
		// these are obly for running local
		PropertyConfigurator.configure("log4j.properties");
		conf.setMaster("local[*]");

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

		// JavaRDD<Customer> customerRDD = sc.objectFile(hdfsNameNodePath + NUMBER_OF_COPIES);
		JavaRDD<Customer> customerRDD = sc.parallelize(DataGenerator.generateData(fileScale), numPartitions);
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

		JavaRDD<Wrapper> myMappedData = customerRDD.map(new Function<Customer, Wrapper>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -7744382435853610996L;

			@Override
			public Wrapper call(Customer customer) throws Exception {

				List<Order> orders = customer.getOrders();

				// we do nothing if
				if (orders.size() == 0 || orders == null) {
					return new Wrapper(customer.getCustkey(), null, 0);
				}

				// Sorting HashSet using List
				List<Integer> partIDSortedList = new ArrayList<Integer>(orders.size());

				// iterates over all orders for a customer
				for (Order order : orders) {
					List<LineItem> lineItems = order.getLineItems();

					// iterates over the items in an order
					for (LineItem lineItem : lineItems) {
						partIDSortedList.add(lineItem.getPart().getPartID());
					}
				}

				// sort the list
				Collections.sort(partIDSortedList);

				// ######################
				// ### QUERY Processing
				// ######################

				// now we run the query on top of that

				List<Integer> allLines = partIDSortedList;
				List<Integer> origList = myQuery;

				// will store the common PartID's
				List<Integer> inCommon = new ArrayList<Integer>();
				int posInOrig = 0;
				int posInThis = 0;

				while (true) {

					// if we got to the end of either, break
					if (posInThis == allLines.size() || posInOrig == origList.size())
						break;

					// first, loop to the last repeated value
					while (posInThis + 1 < allLines.size() && allLines.get(posInThis) == allLines.get(posInThis + 1)) {
						posInThis++;
					}

					// next, see if the two are the same
					if (allLines.get(posInThis) == origList.get(posInOrig)) {
						inCommon.add(allLines.get(posInThis));
						posInThis++;
						posInOrig++;

						// otherwise, advance the smaller one
					} else if (allLines.get(posInThis) < origList.get(posInOrig)) {
						posInThis++;
					} else {
						posInOrig++;
					}
				}

				// and get the number of unique items in the list of parts
				int numUnique = 0;
				posInThis = 0;
				while (true) {

					if (posInThis == allLines.size())
						break;

					// loop to the last repeated value
					while (posInThis + 1 < allLines.size() && allLines.get(posInThis) == allLines.get(posInThis + 1))
						posInThis++;

					// saw another unique
					numUnique++;
					posInThis++;

				}

				double similarityValue = ((double) inCommon.size()) / (double) (numUnique + numUniqueInQuery - inCommon.size());

				// make a new wrapper object and return
				return new Wrapper(customer.getCustkey(), partIDSortedList, similarityValue);
			}

		});

		List<Wrapper> results = myMappedData.top(10);

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

//		// print out the final results
//		if (warmCache == 1)
//			System.out.println("Result Query 1:\nDataset Factor: " + NUMBER_OF_COPIES + "\nNum Part: " + numPartitions + "\nNum Cust: " + numberOfCustomers
//					+ "\nResult count: " + finalResultCount + "\nReads HDFS time: " + readsHDFSTime + "\nLoad RDD time: " + String.format("%.9f", loadRDDTime)
//					+ "\nTime to count: " + String.format("%.9f", countTime) + "\nQuery time: " + String.format("%.9f", queryTime) + "\nTotal time: "
//					+ String.format("%.9f", elapsedTotalTime) + "\n");
//		else
//			System.out.println("Result Query 1:\nDataset Factor: " + NUMBER_OF_COPIES + "\nNum Part: " + numPartitions + "\nNum Cust: " + numberOfCustomers
//					+ "\nResult count: " + finalResultCount + "\nReads HDFS time: " + readsHDFSTime + "\nLoad RDD time: " + String.format("%.9f", loadRDDTime)
//					+ "\nQuery time: " + String.format("%.9f", queryTime) + "\nTotal time: " + String.format("%.9f", elapsedTotalTime) + "\n");

		// Finally stop the Spark context once all is completed

		sc.stop();

	}
}