package edu.rice.exp.spark_exp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import edu.rice.dmodel.Customer;
import edu.rice.dmodel.LineItem;
import edu.rice.dmodel.MyKryoRegistrator;
import edu.rice.dmodel.Order;
import edu.rice.dmodel.Wrapper;
import edu.rice.generate_data.DataGenerator;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

public class TopJaccard {

	// the capacity of priority queue is 10
	public static PriorityQueue<Wrapper> topKQueue = new PriorityQueue<Wrapper>(10);

	public static List<Integer> myQuery = new ArrayList<Integer>();

	public static void main(String[] args) throws FileNotFoundException, IOException {
		int[] thisIsAnIntArray = { 371, 374, 780, 888, 937, 985, 1165, 1249, 1296, 1701, 1808, 2018, 2111 };

		for (int i : thisIsAnIntArray) {
			TopJaccard.myQuery.add(i);
		}

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

			@Override
			public Wrapper call(Customer customer) throws Exception {

				List<Order> orders = customer.getOrders();

				// A List to store partIDs for each Customer
				Set<Integer> partIDSet = new HashSet<Integer>();

				// iterates over all orders for a customer
				for (Order order : orders) {
					List<LineItem> lineItems = order.getLineItems();

					// iterates over the items in an order
					for (LineItem lineItem : lineItems) {
						partIDSet.add(lineItem.getPart().getPartID());
					}
				}

				// Sorting HashSet using List
				List<Integer> partIDSortedList = new ArrayList<Integer>(partIDSet);
				Collections.sort(partIDSortedList);

				return new Wrapper(customer.getCustkey(), partIDSortedList);
			}

		});
		//
		// List<Wrapper> took10 = myMappedData.take(10);
		//
		// for (Wrapper tuple2 : took10) {
		// System.out.println(tuple2);
		//
		// }

		myMappedData.foreach(new VoidFunction<Wrapper>() {

			private static final long serialVersionUID = 4710286206160441612L;

			@Override
			public void call(Wrapper myGuy) throws Exception {

				if (myGuy.getPartIDs() == null)
					return;

				if (myGuy.getPartIDs().size() == 0)
					return;

				List<Integer> customerListOfPartsIds = myGuy.getPartIDs();
				List<Integer> queryListOfPartsIds = TopJaccard.myQuery;

				// will store the common PartID's
				List<Integer> inCommon = new ArrayList<Integer>();

				// will store all PartID's (repeated counts only one)
				List<Integer> totalUniquePartsID = new ArrayList<Integer>();

				int countFirst = 0;
				int countSecond = 0;

				while (countFirst < queryListOfPartsIds.size() && countSecond < customerListOfPartsIds.size()) {

					if (queryListOfPartsIds.get(countFirst) > customerListOfPartsIds.get(countSecond)) {
						totalUniquePartsID.add(customerListOfPartsIds.get(countSecond));
						countSecond++;
					} else {
						if (queryListOfPartsIds.get(countFirst) == customerListOfPartsIds.get(countSecond)) {

							inCommon.add(queryListOfPartsIds.get(countFirst));
							countSecond++;
						}
						totalUniquePartsID.add(queryListOfPartsIds.get(countFirst));
						countFirst++;
					}
				}

				// now add the not in common entries for the largest list
				if (queryListOfPartsIds.size() > customerListOfPartsIds.size()) {

					for (int i = 0; i < queryListOfPartsIds.size(); i++)
						totalUniquePartsID.add(queryListOfPartsIds.get(i));

				} else {

					for (int i = 0; i < customerListOfPartsIds.size(); i++)
						totalUniquePartsID.add(customerListOfPartsIds.get(i));

				}

				Double similarityValue = new Double((double) (inCommon.size() / totalUniquePartsID.size()));

				// set the score for my guy
				myGuy.setScore(similarityValue);

				System.out.println(inCommon.size()  + "/ " + totalUniquePartsID.size() +" = "+similarityValue);

				// add my guy to the priorityQueue.
				TopJaccard.topKQueue.add(myGuy);

			}

		});

		for (Wrapper wrapper : TopJaccard.topKQueue) {
			System.out.println(wrapper);
		}

		//
		// int finalResultCount = finalResult._2;
		//
		// // Stop the timer
		// finalTimestamp = System.nanoTime();
		//
		// // Calculate elapsed times
		// // time to load data from hdfs into RDD
		// loadRDDTime = (startQueryTimestamp - startTime) / 1000000000.0;
		// // reads file from HDFS time
		// readsHDFSTime = (readFileTime - startTime) / 1000000000.0;
		// // query time including loading RDD into memory
		// countTime = (startQueryTimestamp - countTimestamp) / 1000000000.0;
		// // query time not including loading RDD into memory
		// queryTime = (finalTimestamp - startQueryTimestamp) / 1000000000.0;
		// // total elapsed time
		// elapsedTotalTime = (finalTimestamp - startTime) / 1000000000.0;
		//
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
		//
		// // Finally stop the Spark context once all is completed

		sc.stop();

	}
}
