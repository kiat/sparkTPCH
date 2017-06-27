package edu.rice.exp.spark_exp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;
import edu.rice.dmodel.Customer;
import edu.rice.dmodel.LineItem;
import edu.rice.dmodel.Order;
import edu.rice.generate_data.DataGenerator;

public class AggregatePartIDsFromCustomer_RDD {

	private static JavaSparkContext sc;

	public static void main(String[] args) throws FileNotFoundException, IOException {
		long startTime = 0;
		double elapsedTotalTime = 0;
		int NUMBER_OF_COPIES = 0;
		String fileScale = "0.1";

		if (args.length > 0)
			NUMBER_OF_COPIES = Integer.parseInt(args[0]);

		if (args.length > 1)
			fileScale = args[1];

//		PropertyConfigurator.configure("log4j.properties");

		SparkConf conf = new SparkConf();
		
//		conf.set("spark.executor.memory", "15g");
//		conf.set("spark.cores.max", "8");
		
		
//		conf.setMaster("local[*]");
		conf.setAppName("ComplexObjectManipulation_RDD");

		// Kryo Serialization
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kryoserializer.buffer.mb", "64");
		conf.set("spark.kryo.registrationRequired", "true");
		conf.set("spark.kryo.registrator", "edu.rice.dmodel.MyKryoRegistrator");

		sc = new JavaSparkContext(conf);

		if (args.length > 1)
			fileScale = args[1];

		JavaRDD<Customer> customerRDD = sc.parallelize(DataGenerator.generateData(fileScale));
		
		
//		customerRDD.saveAsObjectFile("file");
//		JavaRDD<Customer> customerRDD = sc.objectFile(""); 
		

		// Copy the same data multiple times to make it big data
		for (int i = 0; i < NUMBER_OF_COPIES; i++) {
			customerRDD = customerRDD.union(customerRDD);
		}

		System.out.println("Cache the data");

		customerRDD.cache();
		// customerRDD.persist(StorageLevel.MEMORY_ONLY());

		System.out.println("Get the number of Customers");
		// force spark to do the job and load data into RDD

		long numberOfCustomers = customerRDD.count();
		System.out.println("Number of Customer: " + numberOfCustomers);

		// try to sleep for 5 seconds to be sure that all other tasks are done
		for (int i = 0; i < 5; i++) {
			try {
				Thread.sleep(1000);
				System.out.println("Sleep for 1 sec ... ");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		System.out.println("Data is ready to use. ");

		// #############################################
		// #############################################
		// ######### MAIN Experiment #############
		// #############################################
		// #############################################

		// Now is data loaded in RDD, ready for the experiment
		// Start the timer
		startTime = System.nanoTime();

		JavaRDD<Tuple2<String, Tuple2<String, Integer>>> soldLineItems2 = customerRDD.flatMap(new FlatMapFunction<Customer, Tuple2<String, Tuple2<String, Integer>>>() {

			private static final long serialVersionUID = -7539917700784174380L;

			@Override
			public Iterator<Tuple2<String, Tuple2<String, Integer>>> call(Customer customer) throws Exception {
				List<Order> orders = customer.getOrders();
				List<Tuple2<String, Tuple2<String, Integer>>> returnList = new ArrayList<Tuple2<String, Tuple2<String, Integer>>>();
				for (Order order : orders) {
					List<LineItem> lineItems = order.getLineItems();
					for (LineItem lineItem : lineItems) {
						Tuple2<String, Integer> supplierPartID = new Tuple2<String, Integer>(customer.getName(), lineItem.getPart().getPartID());
						returnList.add(new Tuple2<String, Tuple2<String, Integer>>(lineItem.getSupplier().getName(), supplierPartID));
					}
				}
				return returnList.iterator();
			}
		});

		JavaPairRDD<String, Tuple2<String, Integer>> soldPartIDs = soldLineItems2.mapToPair(w -> new Tuple2<String, Tuple2<String, Integer>>(w._1, w._2));

		// Now, we need to aggregate the results
		// aggregateByKey needs 3 parameters:
		// 1. zero initializations,
		// 2. a function to add data to a Map<String, List<Integer> object and
		// 3. a function to merge two Map<String, List<Integer> objects.
		JavaPairRDD<String, Map<String, List<Integer>>> result = soldPartIDs.aggregateByKey(new HashMap<String, List<Integer>>(),
				new Function2<Map<String, List<Integer>>, Tuple2<String, Integer>, Map<String, List<Integer>>>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = -1688402472496211511L;

					@Override
					public Map<String, List<Integer>> call(Map<String, List<Integer>> suppData, Tuple2<String, Integer> tuple) throws Exception {

						List<Integer> intList = new ArrayList<Integer>();
						intList.add(tuple._2);
						suppData.put((String) tuple._1, intList);

						return suppData;
					}

				}, new Function2<Map<String, List<Integer>>, Map<String, List<Integer>>, Map<String, List<Integer>>>() {
					private static final long serialVersionUID = -1503342516335901464L;

					@Override
					public Map<String, List<Integer>> call(Map<String, List<Integer>> suppData1, Map<String, List<Integer>> suppData2) throws Exception {
						// merge the two HashMaps inside the SupplierData
						Iterator<String> it=suppData2.keySet().iterator();
						
						while (it.hasNext()) {
							String key = it.next();
							if (suppData1.containsKey(key)) {
								List<Integer> tmpIDList=suppData1.get(key);
								// get the List and aggregate PartID to the existing list
								tmpIDList.addAll(suppData2.get(key));
								suppData1.put(key, tmpIDList);
							}
						}
						return suppData1;
					}

				});


		System.out.println("Final Result Count:" + result.count());

		// Stop the timer
		elapsedTotalTime += (System.nanoTime() - startTime) / 1000000000.0;

		System.out.println(numberOfCustomers + "#" + String.format("%.9f", elapsedTotalTime));
	}
}