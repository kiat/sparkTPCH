package edu.rice.exp.spark_exp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import edu.rice.generate_data.DataGenerator;

public class AggregatePartIDsFromCustomer_RDD {

	

	
	public static void main(String[] args) throws FileNotFoundException, IOException {
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

		SparkConf conf = new SparkConf();
		conf.setAppName("ComplexObjectManipulation_RDD");

		// Kryo Serialization
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kryoserializer.buffer", "2048m");
		conf.set("spark.kryo.registrationRequired", "true");
		conf.set("spark.kryo.registrator", MyKryoRegistrator.class.getName());
		
		conf.set("spark.io.compression.codec", "lzf"); // snappy, lzf, lz4 
//		conf.set("spark.speculation", "true"); 
//		conf.set("spark.local.dir", "/mnt/sparkdata");
		
		JavaSparkContext sc = new JavaSparkContext(conf);

		if (args.length > 1)
			fileScale = args[1];

		JavaRDD<Customer> customerRDD_raw = sc.parallelize(DataGenerator.generateData(fileScale), numPartitions);

		JavaRDD<Customer> customerRDD = customerRDD_raw;

		// Copy the same data multiple times to make it big data
		for (int i = 0; i < NUMBER_OF_COPIES; i++) {
			customerRDD = customerRDD.union(customerRDD_raw);
		}
		
		// Caching made the experiment slower 
//		System.out.println("Cache the data");
		customerRDD=customerRDD.coalesce(numPartitions);
		
//		customerRDD.persist(StorageLevel.MEMORY_ONLY_2());

//		customerRDD.persist(StorageLevel.MEMORY_AND_DISK());
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
		// ######### MAIN Experiment #############
		// #############################################
		// #############################################

		// Now is data loaded in RDD, ready for the experiment
		// Start the timer
		startTime = System.nanoTime();

		
		// flatMap to pair <partKey, <CustomerName, PartID>>
		JavaPairRDD<Integer, Tuple2<String, Integer>> soldPartIDs = customerRDD.flatMapToPair(
				new PairFlatMapFunction<Customer, Integer, Tuple2<String, Integer>>(){
			
				private static final long serialVersionUID = -1932241861741271488L;

			@Override
			public Iterator<Tuple2<Integer, Tuple2<String, Integer>>> call(Customer customer) throws Exception {
				List<Order> orders = customer.getOrders();
				List<Tuple2<Integer, Tuple2<String, Integer>>> returnList = new ArrayList<Tuple2<Integer, Tuple2<String, Integer>>>();
				
				for (Order order : orders) {
					List<LineItem> lineItems = order.getLineItems();
					for (LineItem lineItem : lineItems) {
						Tuple2<String, Integer> supplierPartID = new Tuple2<String, Integer>(customer.getName(), lineItem.getPart().getPartID());
						returnList.add(new Tuple2<Integer, Tuple2<String, Integer>>(lineItem.getSupplier().getSupplierKey(), supplierPartID));
					}
				}
				
				
				return returnList.iterator();
			}
		});
		
		

		

		// Now, we need to aggregate the results
		// aggregateByKey needs 3 parameters:
		// 1. zero initializations,
		// 2. a function to add data to a Map<String, List<Integer> object and
		// 3. a function to merge two Map<String, List<Integer> objects.
		JavaPairRDD<Integer, Map<String, List<Integer>>> result = soldPartIDs.aggregateByKey(new HashMap<String, List<Integer>>(),
				new Function2<Map<String, List<Integer>>, Tuple2<String, Integer>, Map<String, List<Integer>>>() {
	
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
//						// merge the two HashMaps inside the SupplierData
//						Iterator<String> it=suppData2.keySet().iterator();
//						
//						while (it.hasNext()) {
//							String key = it.next();
//							if (suppData1.containsKey(key)) {
//								List<Integer> tmpIDList=suppData1.get(key);
//								// get the List and aggregate PartID to the existing list
//								tmpIDList.addAll(suppData2.get(key));
//								suppData1.put(key, tmpIDList);
//							}
//						}
						
						
						suppData1.putAll(suppData2);

						return suppData1;
					}

				});

		

		// At the end we do not do a result.count() but we do a map and reduce task to count up the final results  
		Tuple2<Integer, Integer> finalResult= result.mapToPair(word -> new Tuple2<>(0, 1)).reduce(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>,Tuple2<Integer, Integer>>(){
			private static final long serialVersionUID = 2685491879841689408L;

			@Override
			public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> arg0, Tuple2<Integer, Integer> arg1) throws Exception {
				
				Tuple2<Integer, Integer> sum=new Tuple2<Integer, Integer>(arg0._1, arg0._2+arg1._2 ); 
				return sum ;
			}
			 
		 }); 
		
		int finalResultCount=finalResult._2;
		 
		// Stop the timer
		elapsedTotalTime += (System.nanoTime() - startTime) / 1000000000.0;

		// print out the final results
		System.out.println("Dataset#"+fileScale+"#"+NUMBER_OF_COPIES+"#"+numPartitions+"#"+numberOfCustomers+"#" +finalResultCount+"#"+ String.format("%.9f", elapsedTotalTime));

	}
}