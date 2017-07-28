package edu.rice.exp.spark_exp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;



import edu.rice.dmodel.Customer;
import edu.rice.dmodel.LineItem;
import edu.rice.dmodel.SupplierData;
import edu.rice.dmodel.MyKryoRegistrator;
import edu.rice.dmodel.Order;
import edu.rice.generate_data.DataGenerator;



public class AggregatePartIDsFromCustomer_Dataset {


	public static void main(String[] args) throws FileNotFoundException, IOException {
		long startTime = 0;
		double elapsedTotalTime = 1;
		String fileScale = "0.2";

		int NUMBER_OF_COPIES=4;
		
		// define the number of partitions
		int numPartitions=4;
		
		if (args.length > 0)
			NUMBER_OF_COPIES = Integer.parseInt(args[0]);

		if (args.length > 1)
			fileScale = args[1];
		
		if (args.length > 2)
			numPartitions = Integer.parseInt(args[2]);
		
		
		SparkSession spark = SparkSession.builder()
				// Kryo Serialization
				.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.config(" spark.kryoserializer.buffer", "64k")
				.config("spark.kryo.registrationRequired", "true")
				.config("spark.kryo.registrator", MyKryoRegistrator.class.getName())
				.appName("ComplexObjectManipulation_Dataset")
				.config("spark.io.compression.codec", "lzf") // snappy, lzf, lz4 - Default=snappy 
//				.config("spark.speculation", "true")
				
				// just in case that you want to run this on localhost in stand-alone Spark mode
//				.master("local[*]") 
				.getOrCreate();
		
		

		// Encoders are created for Java beans
		Encoder<Customer> customerEncoder = Encoders.kryo(Customer.class);
		Encoder<SupplierData> supplierData_encoder = Encoders.kryo(SupplierData.class);

		// With huge list of Customer objects I got this exception
		// We need to add data step by step
		// Exception in thread "main" org.apache.spark.SparkException: Job
		// aborted due to stage failure: Serialized task 0:0 was 157073109
		// bytes, which exceeds max allowed: spark.rpc.message.maxSize
		// (134217728 bytes). Consider increasing

		if (args.length > 1)
			fileScale = args[1];

		List<Customer> customerData = DataGenerator.generateData(fileScale);
		Dataset<Customer> customerDS_raw = spark.createDataset(customerData, customerEncoder);


		Dataset<Customer> customerDS  = customerDS_raw;

		// Copy the same data multiple times to make it big data
		for (int i = 0; i < NUMBER_OF_COPIES; i++) {
			customerDS = customerDS.union(customerDS_raw);
		}
		
		
		// force spark to do the job and load data into RDD
		customerDS=customerDS.coalesce(numPartitions).cache();
		customerDS=customerDS.repartition(numPartitions);
		
		long numberOfCustomers=customerDS.count();
		System.out.println("Number of Customer: " + numberOfCustomers);

		System.out.println("Data is ready to use. ");
		
		// Now is the data generated and cached

		// #############################################
		// #############################################
		// #########       MAIN Experiment   ###########
		// #############################################
		// #############################################


		// Now is data loaded in RDD, ready for the experiment
		// Start the timer
		startTime = System.nanoTime();

		
		Dataset<SupplierData> customerNameLineItem = customerDS.flatMap(new FlatMapFunction<Customer, SupplierData>() {

			private static final long serialVersionUID = -3026278471244099707L;

			@Override
			public Iterator<SupplierData> call(Customer customer) throws Exception {
				List<SupplierData> returnList = new ArrayList<SupplierData>();
				
				Map<String, List<Integer>> soldPartIDs = new HashMap<String, List<Integer>>();
				
				List<Order> orders = customer.getOrders();
				for (Order order : orders) {
					List<LineItem> lineItems = order.getLineItems();
					for (LineItem lineItem : lineItems) {
						
						List<Integer> partIDs = new ArrayList<Integer>();
						partIDs.add((Integer) lineItem.getPart().getPartID());
						
						soldPartIDs.put(customer.getName(), partIDs);
						SupplierData mySupplierData =new SupplierData();
						
						mySupplierData.setSupplierKey(lineItem.getSupplier().getSupplierKey());
						mySupplierData.setSoldPartIDs(soldPartIDs);
						returnList.add(mySupplierData);
						
					}
				}
				return returnList.iterator();
			}
		}, supplierData_encoder);
		
		
		// Now Group the SupplierData by customer name as Key
		KeyValueGroupedDataset<Integer, SupplierData> grouped_supplierCustomerPartID_DS = customerNameLineItem.groupByKey(new MapFunction<SupplierData, Integer>() {

			private static final long serialVersionUID = -2443168521619624534L;

			@Override
			public Integer call(SupplierData arg0) throws Exception {

				return arg0.getSupplierKey();
			}
		}, Encoders.INT());

		
		
		
		// Now we go into each row with its key and reduce the values
		Dataset<Tuple2<Integer, SupplierData>> reduced_supplierCustomerPartID_DS = grouped_supplierCustomerPartID_DS.reduceGroups(new ReduceFunction<SupplierData>() {

			private static final long serialVersionUID = -6243848928318981991L;

			@Override
			public SupplierData call(SupplierData arg0, SupplierData arg1) throws Exception {
				// merge the two HashMaps inside the SupplierData
				arg0.merge(arg1);
				return arg0;
			}

		});

		
		
		long finalResultCount= reduced_supplierCustomerPartID_DS.count();
		

		// Stop the timer
		elapsedTotalTime += (System.nanoTime() - startTime) / 1000000000.0;

		System.out.println("Dataset#"+fileScale+"#"+NUMBER_OF_COPIES+"#"+numPartitions+"#"+numberOfCustomers+"#" +finalResultCount+"#"+ String.format("%.9f", elapsedTotalTime));

	}
}