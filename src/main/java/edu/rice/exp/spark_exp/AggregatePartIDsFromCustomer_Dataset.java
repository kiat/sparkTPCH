package edu.rice.exp.spark_exp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import edu.rice.dmodel.Customer;
import edu.rice.dmodel.LineItem;
import edu.rice.dmodel.Order;
import edu.rice.dmodel.SupplierData;
import edu.rice.generate_data.DataGenerator;

public class AggregatePartIDsFromCustomer_Dataset {




	public static void main(String[] args) throws FileNotFoundException, IOException {
		long startTime = 0;
		double elapsedTotalTime = 1;
		String fileScale = "0.1";

		int NUMBER_OF_COPIES=0;
		
		if (args.length > 0)
			NUMBER_OF_COPIES = Integer.parseInt(args[0]);

		if (args.length > 1)
			fileScale = args[1];
		
		
//		PropertyConfigurator.configure("log4j.properties");

		SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example")
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

		List<Customer> customerData_tmp = new ArrayList<Customer>(5000);

		// for scale TPC-H 0.5 we have 75000 Customer Objects
		for (int i = 0; i < 5000; i++) {
			customerData_tmp.add(customerData.get(i));
		}

		Dataset<Customer> customerDS = spark.createDataset(customerData_tmp, customerEncoder);

		for (int j = 5000; j < customerData.size(); j = j + 5000) {
			List<Customer> customerData_tmp1 = new ArrayList<Customer>(5000);
			for (int i = j; i < +5000; i++) {
				customerData_tmp1.add(customerData.get(i));
			}
			Dataset<Customer> customerDS_tmp = spark.createDataset(customerData_tmp, customerEncoder);
			customerDS = customerDS.union(customerDS_tmp);
		}

		// customerDS.show();

		// Copy the same data multiple times to make it big data
		// Original number is 15K
		// 2 copy means 15 X 2 =30 x 2 = 60
		for (int i = 0; i < NUMBER_OF_COPIES; i++) {
			customerDS = customerDS.union(customerDS);
		}

		// force spark to do the job and load data into RDD
		customerDS.cache();


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
		
		
		
		
		// Now is the data generated and cached

		// #############################################
		// #############################################
		// #########       MAIN Experiment   ###########
		// #############################################
		// #############################################

		long numberOfCustomers = customerDS.count();
		System.out.println("Number of Customer: " + numberOfCustomers);

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
						
						mySupplierData.setSupplierName(lineItem.getSupplier().getName());
						mySupplierData.setSoldPartIDs(soldPartIDs);
						returnList.add(mySupplierData);
						
					}
				}
				return returnList.iterator();
			}
		}, supplierData_encoder);
		
		
		
		
		
		// Now Group the SupplierData by customer name as Key
		KeyValueGroupedDataset<String, SupplierData> grouped_supplierCustomerPartID_DS = customerNameLineItem.groupByKey(new MapFunction<SupplierData, String>() {

			private static final long serialVersionUID = -2443168521619624534L;

			@Override
			public String call(SupplierData arg0) throws Exception {

				return arg0.getSupplierName();
			}
		}, Encoders.STRING());

		

		
		
		// Now we go into each row with its key and reduce the values
		Dataset<Tuple2<String, SupplierData>> reduced_supplierCustomerPartID_DS = grouped_supplierCustomerPartID_DS.reduceGroups(new ReduceFunction<SupplierData>() {

			private static final long serialVersionUID = -6243848928318981991L;

			@Override
			public SupplierData call(SupplierData arg0, SupplierData arg1) throws Exception {
				// merge the two HashMaps inside the SupplierData
				arg0.merge(arg1);
				return arg0;
			}

		});

		
		
		
//		 // Produce the final Result as a dataset of SupplierData Objects
//		 Dataset<SupplierData> finalResults=reduced_supplierCustomerPartID_DS.map(new MapFunction<Tuple2<String, SupplierData>, SupplierData>() {
//			private static final long serialVersionUID = 1092513431731531012L;
//			@Override
//			public SupplierData call(Tuple2<String, SupplierData> arg0) throws Exception {
//				return arg0._2;
//			}
//		}, supplierData_encoder);
//
//		 
//		 finalResults.show();
//		 List<SupplierData> someResults= finalResults.takeAsList(1);
//		 System.out.println(someResults);
		
		 System.out.println(reduced_supplierCustomerPartID_DS.count());

		// Stop the timer
		elapsedTotalTime += (System.nanoTime() - startTime) / 1000000000.0;

		System.out.println(numberOfCustomers + "#" + String.format("%.9f", elapsedTotalTime));
	}
}