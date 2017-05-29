package edu.rice.exp.spark_exp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
import edu.rice.dmodel.SupplierData;
import edu.rice.generate_data.DataGenerator;

public class AggregatePartIDsFromCustomer {
	
	
	private static JavaSparkContext sc;


	public static void main(String[] args) throws FileNotFoundException, IOException {
		long startTime = 0;
		double elapsedTotalTime = 0;
		int NUMBER_OF_COPIES =1;
		
		if(args.length>0)
		NUMBER_OF_COPIES = Integer.parseInt(args[0]);

		PropertyConfigurator.configure("log4j.properties");

		// run on local machine with 8 CPU cores and 8GB spark memory
		SparkConf sparkConf = new SparkConf().setAppName("ComplexObjectManipulation").setMaster("local[8]").set("spark.executor.memory", "32g");
//		SparkConf sparkConf = new SparkConf();

		sc = new JavaSparkContext(sparkConf);
		
		JavaRDD<Customer> customerRDD = sc.parallelize(DataGenerator.generateData());
		
		customerRDD.cache();

		// Copy the same data multiple times to make it big data 
		for (int i = 0; i < NUMBER_OF_COPIES; i++) {
			customerRDD = customerRDD.union(customerRDD);
			System.out.println("Added " + (i+1) * 15000 + " Customers.");
		}

		// enforce spark to do the job and load data into RDD 
		System.out.println(customerRDD.count());

		// Now is data loaded in RDD, ready for the experiment
		// Start the timer
		startTime = System.nanoTime();

		
		
//		JavaRDD<Tuple2<String, Order>>  customerOrders = customerRDD.flatMap(new FlatMapFunction<Customer, Tuple2<String,  Order>>() {
//			@Override
//			public Iterator<Tuple2<String, Order>> call(Customer customer) throws Exception {
//
//				List<Tuple2<String, Order>> returnList = new ArrayList<Tuple2<String, Order>>();
//			
//				List<Order> orders= customer.getOrders(); 
//				
//				for (Order order : orders) {
//					returnList.add(new Tuple2<String, Order>(customer.getName(), order)); 
//				}
//				return returnList.iterator();
//			}
//		}) ; 
		
		
		
		JavaRDD< Tuple2<String,  LineItem>>  soldLineItems = customerRDD.flatMap(new FlatMapFunction<Customer, Tuple2<String,  LineItem>>() {

			@Override
			public Iterator<Tuple2<String, LineItem>> call(Customer customer) throws Exception {
				List<Order> orders= customer.getOrders();
				
				List<Tuple2<String, LineItem>> returnList = new ArrayList<Tuple2<String, LineItem>>();
				for (Order order : orders) {
					List<LineItem> lineItems= order.getLineItems();
					for (LineItem  lineItem : lineItems) {
						returnList.add(new Tuple2<String, LineItem>(customer.getName(), lineItem));
					}
				}
				return returnList.iterator();
			}	
		});
		
		
		JavaPairRDD<String,  Tuple2<String,  Integer>>  soldPartIDs =soldLineItems.mapToPair(w ->  new Tuple2(w._2.getSupplier().getName() , new Tuple2(w._1, w._2.getPart().getPartID()))); 
		
		
//		JavaPairRDD<String,  SupplierData> result=soldPartIDs.reduceByKey(new Function2<Tuple2<String,  Integer>, Tuple2<String,  Integer>, SupplierData>() {
//
//			@Override
//			public SupplierData call(Tuple2<String, Integer> arg0, Tuple2<String, Integer> arg1) throws Exception {
//
//				
//				return null;
//			}
//			
//		}); 
 
//		JavaPairRDD<String,  Tuple2<SupplierData>> result=soldPartIDs.reduceByKey(
				
				
				
//				new Function2<Tuple2<String,  Integer>, Tuple2<String,  Integer>, Tuple2<SupplierData>>() {
//
//			@Override
//			public Tuple2<SupplierData> call(Tuple2<String, Integer> arg0, Tuple2<String, Integer> arg1) throws Exception {
//				List<Tuple2<String, Integer>> returnList = new ArrayList<Tuple2<String, Integer>>();
//
//				
//				
//				return null;
//			}}
//				);
		
		
		
//		// reduce task to count the overall counts of words in all documents
//				JavaPairRDD<String, Integer> counts = wordsCountPairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
//
//					private static final long serialVersionUID = 6834587562219302952L;
//
//							@Override
//							public Integer call(Integer i1, Integer i2) {
//								return i1 + i2;
//							}
//						});
				
				
		List sold=soldPartIDs.take(10);
		
		for (Object  object: sold) {
			System.out.println(object);
		}
		
		
		
		
		
		
//		
//		// modify each customers, go deep into orders -> lineitems -> parts
//		JavaRDD<Customer> new_customerRDD = customerRDD.map(customer -> DataGenerator.changeIt(customer));
//
//		// to enforce spark to do the job
//     	System.out.println(new_customerRDD.count());

		// Stop the timer
		elapsedTotalTime += (System.nanoTime() - startTime) / 1000000000.0;
		
		System.out.println(String.format("%.9f", elapsedTotalTime));

     	
		
	}

}
