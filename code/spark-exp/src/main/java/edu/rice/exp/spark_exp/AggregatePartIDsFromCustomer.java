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
import org.apache.spark.api.java.function.PairFunction;

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
//		SparkConf sparkConf = new SparkConf().setAppName("ComplexObjectManipulation").setMaster("local[8]").set("spark.executor.memory", "32g");
		SparkConf sparkConf = new SparkConf();

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

		
		
		JavaPairRDD<String, SupplierData> result = soldPartIDs.aggregateByKey(new SupplierData(), new Function2<SupplierData, Tuple2<String,  Integer>, SupplierData>(){

//			private static final long serialVersionUID = 7295222894776950599L;

			@Override
			public SupplierData call(SupplierData suppData, Tuple2<String, Integer> tuple) throws Exception {
				suppData.addCustomer(tuple._1, tuple._2);
				return suppData;
			}
			
		}, new Function2<SupplierData, SupplierData, SupplierData>(){
//			private static final long serialVersionUID = -1503342516335901464L;

			@Override
			public SupplierData call(SupplierData suppData1, SupplierData suppData2) throws Exception {
				
				 Map<String, List<Integer>> tmp=suppData1.getSoldPartIDs();
				 tmp.putAll(suppData2.getSoldPartIDs());
				 suppData1.setSoldPartIDs(tmp);
				
				return suppData1;
			}});
		
		
				
		List sold=result.take(10);
		
		for (Object  object: sold) {
			System.out.println(object);
		}

		
		// Stop the timer
		elapsedTotalTime += (System.nanoTime() - startTime) / 1000000000.0;
		
		System.out.println(String.format("%.9f", elapsedTotalTime));

     	
		
	}

}
