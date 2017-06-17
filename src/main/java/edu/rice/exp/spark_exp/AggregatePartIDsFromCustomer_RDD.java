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

public class AggregatePartIDsFromCustomer_RDD {
	
	
	private static JavaSparkContext sc;


	public static void main(String[] args) throws FileNotFoundException, IOException {
		long startTime = 0;
		double elapsedTotalTime = 0;
		int NUMBER_OF_COPIES =0;
		String fileScale="0.1";

		
		if(args.length>0)
		NUMBER_OF_COPIES = Integer.parseInt(args[0]);

		PropertyConfigurator.configure("log4j.properties");

		SparkConf sparkConf = new SparkConf()
		        .setAppName("ComplexObjectManipulation")
				.setMaster("local[*]")
				.set("spark.executor.memory", "32g")
				;

		sc = new JavaSparkContext(sparkConf);
		
		if (args.length > 1)
			fileScale = args[1];
		
		JavaRDD<Customer> customerRDD = sc.parallelize(DataGenerator.generateData(fileScale));
		
		// Copy the same data multiple times to make it big data 
		for (int i = 0; i < NUMBER_OF_COPIES; i++) {
			customerRDD = customerRDD.union(customerRDD);
		}
		
		customerRDD.cache();

		// force spark to do the job and load data into RDD 
		
		long numberOfCustomers=customerRDD.count();
		System.out.println("Number of Customer: " + numberOfCustomers);
		
		
//		// fore to do the garbage collection 
//		System.gc();
//		
//
//		// try to sleep for 5 seconds to be sure that all other tasks are done 
//		for (int i = 0; i < 5; i++) {
//			try {
//				Thread.sleep(1000);
//				System.out.println("Sleep for 1 sec ... ");
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}	
//		}
		
		System.out.println("Data is ready to use. ");
		
		// #############################################
		// #############################################
		// #########     MAIN Experiment   #############
		// #############################################
		// #############################################
		
		// Now is data loaded in RDD, ready for the experiment
		// Start the timer
		startTime = System.nanoTime();
		
		JavaRDD<Tuple2<String,  Tuple2<String, Integer>>>  soldLineItems2 = customerRDD.flatMap(new FlatMapFunction<Customer, Tuple2<String,  Tuple2<String, Integer>>>() {

			private static final long serialVersionUID = -7539917700784174380L;

			@Override
			public Iterator<Tuple2<String, Tuple2<String, Integer>>> call(Customer customer) throws Exception {
				List<Order> orders= customer.getOrders();
				List<Tuple2<String, Tuple2<String, Integer>>> returnList = new ArrayList<Tuple2<String, Tuple2<String, Integer>>>();
				for (Order order : orders) {
					List<LineItem> lineItems= order.getLineItems();
					for (LineItem  lineItem : lineItems) {
						Tuple2<String, Integer> supplierPartID=new Tuple2<String, Integer>(customer.getName(), lineItem.getPart().getPartID() );
						returnList.add(new Tuple2<String, Tuple2<String, Integer>>(lineItem.getSupplier().getName() , supplierPartID));
					}
				}
				return returnList.iterator();
			}	
		});
		

		JavaPairRDD<String,  Tuple2<String,  Integer>>  soldPartIDs =soldLineItems2
		.mapToPair(w ->  new Tuple2 <String, Tuple2<String,  Integer>>(w._1 , w._2)); 
		
		 // Now, we need to aggregate the results 
		// aggregateByKey needs 3 parameters:
		// 1. zero initializations, 
		// 2. a function to add data to a supplierData object and 
		// 3. a function to merge two supplierData objects. 
		JavaPairRDD<String, SupplierData> result = soldPartIDs.aggregateByKey(new SupplierData(), new Function2<SupplierData, Tuple2<String,  Integer>, SupplierData>(){

			private static final long serialVersionUID = 7295222894776950599L;

			@Override
			public SupplierData call(SupplierData suppData, Tuple2<String, Integer> tuple) throws Exception {
				suppData.addCustomer(tuple._1, tuple._2);
				return suppData;
			}
			
		}, new Function2<SupplierData, SupplierData, SupplierData>(){
			private static final long serialVersionUID = -1503342516335901464L;

			@Override
			public SupplierData call(SupplierData suppData1, SupplierData suppData2) throws Exception {
				// merge the two HashMaps inside the SupplierData
				suppData1.merge(suppData2);
				return suppData1;
			}});
		

		// At the end, what we need is a vector of SupplierData 
		JavaRDD<SupplierData> resultFinal = result.map(x -> x._2);
		
		System.out.println("Final Result Count:"+resultFinal.count());
		
		// Stop the timer
		elapsedTotalTime += (System.nanoTime() - startTime) / 1000000000.0;
		
		System.out.println(numberOfCustomers+"#" +String.format("%.9f", elapsedTotalTime));
	}
}