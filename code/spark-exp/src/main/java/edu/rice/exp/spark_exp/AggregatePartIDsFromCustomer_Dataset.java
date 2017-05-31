package edu.rice.exp.spark_exp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
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

public class AggregatePartIDsFromCustomer_Dataset {
	
	
	private static JavaSparkContext sc;


	public static void main(String[] args) throws FileNotFoundException, IOException {
		long startTime = 0;
		double elapsedTotalTime = 0;
		int NUMBER_OF_COPIES =2;
		
		if(args.length>0)
		NUMBER_OF_COPIES = Integer.parseInt(args[0]);

		PropertyConfigurator.configure("log4j.properties");

		
		
//		SparkConf sparkConf = new SparkConf()
//		        .setAppName("ComplexObjectManipulation")
//				.setMaster("local[*]")
//				.set("spark.executor.memory", "32g")
//				;

		
		
		  SparkSession spark = SparkSession
			      .builder()
			      .appName("Java Spark SQL basic example")
			      .config("spark.some.config.option", "some-value")
			      .getOrCreate();
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
//		  private static void runDatasetCreationExample(SparkSession spark) {
//			    // $example on:create_ds$
//			    // Create an instance of a Bean class
//			    Person person = new Person();
//			    person.setName("Andy");
//			    person.setAge(32);
//
//			    // Encoders are created for Java beans
//			    Encoder<Person> personEncoder = Encoders.bean(Person.class);
//			    Dataset<Person> javaBeanDS = spark.createDataset(
//			      Collections.singletonList(person),
//			      personEncoder
//			    );
//			    javaBeanDS.show();
//			    // +---+----+
//			    // |age|name|
//			    // +---+----+
//			    // | 32|Andy|
//			    // +---+----+
//
//			    // Encoders for most common types are provided in class Encoders
//			    Encoder<Integer> integerEncoder = Encoders.INT();
//			    Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
//			    Dataset<Integer> transformedDS = primitiveDS.map(
//			        (MapFunction<Integer, Integer>) value -> value + 1,
//			        integerEncoder);
//			    transformedDS.collect(); // Returns [2, 3, 4]
//
//			    // DataFrames can be converted to a Dataset by providing a class. Mapping based on name
//			    String path = "examples/src/main/resources/people.json";
//			    Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
//			    peopleDS.show();
//			    // +----+-------+
//			    // | age|   name|
//			    // +----+-------+
//			    // |null|Michael|
//			    // |  30|   Andy|
//			    // |  19| Justin|
//			    // +----+-------+
//			    // $example off:create_ds$
//			  }

		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
//		sc = new JavaSparkContext(sparkConf);
//		
//		JavaRDD<Customer> customerRDD = sc.parallelize(DataGenerator.generateData());
//		
//		Dataset customerset
//		
//		
//		customerRDD.cache();
//
//		// Copy the same data multiple times to make it big data 
//		for (int i = 0; i < NUMBER_OF_COPIES; i++) {
//			customerRDD = customerRDD.union(customerRDD);
//		}
//
//		// force spark to do the job and load data into RDD 
//		System.out.println(customerRDD.count());
//		
//		// fore to do the garbage collection 
//		System.gc();
//		
//       // try to sleep for 5 seconds to be sure that all other tasks are done 
//		try {
//			Thread.sleep(5000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//		
		// Now is data loaded in RDD, ready for the experiment
		// Start the timer
		startTime = System.nanoTime();
		
		
		
//		
//		JavaRDD< Tuple2<String,  LineItem>>  soldLineItems = customerRDD.flatMap(new FlatMapFunction<Customer, Tuple2<String,  LineItem>>() {
//
//			private static final long serialVersionUID = -7539917700784174380L;
//
//			@Override
//			public Iterator<Tuple2<String, LineItem>> call(Customer customer) throws Exception {
//				List<Order> orders= customer.getOrders();
//				
//				List<Tuple2<String, LineItem>> returnList = new ArrayList<Tuple2<String, LineItem>>();
//				for (Order order : orders) {
//					List<LineItem> lineItems= order.getLineItems();
//					for (LineItem  lineItem : lineItems) {
//						returnList.add(new Tuple2<String, LineItem>(customer.getName(), lineItem));
//					}
//				}
//				return returnList.iterator();
//			}	
//		});
//		
//		
//		JavaPairRDD<String,  Tuple2<String,  Integer>>  soldPartIDs =soldLineItems
//				.mapToPair(w ->  new Tuple2 <String, Tuple2<String,  Integer>>(w._2.getSupplier().getName() , new Tuple2<String,  Integer>(w._1, w._2.getPart().getPartID()))); 
//		
//		
//		 // Now, we need to aggregate the results 
//		// aggregateByKey needs 3 parameters:
//		// 1. zero initializations, 
//		// 2. a function to add data to a supplierData object and 
//		// 3. a function to merge two supplierData objects. 
//		JavaPairRDD<String, SupplierData> result = soldPartIDs.aggregateByKey(new SupplierData(), new Function2<SupplierData, Tuple2<String,  Integer>, SupplierData>(){
//
//			private static final long serialVersionUID = 7295222894776950599L;
//
//			@Override
//			public SupplierData call(SupplierData suppData, Tuple2<String, Integer> tuple) throws Exception {
//				suppData.addCustomer(tuple._1, tuple._2);
//				return suppData;
//			}
//			
//		}, new Function2<SupplierData, SupplierData, SupplierData>(){
//			private static final long serialVersionUID = -1503342516335901464L;
//
//			@Override
//			public SupplierData call(SupplierData suppData1, SupplierData suppData2) throws Exception {
//				
//				 Map<String, List<Integer>> tmp=suppData1.getSoldPartIDs();
//				 tmp.putAll(suppData2.getSoldPartIDs());
//				 suppData1.setSoldPartIDs(tmp);
//				
//				return suppData1;
//			}});
//		
//
//		result.saveAsTextFile("output");
		
		// Stop the timer
		elapsedTotalTime += (System.nanoTime() - startTime) / 1000000000.0;
		
		System.out.println(String.format("%.9f", elapsedTotalTime));
	}
}