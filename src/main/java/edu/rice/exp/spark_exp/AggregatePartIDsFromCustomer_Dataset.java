package edu.rice.exp.spark_exp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;

import scala.Tuple2;
import scala.collection.Seq;
import edu.rice.dmodel.Customer;
import edu.rice.dmodel.LineItem;
import edu.rice.dmodel.Order;
import edu.rice.dmodel.Part;
import edu.rice.dmodel.Supplier;
import edu.rice.dmodel.SupplierCustomerPartID;
import edu.rice.dmodel.SupplierData;
import edu.rice.dmodel.TupleCustomerNameLineItem;
import edu.rice.generate_data.DataGenerator;


//col("...") is preferable to df.col("...")
import static  org.apache.spark.sql.functions.*;



public class AggregatePartIDsFromCustomer_Dataset {
	
 
	
	
	// val zipper = udf[Seq[(String, Double)], Seq[String], Seq[Double]](_.zip(_))

	public static void main(String[] args) throws FileNotFoundException, IOException {
		long startTime = 0;
		double elapsedTotalTime = 0;
		int NUMBER_OF_COPIES =1;
		
		if(args.length>0)
		NUMBER_OF_COPIES = Integer.parseInt(args[0]);

		PropertyConfigurator.configure("log4j.properties");
		
		  SparkSession spark = SparkSession
			      .builder()
			      .appName("Java Spark SQL basic example")
//			      .master("local[*]")  just in case that you want to run this on localhost in stand-alone Spark mode 
			      .getOrCreate();
		  
		  Customer cust=new Customer();
		  cust.setName("Customer-1");
		  cust.setNationkey(1);
		  
		  
		  
		    // Encoders are created for Java beans
		    Encoder<Customer> customerEncoder = Encoders.kryo(Customer.class);
		    Encoder <TupleCustomerNameLineItem> tupleCustomerNameLineItem=  Encoders.kryo(TupleCustomerNameLineItem.class);
		    Encoder <SupplierCustomerPartID> supplierCustomerPartID_encoder= Encoders.bean(SupplierCustomerPartID.class);
		    Encoder <SupplierData>   supplierData_encoder= Encoders.bean(SupplierData.class); 
		    
		    // Create the Dataset using kryo 		    
		    Dataset<Customer> customerDS = spark.createDataset(DataGenerator.generateData(), 
		      customerEncoder
		    );
		    
		    
//		    customerDS.show();
		  
		    // Copy the same data multiple times to make it big data 
		    // Original number is 15K 
		    // 2 copy means 15 X 2 =30 x 2 = 60
		    for (int i = 0; i < NUMBER_OF_COPIES; i++) {
		    	customerDS=customerDS.union(customerDS);
		    }

		    customerDS.cache();
		    System.out.println("Number of Customers in Dataset: "+customerDS.count());
		    
//			// fore to do the garbage collection 
//			System.gc();
//			
//	       // try to sleep for 5 seconds to be sure that all other tasks are done 
//			try {
//				Thread.sleep(5000);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
	
		  
			// Now is data loaded in RDD, ready for the experiment
			// Start the timer
			startTime = System.nanoTime();
		  
			
			Dataset<TupleCustomerNameLineItem> customerNameLineItem = customerDS.flatMap(new FlatMapFunction<Customer, TupleCustomerNameLineItem>() {
				private static final long serialVersionUID = -3026278471244099707L;

				@Override
				public Iterator<TupleCustomerNameLineItem> call(Customer customer) throws Exception {
					List<TupleCustomerNameLineItem> returnList = new ArrayList<TupleCustomerNameLineItem>();
					List<Order> orders= customer.getOrders();
					for (Order order : orders) {
						List<LineItem> lineItems= order.getLineItems();
						for (LineItem  lineItem : lineItems) {
							returnList.add(new TupleCustomerNameLineItem(customer.getName(), lineItem));
						}
					}
					return returnList.iterator();
				}}, tupleCustomerNameLineItem);
		
			
			
//			customerNameLineItem.show();
		    System.out.println("Number of TupleCustomerNameLineItem in Dataset: " + customerNameLineItem.count());
		    
		    Dataset<SupplierCustomerPartID>  supplierCustomerPartID_DS=customerNameLineItem.map(new MapFunction<TupleCustomerNameLineItem,SupplierCustomerPartID>(){

				private static final long serialVersionUID = -6842811770278738421L;

				@Override
				public SupplierCustomerPartID call(TupleCustomerNameLineItem arg0) throws Exception {
					SupplierCustomerPartID returnValue=new SupplierCustomerPartID(arg0.getCustomerName(), arg0.getLineItem().getSupplier().getName(),  arg0.getLineItem().getPart().getPartID()); 
					return returnValue;
				}
		    	
		    }, supplierCustomerPartID_encoder);
		    
		    supplierCustomerPartID_DS.show(5);

//		    spark.udf().register("simpleUDF", () -> v * v);

		    
		    Dataset<Row> dataSet1 = supplierCustomerPartID_DS.groupBy("customerName").agg(org.apache.spark.sql.functions.collect_list("supplierName"));
		    		 
		    
//		    Dataset<Row> dataSet1 = supplierCustomerPartID_DS.groupBy("customerName").agg(org.apache.spark.sql.functions.collect_list("supplierName").as("supplierName") ,
//		    		org.apache.spark.sql.functions.collect_list("partID").as("partID")); 
//		    		). withColumn("supplierName", callUDF("countTokens", col("words"))); 
//		    		.withColumn("supplierName", new Zipper(new Column("supplierName"), new Column("partID")));
		    		
		    
		    
		    dataSet1.show(5);
		    
		    System.out.println(dataSet1.schema());
		    
//		    Dataset<SupplierData>  supplierData_DS= dataSet1.map(new MapFunction<Row, SupplierData>() {
//
//				private static final long serialVersionUID = -5684258513081782258L;
//
//				@Override
//				public SupplierData call(Row arg0) throws Exception {
//					SupplierData returnResult=new SupplierData();
//					String customerName=arg0.getAs("customerName");
//
//					Integer[] partIDs= arg0.getAs("partID");
//					String[] supplierNames= arg0.getAs("supplierName");
//					
//					
//					
//					
//					returnResult.setCustomerName(customerName);
//					
//					
//					
//					return returnResult;
//				}
//		    	
//		    }, supplierData_encoder );

		    
		    dataSet1.toJavaRDD().coalesce(1,true).saveAsTextFile("output");
		    
		    
//		    Dataset<SupplierData>  supplierData_DS= dataSet1.map(new MapFunction<Row, SupplierData>() {
//				private static final long serialVersionUID = -5684258513081782258L;
//				@Override
//				public SupplierData call(Row arg0) throws Exception {
//					SupplierData returnResult=new SupplierData();
//					 Map<String, List<Integer>> tmp= new  HashMap<String, List<Integer>>();
//					 String key=(String)arg0.get(0);
//					 List<Integer> tmpList= (List<Integer>) arg0.get(1);
//					 tmp.put(key, tmpList);
//					return returnResult;
//				}
//		    }, supplierData_encoder);
//		    
//		    supplierData_DS.show();

//		    Dataset<SupplierCustomerPartID>  supplierCustomerPartID_DS=customerNameLineItem.map(new Function1<TupleCustomerNameLineItem,U>, supplierCustomerPartID_encoder)() {
//
//				@Override
//				public Iterator<SupplierCustomerPartID> call(TupleCustomerNameLineItem arg0) throws Exception {
//
//					SupplierCustomerPartID returnValue=new SupplierCustomerPartID(); 
//					
//					return returnValue;
//				}}, supplierCustomerPartID_encoder);

		    
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