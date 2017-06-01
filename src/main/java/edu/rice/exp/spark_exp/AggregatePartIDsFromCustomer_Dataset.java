package edu.rice.exp.spark_exp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import edu.rice.dmodel.Customer;
import edu.rice.dmodel.LineItem;
import edu.rice.dmodel.Order;
import edu.rice.dmodel.SupplierCustomerPartID;
import edu.rice.dmodel.SupplierData;
import edu.rice.dmodel.TupleCustomerNameLineItem;
import edu.rice.generate_data.DataGenerator;


//col("...") is preferable to df.col("...")
//import static  org.apache.spark.sql.functions.*;



public class AggregatePartIDsFromCustomer_Dataset {
	
 
	
	
	// val zipper = udf[Seq[(String, Double)], Seq[String], Seq[Double]](_.zip(_))

	public static void main(String[] args) throws FileNotFoundException, IOException {
		long startTime = 0;
		double elapsedTotalTime = 0;
		int NUMBER_OF_COPIES =2;
		
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
		
		    System.out.println("Number of TupleCustomerNameLineItem in Dataset: " + customerNameLineItem.count());
		    
		    Dataset<SupplierCustomerPartID>  supplierCustomerPartID_DS=customerNameLineItem.map(new MapFunction<TupleCustomerNameLineItem,SupplierCustomerPartID>(){

				private static final long serialVersionUID = -6842811770278738421L;

				@Override
				public SupplierCustomerPartID call(TupleCustomerNameLineItem arg0) throws Exception {
					SupplierCustomerPartID returnValue=new SupplierCustomerPartID(arg0.getCustomerName(), arg0.getLineItem().getSupplier().getName(),  arg0.getLineItem().getPart().getPartID()); 
					return returnValue;
				}
		    	
		    }, supplierCustomerPartID_encoder);
		    
//		    supplierCustomerPartID_DS.show(5);
		    
		    Dataset<Row> dataSet1 = supplierCustomerPartID_DS.groupBy("customerName").agg(org.apache.spark.sql.functions.collect_list("supplierName"));
		    		 
//		    dataSet1.show(5);
		    
		    System.out.println(dataSet1.count());
		    
		// Stop the timer
		elapsedTotalTime += (System.nanoTime() - startTime) / 1000000000.0;
		
		System.out.println(String.format("%.9f", elapsedTotalTime));
	}
}