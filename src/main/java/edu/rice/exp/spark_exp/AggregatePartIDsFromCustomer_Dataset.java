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

	// val zipper = udf[Seq[(String, Double)], Seq[String],
	// Seq[Double]](_.zip(_))

	public static void main(String[] args) throws FileNotFoundException, IOException {
		long startTime = 0;
		double elapsedTotalTime = 0;
		int NUMBER_OF_COPIES = 2;

		if (args.length > 0)
			NUMBER_OF_COPIES = Integer.parseInt(args[0]);

		PropertyConfigurator.configure("log4j.properties");

		SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example")
		// .master("local[*]") just in case that you want to run this on
		// localhost in stand-alone Spark mode
				.getOrCreate();

		Customer cust = new Customer();
		cust.setName("Customer-1");
		cust.setNationkey(1);

		// Encoders are created for Java beans
		Encoder<Customer> customerEncoder = Encoders.kryo(Customer.class);
		Encoder<TupleCustomerNameLineItem> tupleCustomerNameLineItem = Encoders.kryo(TupleCustomerNameLineItem.class);
		Encoder<SupplierCustomerPartID> supplierCustomerPartID_encoder = Encoders.bean(SupplierCustomerPartID.class);
		Encoder<SupplierData> supplierData_encoder = Encoders.bean(SupplierData.class);

		// Create the Dataset using kryo
		// Dataset<Customer> customerDS =
		// spark.createDataset(DataGenerator.generateData(),
		// customerEncoder
		// );

		// With huge list of Customer objects I got this exception
		// Exception in thread "main" org.apache.spark.SparkException: Job
		// aborted due to stage failure: Serialized task 0:0 was 157073109
		// bytes, which exceeds max allowed: spark.rpc.message.maxSize
		// (134217728 bytes). Consider increasing

		List<Customer> customerData = DataGenerator.generateData();

		List<Customer> customerData_tmp = new ArrayList<Customer>(5000);

		// for scale TPC-H 0.5 we have 75000 Customer Objects 
		for (int i = 0; i < 5000; i++) {
			customerData_tmp.add(customerData.get(i));
		}

		Dataset<Customer> customerDS = spark.createDataset(customerData_tmp, customerEncoder);
		
		for (int j = 5000; j < customerData.size(); j=j+5000) {
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

		customerDS.cache();
		// force spark to do the job and load data into RDD 
		
		long numberOfCustomers=customerDS.count();
		System.out.println("Number of Customer: " + numberOfCustomers);
		
		
		// Now is data loaded in RDD, ready for the experiment
		// Start the timer
		startTime = System.nanoTime();

		Dataset<TupleCustomerNameLineItem> customerNameLineItem = customerDS.flatMap(new FlatMapFunction<Customer, TupleCustomerNameLineItem>() {
			private static final long serialVersionUID = -3026278471244099707L;

			@Override
			public Iterator<TupleCustomerNameLineItem> call(Customer customer) throws Exception {
				List<TupleCustomerNameLineItem> returnList = new ArrayList<TupleCustomerNameLineItem>();
				List<Order> orders = customer.getOrders();
				for (Order order : orders) {
					List<LineItem> lineItems = order.getLineItems();
					for (LineItem lineItem : lineItems) {
						returnList.add(new TupleCustomerNameLineItem(customer.getName(), lineItem));
					}
				}
				return returnList.iterator();
			}
		}, tupleCustomerNameLineItem);

		System.out.println("Number of TupleCustomerNameLineItem in Dataset: " + customerNameLineItem.count());

		Dataset<SupplierCustomerPartID> supplierCustomerPartID_DS = customerNameLineItem.map(new MapFunction<TupleCustomerNameLineItem, SupplierCustomerPartID>() {

			private static final long serialVersionUID = -6842811770278738421L;

			@Override
			public SupplierCustomerPartID call(TupleCustomerNameLineItem arg0) throws Exception {
				SupplierCustomerPartID returnValue = new SupplierCustomerPartID(arg0.getCustomerName(), arg0.getLineItem().getSupplier().getName(), arg0.getLineItem().getPart()
						.getPartID());
				return returnValue;
			}

		}, supplierCustomerPartID_encoder);

		// supplierCustomerPartID_DS.show(5);

		Dataset<Row> dataSet1 = supplierCustomerPartID_DS.groupBy("customerName").agg(org.apache.spark.sql.functions.collect_list("supplierName"));

		// dataSet1.show(5);

		System.out.println(dataSet1.count());

		// Stop the timer
		elapsedTotalTime += (System.nanoTime() - startTime) / 1000000000.0;

		System.out.println(numberOfCustomers+"#" +String.format("%.9f", elapsedTotalTime));
	}
}