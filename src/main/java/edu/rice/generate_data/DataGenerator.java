package edu.rice.generate_data;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import edu.rice.dmodel.Customer;
import edu.rice.dmodel.LineItem;
import edu.rice.dmodel.Order;
import edu.rice.dmodel.Part;
import edu.rice.dmodel.Supplier;

/**
 * 
 * @author Kia
 * 
 *         This class reads the TPC-H table files and generate data in various serializations together with their index files. Scale 0.1 of TPC-H dbgen
 *         generates 20000 Parts, 600000 LineItem and 15000 Customers. We store multiply each of these data sets with different factors. Parts * x100 i.e.,
 *         20000x 100 = 2000000 Parts that we store in file LineItems x20 i.e., 600000 x 20 = 12000000 LineItems Customers x80 i.e., 15000 x 80 = 1200000
 *         Customers
 */
public class DataGenerator {

	public static int NUMBER_OF_COPIES = 1;

	public static List<Customer> tmpList = new ArrayList<Customer>(1000);

	public static void main(String[] args) throws FileNotFoundException, IOException {

		PropertyConfigurator.configure("log4j.properties");

		JavaRDD<Customer> customerRDD = DataGenerator.generateData();

		for (int i = 0; i < NUMBER_OF_COPIES; i++) {
			customerRDD = customerRDD.union(customerRDD);
			System.out.println("Added " + i + " Customers Data.");

		}

		// Data is generated and is loaded in RDD

		// Start the timer
		
		
		// modify each customers, go deep into orders -> lineitems -> parts
		JavaRDD<Customer> new_customerRDD = customerRDD.map(customer -> DataGenerator.changeIt(customer));

		

		
//		System.out.println("Now Collect");
//		List<Customer> output = new_customerRDD.collect();
//
//		System.out.println(output.size());
//
//		print_a_SinglePart(output);

	}

	private static void print_a_SinglePart(List<Customer> output) {
		for (Customer customer : output) {
			List<Order> orders = customer.getOrders();
			if (orders.size() > 0) {
				List<LineItem> lineitems = orders.get(0).getLineItems();
				if (lineitems.size() > 0)
					System.out.println(customer.getOrders().get(0).getLineItems().get(0).getPart().getPartID());
			}
		}
	}

	public static Customer changeIt(Customer cust) {
		List<Order> orders = cust.getOrders();
		orders.parallelStream().forEach(order -> changeIt(order));
		cust.setOrders(orders);
		return cust;
	}

	public static Order changeIt(Order order) {
		List<LineItem> lineitems = order.getLineItems();
		lineitems.parallelStream().forEach(lineitem -> changeIt(lineitem));
		order.setLineItems(lineitems);
		return order;
	}

	public static LineItem changeIt(LineItem lineitem) {
		lineitem.setPart(changeIt(lineitem.getPart()));
		return lineitem;
	}

	public static Part changeIt(Part part) {
		part.setPartID(1);
		return part;
	}

	public static JavaRDD<Customer> generateData() throws FileNotFoundException, IOException {

		// run on local machine with 8 CPU cores and 8GB spark memory
		SparkConf sparkConf = new SparkConf().setAppName("ComplexObjectManipulation").setMaster("local[8]").set("spark.executor.memory", "8g");

		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		String filename = "0.1";

		String PartFile = "tables_scale_" + filename + "/part.tbl";
		String SupplierFile = "tables_scale_" + filename + "/supplier.tbl";
		String OrderFile = "tables_scale_" + filename + "/orders.tbl";
		String LineitemFile = "tables_scale_" + filename + "/lineitem.tbl";
		String CustomerFile = "tables_scale_" + filename + "/customer.tbl";

		HashMap<Integer, Part> partMap = new HashMap<Integer, Part>(6000000);
		HashMap<Integer, Supplier> supplierMap = new HashMap<Integer, Supplier>(100000);
		HashMap<Integer, ArrayList<LineItem>> lineItemMap = new HashMap<Integer, ArrayList<LineItem>>(60000000);
		HashMap<Integer, ArrayList<Order>> orderMap = new HashMap<Integer, ArrayList<Order>>(60000000);

		// ####################################
		// ####################################
		// ########## #########
		// ########## Part #########
		// ########## #########
		// ####################################
		// ####################################

		// READING PARTS line by line

		// CREATE TABLE PART ( P_PARTKEY INTEGER NOT NULL,
		// P_NAME VARCHAR(55) NOT NULL,
		// P_MFGR CHAR(25) NOT NULL,
		// P_BRAND CHAR(10) NOT NULL,
		// P_TYPE VARCHAR(25) NOT NULL,
		// P_SIZE INTEGER NOT NULL,
		// P_CONTAINER CHAR(10) NOT NULL,
		// P_RETAILPRICE DECIMAL(15,2) NOT NULL,
		// P_COMMENT VARCHAR(23) NOT NULL );
		System.out.println("Reading Parts ...");

		try (BufferedReader br = new BufferedReader(new FileReader(PartFile))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] data = line.split("\\|");
				Part myPart = new Part(Integer.parseInt(data[0]), data[1], data[2], data[3], data[4], Integer.parseInt(data[5]), data[6],
						Double.parseDouble(data[7]), data[8]);

				// index based on counter
				partMap.put(Integer.parseInt(data[0]), myPart);

			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// ####################################
		// ####################################
		// ########## #########
		// ########## Supplier #########
		// ########## #########
		// ####################################
		// ####################################

		// READING Suppliers line by line

		// CREATE TABLE [dbo].[SUPPLIER](
		// [S_SUPPKEY] [int] NOT NULL,
		// [S_NAME] [char](25) NOT NULL,
		// [S_ADDRESS] [varchar](40) NOT NULL,
		// [S_NATIONKEY] [int] NOT NULL,
		// [S_PHONE] [char](15) NOT NULL,
		// [S_ACCTBAL] [decimal](15, 2) NOT NULL,
		// [S_COMMENT] [varchar](101) NOT NULL
		// );

		System.out.println("Start reading Suppliers  ...");

		try (BufferedReader br = new BufferedReader(new FileReader(SupplierFile))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] data = line.split("\\|");
				Supplier mySupplier = new Supplier(Integer.parseInt(data[0]), data[1], data[2], Integer.parseInt(data[3]), data[4],
						Double.parseDouble(data[5]), data[6]);
				supplierMap.put(Integer.parseInt(data[0]), mySupplier);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// ####################################
		// ####################################
		// ########## #########
		// ########## LineItem #########
		// ########## #########
		// ####################################
		// ####################################

		// CREATE TABLE [dbo].[LINEITEM](
		// [L_ORDERKEY] [int] NOT NULL,
		// [L_PARTKEY] [int] NOT NULL,
		// [L_SUPPKEY] [int] NOT NULL,
		// [L_LINENUMBER] [int] NOT NULL,
		// [L_QUANTITY] [decimal](15, 2) NOT NULL,
		// [L_EXTENDEDPRICE] [decimal](15, 2) NOT NULL,
		// [L_DISCOUNT] [decimal](15, 2) NOT NULL,
		// [L_TAX] [decimal](15, 2) NOT NULL,
		// [L_RETURNFLAG] [char](1) NOT NULL,
		// [L_LINESTATUS] [char](1) NOT NULL,
		// [L_SHIPDATE] [date] NOT NULL,
		// [L_COMMITDATE] [date] NOT NULL,
		// [L_RECEIPTDATE] [date] NOT NULL,
		// [L_SHIPINSTRUCT] [char](25) NOT NULL,
		// [L_SHIPMODE] [char](10) NOT NULL,
		// [L_COMMENT] [varchar](44) NOT NULL
		// );
		System.out.println("Start reading LineItem  ...");

		// set counter back to zero
		BufferedReader brLineItems = new BufferedReader(new FileReader(LineitemFile));
		String lineLineItem;

		while ((lineLineItem = brLineItems.readLine()) != null) {

			String[] lineItemData = lineLineItem.split("\\|");
			int orderKey = Integer.parseInt(lineItemData[0]);
			int partKey = Integer.parseInt(lineItemData[1]);
			int supplierKey = Integer.parseInt(lineItemData[2]);

			// get the Part
			Part partTmp = null;
			if (partMap.containsKey(partKey))
				partTmp = partMap.get(partKey);
			else
				System.err.println("There is no such Part");

			// get the Supplier
			Supplier supplierTmp = null;
			if (supplierMap.containsKey(supplierKey))
				supplierTmp = supplierMap.get(supplierKey);
			else
				System.err.println("There is no such Supplier");

			LineItem tmpLineItem = new LineItem(orderKey, supplierTmp, partTmp, Integer.parseInt(lineItemData[3]), Double.parseDouble(lineItemData[4]),
					Double.parseDouble(lineItemData[5]), Double.parseDouble(lineItemData[6]), Double.parseDouble(lineItemData[7]), lineItemData[8],
					lineItemData[9], lineItemData[10], lineItemData[11], lineItemData[12], lineItemData[13], lineItemData[14], lineItemData[15]);

			if (lineItemMap.containsKey(orderKey)) {
				ArrayList<LineItem> values = lineItemMap.get(orderKey);

				values.add(tmpLineItem); // add the new one
				// put back
				lineItemMap.put(orderKey, values);

			} else {
				ArrayList<LineItem> values = new ArrayList<LineItem>();
				values.add(tmpLineItem); // add the new one
				// add for the first time
				lineItemMap.put(orderKey, values);
			}

		}

		// LineItems loaded to memory
		System.out.println("LineItems loaded to memory");

		// ####################################
		// ####################################
		// ########## #########
		// ########## Order #########
		// ########## #########
		// ####################################
		// ####################################

		// Reading Orders

		// CREATE TABLE ORDERS ( O_ORDERKEY INTEGER NOT NULL,
		// O_CUSTKEY INTEGER NOT NULL,
		// O_ORDERSTATUS CHAR(1) NOT NULL,
		// O_TOTALPRICE DECIMAL(15,2) NOT NULL,
		// O_ORDERDATE DATE NOT NULL,
		// O_ORDERPRIORITY CHAR(15) NOT NULL,
		// O_CLERK CHAR(15) NOT NULL,
		// O_SHIPPRIORITY INTEGER NOT NULL,
		// O_COMMENT VARCHAR(79) NOT NULL);

		System.out.println("Start reading Orders ...");

		BufferedReader brOrders = new BufferedReader(new FileReader(OrderFile));

		String lineOrder;

		while ((lineOrder = brOrders.readLine()) != null) {
			String[] orderData = lineOrder.split("\\|");
			int orderKey = Integer.parseInt(orderData[0]);
			int customerKey = Integer.parseInt(orderData[1]);

			Order myOrder = new Order(lineItemMap.get(orderKey), orderKey, Integer.parseInt(orderData[1]), orderData[2], Double.parseDouble(orderData[3]),
					orderData[4], orderData[5], orderData[6], Integer.parseInt(orderData[7]), orderData[8]);

			if (orderMap.containsKey(customerKey)) {
				ArrayList<Order> values = orderMap.get(customerKey);
				// add the new value
				values.add(myOrder);

				// put the list back
				orderMap.put(customerKey, values);
			} else {
				ArrayList<Order> values = new ArrayList<Order>();
				values.add(myOrder); // add the new one
				// add for the first time
				orderMap.put(customerKey, values);
			}

		}

		// Orders loaded to memory
		System.out.println("Orders loaded to memory");

		brLineItems.close();
		brOrders.close();

		// ####################################
		// ####################################
		// ########## #########
		// ########## Customers #########
		// ########## #########
		// ####################################
		// ####################################

		// Reading Orders
		// private List<Order> orders;
		// private int custkey;
		// private String name;
		// private String address;
		// private int nationkey;
		// private String phone;
		// private double accbal;
		// private String mktsegment;
		// private String comment;
		//

		System.out.println("Start reading Customers ...");

		List<Customer> customerList = new ArrayList<Customer>(1200000);

		BufferedReader brCustomers = new BufferedReader(new FileReader(CustomerFile));
		String lineCustomer;

		while ((lineCustomer = brCustomers.readLine()) != null) {

			String[] customerData = lineCustomer.split("\\|");
			int customerKey = Integer.parseInt(customerData[0]);

			ArrayList<Order> values = new ArrayList<Order>();

			if (orderMap.containsKey(customerKey)) {
				// if the orderMap contains another with this id add it to the List
				values = orderMap.get(customerKey);
			}

			Customer myCustomer = new Customer(values, customerKey, customerData[1], customerData[2], Integer.parseInt(customerData[3]), customerData[4],
					Double.parseDouble(customerData[5]), customerData[6], customerData[7]);

			customerList.add(myCustomer);
		}

		brCustomers.close();

		JavaRDD<Customer> customerRDD = sc.parallelize(customerList);

		return customerRDD;

	}

}
