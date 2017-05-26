package edu.rice.exp.spark_exp;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.rice.dmodel.Customer;
import edu.rice.generate_data.DataGenerator;

public class LoadIntoRDDAndChangeIt {
	
	public static int NUMBER_OF_COPIES = 6;
	
	private static JavaSparkContext sc;


	public static void main(String[] args) throws FileNotFoundException, IOException {
		long startTime = 0;
		double elapsedTotalTime = 0;
		
		PropertyConfigurator.configure("log4j.properties");

		// run on local machine with 8 CPU cores and 8GB spark memory
		SparkConf sparkConf = new SparkConf().setAppName("ComplexObjectManipulation").setMaster("local[8]").set("spark.executor.memory", "8g");
		sc = new JavaSparkContext(sparkConf);
		
		JavaRDD<Customer> customerRDD = sc.parallelize(DataGenerator.generateData());

		for (int i = 0; i < NUMBER_OF_COPIES; i++) {
			customerRDD = customerRDD.union(customerRDD);
			System.out.println("Added " + (i+1) * 15000 + " Customers.");
		}

		// enforce spark to do the job and load data into RDD 
		System.out.println(customerRDD.count());

		// Data is generated and is loaded in RDD

		
		// Start the timer
		startTime = System.nanoTime();
		
		
		// modify each customers, go deep into orders -> lineitems -> parts
		JavaRDD<Customer> new_customerRDD = customerRDD.map(customer -> DataGenerator.changeIt(customer));

		// to enforce spark to do the job
     	System.out.println(new_customerRDD.count());

		elapsedTotalTime += (System.nanoTime() - startTime) / 1000000000.0;
		
		System.out.println(String.format("%.9f", elapsedTotalTime));

     	
		
	}

}
