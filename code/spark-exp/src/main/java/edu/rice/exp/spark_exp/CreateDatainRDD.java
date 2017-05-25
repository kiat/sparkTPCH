package edu.rice.exp.spark_exp;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import edu.rice.dmodel.Customer;



/**
 * Hello world!
 *
 */
public class CreateDatainRDD {

	public static void main(String[] args) {

		PropertyConfigurator.configure("log4j.properties");

		// run on local machine with 8 CPU cores and 8GB spark memory
		SparkConf sparkConf = new SparkConf().setAppName("ComplexObjectManipulation").setMaster("local[8]").set("spark.executor.memory", "8g");

		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		List<Customer> employeeList=new ArrayList<Customer>();
		
		
		List<Customer> employeeList_tmp=new ArrayList<Customer>();


		// deep copy the data from the list to a tmp list. 
		for (Customer customer : employeeList) {
			employeeList_tmp.add(customer);
		}
		
		
		
		JavaRDD<Customer> employeeRDD = sc.parallelize(employeeList);
		
		

		
		
	}
}
