package edu.rice.exp.spark_exp;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Hello world!
 *
 */
public class CreateDatainRDD {

	public static void main(String[] args) {

		PropertyConfigurator.configure("log4j.properties");

		// run on local machine with 8 CPU cores and 8GB spark memory
		SparkConf sparkConf = new SparkConf().setAppName("ComplexObjectManipulation").setMaster("localhost").set("spark.executor.memory", "8g");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		// JavaPairRDD<String, String> files2 = sc.wholeTextFiles("./20News/*");
		
		
		
//		JavaRDD<Employee> employeeRDD = arrayOfEmployeeList.flatMap(empArray -> Arrays.asList(empArray));

		
	}
}
