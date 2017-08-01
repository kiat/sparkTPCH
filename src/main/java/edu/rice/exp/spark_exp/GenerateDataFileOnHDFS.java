package edu.rice.exp.spark_exp;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.storage.StorageLevel;

import edu.rice.dmodel.Customer;
import edu.rice.dmodel.MyKryoRegistrator;
import edu.rice.generate_data.DataGenerator;

public class GenerateDataFileOnHDFS {

	public static void main(String[] args) throws FileNotFoundException, IOException {

		String hdfsNameNodePath = "hdfs://10.134.96.100:8080/user/kia/";

		// define the number of partitions
		int numPartitions = 8;

		int NUMBER_OF_COPIES = 1;// number of Customers multiply X
									// 2^REPLICATION_FACTOR
		String fileScale = "0.2";

		if (args.length > 0)
			NUMBER_OF_COPIES = Integer.parseInt(args[0]);

		if (args.length > 1)
			fileScale = args[1];

		SparkConf conf = new SparkConf();

		// PropertyConfigurator.configure("log4j.properties");

		// conf.set("spark.executor.memory", "32g");
		conf.setMaster("local[*]");

		conf.setAppName("GenerateDataFileOnHDFS");

		// Kryo Serialization
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kryoserializer.buffer.mb", "64");
		conf.set("spark.kryo.registrationRequired", "true");
		conf.set("spark.kryo.registrator", MyKryoRegistrator.class.getName());

		conf.set("spark.io.compression.codec", "lzf"); // snappy, lzf, lz4
		// conf.set("spark.speculation", "true");
		// conf.set("spark.local.dir", "/mnt/sparkdata");

		JavaSparkContext sc = new JavaSparkContext(conf);

		if (args.length > 2)
			hdfsNameNodePath = args[1];

		JavaRDD<Customer> customerRDD_raw = sc.parallelize(DataGenerator.generateData(fileScale), numPartitions);

		JavaRDD<Customer> customerRDD = customerRDD_raw;

		// Copy the same data multiple times to make it big data
		for (int i = 0; i < NUMBER_OF_COPIES; i++) {
			customerRDD = customerRDD.union(customerRDD_raw);
		}
		
		
		// Caching made the experiment slower
		// System.out.println("Cache the data");
//		customerRDD = customerRDD.coalesce(numPartitions);

		// customerRDD.persist(StorageLevel.MEMORY_ONLY_2());

		// customerRDD.persist(StorageLevel.MEMORY_AND_DISK());
//		customerRDD.persist(StorageLevel.MEMORY_ONLY_SER());

		// System.out.println("Get the number of Customers");
		//
		// // force spark to do the job and load data into RDD
		// long numberOfCustomers = customerRDD.count();
		// System.out.println("Number of Customer: " + numberOfCustomers);
		//
		// // do something else to have the data in memory
		// long numberOfDistinctCustomers = customerRDD.distinct().count();
		// System.out.println("Number of Distinct Customer: " +
		// numberOfDistinctCustomers);

		//
		// Configuration config = new Configuration();
		// FileSystem fs = FileSystem.get(config);
		//
		// Path filenamePath = new Path("CUSTOMER.obj");
		//
		// if (fs.exists(filenamePath)) {
		// fs.delete(filenamePath, true);
		// }
		//
		// FSDataOutputStream fin = fs.create(filenamePath);
		// fin.writeUTF("hello");
		// fin.close();
		//

//		Configuration hadoopConfig = SparkHadoopUtil.get().newConfiguration(conf);
//
//		hadoopConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//		hadoopConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
//		hadoopConfig.set("fs.defaultFS", hdfsNameNodePath);

		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		
		customerRDD.saveAsObjectFile("hdfs://10.134.96.100:9000/user/kia/customer-"+NUMBER_OF_COPIES);

		
		
//		FileSystem hdfs;
//		try {
//			hdfs = FileSystem.get(new URI("hdfs://10.134.96.100:9000"), hadoopConfig);
//
//			Path file = new Path("hdfs://10.134.96.100:9000/user/kia/customer.obj");
//
//			if (hdfs.exists(file)) {
//				hdfs.delete(file, true);
//			}
//			
//
//			
//			//
//			// OutputStream os = hdfs.create(file, new Progressable() {
//			// public void progress() {
//			// // out.println("...bytes written: [ "+bytesWritten+" ]");
//			// }
//			// });
//			//
//			// BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os,
//			// "UTF-8"));
//			// br.write("Hello World");
//			//
//			hdfs.close();
//
//		} catch (URISyntaxException e) {
//			e.printStackTrace();
//		}


	}
}