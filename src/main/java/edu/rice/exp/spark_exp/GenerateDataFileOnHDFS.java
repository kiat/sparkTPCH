package edu.rice.exp.spark_exp;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.rice.dmodel.Customer;
import edu.rice.dmodel.MyKryoRegistrator;
import edu.rice.generate_data.DataGenerator;

public class GenerateDataFileOnHDFS {

	public static void main(String[] args) throws FileNotFoundException, IOException {

		String hdfsNameNodePath = "hdfs://10.134.96.100:9000/user/kia/customer-";

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
			hdfsNameNodePath = args[2];

		JavaRDD<Customer> customerRDD_raw = sc.parallelize(DataGenerator.generateData(fileScale), numPartitions);

		JavaRDD<Customer> customerRDD = customerRDD_raw;

		// Copy the same data multiple times to make it big data
		for (int i = 0; i < NUMBER_OF_COPIES; i++) {
			customerRDD = customerRDD.union(customerRDD_raw);
		}

		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		
		conf.set("fs.local.block.size", "268435456");

		customerRDD.saveAsObjectFile(hdfsNameNodePath + NUMBER_OF_COPIES);

		
//		JavaRDD<Customer> customerRDD_new = sc.objectFile("hdfs://10.134.96.100:9000/user/kia/customer-" + NUMBER_OF_COPIES); 
//		
//		long numberOfCustomers_new = customerRDD_new.count();
//		System.out.println("Number of New Customer: " + numberOfCustomers_new);
		
		
		// FileSystem hdfs;
		// try {
		// hdfs = FileSystem.get(new URI("hdfs://10.134.96.100:9000"),
		// hadoopConfig);
		//
		// Path file = new
		// Path("hdfs://10.134.96.100:9000/user/kia/customer.obj");
		//
		// if (hdfs.exists(file)) {
		// hdfs.delete(file, true);
		// }
		//
		//
		//
		// //
		// // OutputStream os = hdfs.create(file, new Progressable() {
		// // public void progress() {
		// // // out.println("...bytes written: [ "+bytesWritten+" ]");
		// // }
		// // });
		// //
		// // BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os,
		// // "UTF-8"));
		// // br.write("Hello World");
		// //
		// hdfs.close();
		//
		// } catch (URISyntaxException e) {
		// e.printStackTrace();
		// }

	}
}