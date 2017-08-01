package edu.rice.exp.spark_exp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.rice.dmodel.Customer;
import edu.rice.dmodel.MyKryoRegistrator;
import edu.rice.generate_data.DataGenerator;

// used SPARL Submit command like this
// spark-submit --master  spark://10.134.96.100:7077  --class edu.rice.exp.spark_exp.GenerateDataFileOnHDFS  --executor-cores 4  --executor-memory 4G  --driver-memory 10G ./target/spark-exp-0.0.1-SNAPSHOT-jar-with-dependencies.jar  "5,10,15,20"  0.2  10


public class GenerateDataFileOnHDFS_RDD {

	public static void main(String[] args) throws FileNotFoundException, IOException {

		String hdfsNameNodePath = "hdfs://10.134.96.100:9000/user/kia/customer-";

		// define the number of partitions
		int numPartitions = 8;

		int NUMBER_OF_COPIES = 1;// number of Customers multiply X
									// 2^REPLICATION_FACTOR
		String fileScale = "0.2";

		// if (args.length > 0)
		// NUMBER_OF_COPIES = Integer.parseInt(args[0]);

		// read a set of integer numbers - data sets that we need to save on
		// hdfs.
		// The numbers indicate the replication number of data, on each of these
		// numbers
		// we save the object file on a separate folder to be able to access
		// them for the final experiments
		String s = args[0];
		String[] numberOfCopies_string = s.split(",");
		Set<Integer> numberOfCopies_set = new HashSet<Integer>(numberOfCopies_string.length);

		for (int i = 0; i < numberOfCopies_string.length; i++) {
			int tmp = Integer.parseInt(numberOfCopies_string[i].replaceAll(" ", ""));
			numberOfCopies_set.add(tmp);
			if (i == (numberOfCopies_string.length - 1))
				// the last number is the maximum
				NUMBER_OF_COPIES = tmp;
		}

		if (args.length > 1)
			fileScale = args[1];

		if (args.length > 2)
			numPartitions = Integer.parseInt(args[2]);

		if (args.length > 3)
			hdfsNameNodePath = args[3];

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

		// hadoop configurations
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		// set the block size to 256 MB
		conf.set("fs.local.block.size", "268435456");

		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Customer> customerRDD_raw = sc.parallelize(DataGenerator.generateData(fileScale), numPartitions);

		JavaRDD<Customer> customerRDD = customerRDD_raw;

		// Copy the same data multiple times to make it big data
		for (int i = 0; i < NUMBER_OF_COPIES+1; i++) {
			customerRDD = customerRDD.union(customerRDD_raw);

			if (numberOfCopies_set.contains(i)) {
				System.out.println("Saveing the dataset for " + i);
				// coalesce the RDD based on number of partitions.
				customerRDD = customerRDD.coalesce(numPartitions);
				customerRDD.saveAsObjectFile(hdfsNameNodePath + i);
			}
		}

		// JavaRDD<Customer> customerRDD_new =
		// sc.objectFile("hdfs://10.134.96.100:9000/user/kia/customer-" +
		// NUMBER_OF_COPIES);
		//
		// long numberOfCustomers_new = customerRDD_new.count();
		// System.out.println("Number of New Customer: " +
		// numberOfCustomers_new);

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