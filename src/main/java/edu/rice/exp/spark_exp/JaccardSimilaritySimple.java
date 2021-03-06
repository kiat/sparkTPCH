package edu.rice.exp.spark_exp;

import java.io.BufferedReader;
import java.util.Comparator;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import edu.rice.dmodel.Customer;
import edu.rice.dmodel.LineItem;
import edu.rice.dmodel.MyKryoRegistrator;
import edu.rice.dmodel.Order;

/**
 * This is a class for computing a Top-K Similarity Query using
 * JaccardSimilarity between a Query Object and entries in the 
 * dataset.
 * 
 * The objects to compare are as follows: For each Customer 
 * obtain a list of all Orders and within each Order, obtain a 
 * list of unique Part ID's (i.e. duplicates are included only
 * once).
 * 
 * For each of these objects we compute Jaccard Similarity,
 * which is calculated as the number of parts that are in
 * both the Query Object and a Customer's List (intersection), 
 * divided by the total number of parts in both sets 
 * (duplicated counted once).
 *  
 * First, for each customer we obtain a list of unique parts 
 * ordered for all orders, returning a pair of:
 * 
 * Tuple2< customer.key, List<partId's> >
 * 
 * Second, for each customer we calculate the Jaccard Similarity
 * against the list of parts from the query, returning a pair of:
 * 
 * Tuple2< similarity.value, Tuple2< Customer.key List<partId's>>,
 * similarity.values closer to 1. means the query is more similar
 * to a give Customer's list; values closer to 0, the lists
 * are less similar.
 * 
 * Third, we obtained the top K entries based on the similarity.value
 * and return them.
 *
 */

public class JaccardSimilaritySimple implements Serializable {
    
    private static final long serialVersionUID = 3045541819871271488L;    

    public static void main(String[] args) throws FileNotFoundException, IOException {

        long numberOfCustomers = 0;
        long numberOfDistinctCustomers = 0;
        
        long startTime = 0;             // timestamp from the beginning
        long readFileTime = 0;          // timestamp after reading from HDFS
        long countTimestamp = 0;        // timestamp after count that reads
                                        // from disk into RDD
        long startQueryTimestamp = 0;   // timestamp before query begins
        long finalTimestamp = 0;        // timestamp final

        double readsHDFSTime = 0;       // time to read from HDFS (not including count + count.distinct)
        double loadRDDTime = 0;         // time to load RDD in memory (includes count + count.distinct)
        double countTime = 0;           // time to count (includes only count)
        double queryTime = 0;           // time to run the query (doesn't include data load)
        double elapsedTotalTime = 0;    // total elapsed time

        // number of Customers multiply X 2^REPLICATION_FACTOR
        // can be overwritten by the first arg
        int NUMBER_OF_COPIES = 0;
        
        // Default name of file with query data
        // can be overwritten by 2n arg
        String inputQueryFile = "jaccardInput";        
        
        // what top K elements to include in query
        // can be overwritten by the 3rd arg
        int topKValue = 10;
                
        // URL of the hdfs file system
        // can be overwritten by the 4th arg
        String hdfsNameNodePath = "hdfs://10.134.96.100:9000/user/kia/customer-";        

        // 0 = the query time doesn't include count nor count.distinct
        // (thus calculated time includes reading from HDFS)
        // 1 = the query includes count and count.distinct (default)
        // can be overwritten by 5th arg
        int warmCache = 1;
        
        if (args.length > 0)
            NUMBER_OF_COPIES = Integer.parseInt(args[0]);

        if (args.length > 1)        
            inputQueryFile = args[1];

        String[] listOfParts = null;
        
        // reads list of partID's from a system file
        try (BufferedReader br = new BufferedReader(new FileReader(inputQueryFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                listOfParts = line.split(",");
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }        

        // Creates a List<Integer> with the PartID's to be used for 
        // the query
        List<Integer> queryListOfPartsIds = 
            new ArrayList<Integer>(listOfParts.length);        

        for (int i = 0; i < listOfParts.length; i++) {
            int tmp = Integer.parseInt(listOfParts[i]);
            queryListOfPartsIds.add(tmp);
        }

        Collections.sort(queryListOfPartsIds);
                
        if (args.length > 2)
            topKValue = Integer.parseInt(args[2]);

        if (args.length > 3)
            hdfsNameNodePath = args[3];

        if (args.length > 4)
            warmCache = Integer.parseInt(args[4]);

        SparkConf conf = new SparkConf();
        conf.setAppName("JaccardScoreSimple-" + NUMBER_OF_COPIES);
                
        // Kryo Serialization
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrationRequired", "true");
        conf.set("spark.kryo.registrator", MyKryoRegistrator.class.getName());

        conf.set("spark.io.compression.codec", "lzf"); // snappy, lzf, lz4

        conf.set("spark.shuffle.spill", "true");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // Print application Id so it can be used via REST API to analyze processing
        // times
        System.out.println("Application Id: " + sc.sc().applicationId());

        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        conf.set("fs.local.block.size", "268435456");
                        
        // Get the initial time
        startTime = System.nanoTime();

        JavaRDD<Customer> customerRDD = sc.objectFile(hdfsNameNodePath + NUMBER_OF_COPIES);

        customerRDD.persist(StorageLevel.MEMORY_ONLY_SER());
        
        readFileTime = System.nanoTime();

        if (warmCache == 1) {
            System.out.println("Get the number of Customers");

            // force spark to do the job and load data into RDD
            numberOfCustomers = customerRDD.count();

            countTimestamp = System.nanoTime();

            System.out.println("Number of Customer: " + numberOfCustomers);

            // do something else to have the data in memory
            numberOfDistinctCustomers = customerRDD.distinct().count();
            System.out.println("Number of Distinct Customer: " + numberOfDistinctCustomers);

        }
        
        System.out.println("The query parts are: " + queryListOfPartsIds.toString() );

        // #############################################
        // #############################################
        // #########     MAIN Experiment   #############
        // #############################################
        // #############################################

        // Now is data loaded in RDD, ready for the experiment
        // Start the timer
        startQueryTimestamp = System.nanoTime();
        
        // map that generates pairs of <Similarity.score, Tuple2<Customer.key, List<PartID>>>
        JavaPairRDD<Double, Tuple2<Integer, List<Integer>>> jaccardSimilarityScore = 
            customerRDD.mapToPair(new PairFunction<Customer,  			// Type of Input Object: A Customer
                                  Double,                    			// Returning Key: Customer
                                  Tuple2<Integer, List<Integer>>>() {   // Returning Value: A List of all parts Id's
            															// from all orders for each customer
    
                private static final long serialVersionUID = 1932241819871271488L;
    
                @Override
                public Tuple2<Double, Tuple2<Integer, List<Integer>>> call(Customer customer) throws Exception {
                    List<Order> orders = customer.getOrders();
                    Integer customerKey = customer.getCustkey();

                    // List for storing all partID's for this Customer
                    List<Integer> listOfPartsIds = 
                        new ArrayList<Integer>();
                        
                    // iterates over all orders for a customer
                    for (Order order : orders) {
                        
                        List<LineItem> lineItems = order.getLineItems();
                        Integer partKey = new Integer(0);
                        
                        //iterates over the items in an order
                        for (LineItem lineItem : lineItems) {
                            partKey = lineItem.getPart().getPartID();
                            
                            if (listOfPartsIds.contains(partKey) == false)
                                listOfPartsIds.add(partKey);
                        }
                        
                    }
                    // sorts partId's
                    Collections.sort(listOfPartsIds);
                                        
                    // will store the common PartID's in this List
                    List<Integer> inCommon = 
                            new ArrayList<Integer>();

                    // will store all PartID's (repeated counts only one) in this List
                    List<Integer> totalUniquePartsID = 
                            new ArrayList<Integer>();    
                                    
                    int indexQueryList = 0;
                    int indexCustoList = 0;
                                    
                    // iterates until the end of the shortest list is reached
                    while(indexQueryList < queryListOfPartsIds.size() && 
                          indexCustoList < listOfPartsIds.size()){
                        
                        // if the value in the current entry in Query List is greater 
                        // than the one in the Customer List, this is a unique partId
                        if (queryListOfPartsIds.get(indexQueryList).intValue() > listOfPartsIds.get(indexCustoList).intValue()){
                            
                            totalUniquePartsID.add(listOfPartsIds.get(indexCustoList));
                            // move index in Customer List to the next entry
                            indexCustoList++;
                            
                        } else {
                            // if both values in the current Query List and Customer List 
                            // are equal, this is a common partId                    
                            if (queryListOfPartsIds.get(indexQueryList).intValue() == listOfPartsIds.get(indexCustoList).intValue()){
                                
                                inCommon.add(queryListOfPartsIds.get(indexQueryList));
                                // but a common part is also unique, so put it in the 
                                // corresponding list
                                totalUniquePartsID.add(queryListOfPartsIds.get(indexQueryList));
                                
                                // move index in both Lists to the next entry
                                indexCustoList++;
                                indexQueryList++;
                                
                            } else {
                                // if the value in the current Query List is less than the 
                                // one in the Customer List, this is not a common part 
                                totalUniquePartsID.add(queryListOfPartsIds.get(indexQueryList));
                                // move index in Query List to the next entry
                                indexQueryList++;    
                                
                            }                    
                        }                
                    }        
                    
                    // we will iterate from the last index in the shortest List
                    // until the end of the largest List, all entries are unique
                    // partID's
                    if (queryListOfPartsIds.size() > listOfPartsIds.size()){
                        
                        for (int i=indexQueryList; i< queryListOfPartsIds.size(); i++)
                            totalUniquePartsID.add(queryListOfPartsIds.get(i));
                        
                    } else{
                        
                        for (int i=indexCustoList; i< listOfPartsIds.size(); i++)
                            totalUniquePartsID.add(listOfPartsIds.get(i));
                        
                    }                        

                    Double similarityValue = new Double(0.0);

                    // Prevents divided by zero errors
                    if (totalUniquePartsID.size()!=0)
                        similarityValue = new Double((double)inCommon.size() / (double)totalUniquePartsID.size());

                    // adds the Customer.key along with part ID's from this customer
                    // that are common with the query list
                    Tuple2<Integer, List<Integer>> innerTuple = 
                            new Tuple2<Integer, List<Integer>>(customerKey, inCommon);
                    // adds the similarity score
                    Tuple2<Double, Tuple2<Integer, List<Integer>>> outerTuple = 
                            new Tuple2<Double, Tuple2<Integer, List<Integer>>>(similarityValue, innerTuple);
                                                            
                    return outerTuple;
                }
            });                
                
        // Comparator class for comparing similarity results, to be used in top() and
        // prevents kryo serialization error by implementing Serializable
        class TupleComparator implements Comparator<Tuple2<Double, Tuple2<Integer, List<Integer>>>>, Serializable {

            private static final long serialVersionUID = 1972211969496211511L;

            @Override
            public int compare(Tuple2<Double, Tuple2<Integer,List<Integer>>> score_1, 
                               Tuple2<Double, Tuple2<Integer,List<Integer>>> score_2) {
                return Double.compare(score_1._1, score_2._1);
            }
        }    

        // Return the top 10 entries in the RDD, the key is the 
        // similarity score.        
        List<Tuple2<Double, Tuple2<Integer, List<Integer>>>> topKResults = 
                jaccardSimilarityScore.top(topKValue, new TupleComparator ());
        
        System.out.println("Total after topK " + topKValue + " is: " +topKResults.size());
                
        // print the topK entries
        for (Tuple2<Double, Tuple2<Integer, List<Integer>>>  resultItem : topKResults) {
            System.out.println("Score is: [" + resultItem._1.toString() + 
                               "] Customer key: ["+ resultItem._2._1 + 
                               "] Part keys: " + resultItem._2._2);    
        }
                        
        // Stop the timer
        finalTimestamp = System.nanoTime();

        // Calculate elapsed times
        // time to load data from hdfs into RDD
        loadRDDTime = (startQueryTimestamp - startTime) / 1000000000.0;
        // reads file from HDFS time
        readsHDFSTime = (readFileTime - startTime) / 1000000000.0;
        // query time including loading RDD into memory
        countTime = (startQueryTimestamp - countTimestamp) / 1000000000.0;
        // query time not including loading RDD into memory
        queryTime = (finalTimestamp - startQueryTimestamp) / 1000000000.0;
        // total elapsed time
        elapsedTotalTime = (finalTimestamp - startTime) / 1000000000.0;

        // print out the final results
        if (warmCache == 1)
            System.out.println("Result Query 1:\nDataset Factor: " + NUMBER_OF_COPIES + "\ntopKValue: " + topKValue + "\nNum Cust: " + numberOfCustomers
                    + "\nReads HDFS time: " + readsHDFSTime + "\nLoad RDD time: " + String.format("%.9f", loadRDDTime)
                    + "\nTime to count: " + String.format("%.9f", countTime) + "\nQuery time: " + String.format("%.9f", queryTime) + "\nTotal time: "
                    + String.format("%.9f", elapsedTotalTime) + "\n");
        else
            System.out.println("Result Query 1:\nDataset Factor: " + NUMBER_OF_COPIES + "\ntopKValue: " + topKValue + "\nNum Cust: " + numberOfCustomers
                    + "\nReads HDFS time: " + readsHDFSTime + "\nLoad RDD time: " + String.format("%.9f", loadRDDTime)
                    + "\nQuery time: " + String.format("%.9f", queryTime) + "\nTotal time: " + String.format("%.9f", elapsedTotalTime) + "\n");

        // Finally stop the Spark context once all is completed
        sc.stop();

    }
    
}


