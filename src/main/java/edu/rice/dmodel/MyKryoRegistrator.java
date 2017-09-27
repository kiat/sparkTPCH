package edu.rice.dmodel;

import java.io.Serializable;
import java.lang.reflect.Array;

import javax.annotation.Nonnull;

import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;

// Based on http://lordjoesoftware.blogspot.com/2015/02/using-kryoserializer-in-spark.html
public class MyKryoRegistrator implements KryoRegistrator, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7700960882306273668L;

	public MyKryoRegistrator() {
	}

	/**
	 * register a class indicated by name
	 *
	 * @param kryo
	 * @param s
	 *            name of a class - might not exist
	 * @param handled
	 *            Set of classes already handles
	 */
	protected void doRegistration(@Nonnull Kryo kryo, @Nonnull String s) {
		Class c;
		try {
			c = Class.forName(s);
			doRegistration(kryo, c);
		} catch (ClassNotFoundException e) {
			return;
		}
	}

	/**
	 * register a class
	 *
	 * @param kryo
	 * @param s
	 *            name of a class - might not exist
	 * @param handled
	 *            Set of classes already handles
	 */
	protected void doRegistration(final Kryo kryo, final Class pC) {
		if (kryo != null) {
			kryo.register(pC);
			// also register arrays of that class
			Class arrayType = Array.newInstance(pC, 0).getClass();
			kryo.register(arrayType);
		}
	}

	/**
	 * do the real work of registering all classes
	 * 
	 * @param kryo
	 */
	@Override
	public void registerClasses(@Nonnull Kryo kryo) {
		// the order in which these classes are registered matters, if
		// you need to add more classes, add them at the end of the list!!!
		// by default kryo assigns the next int ID available when registering
		// classes.
		kryo.register(Object[].class);
		kryo.register(scala.Tuple2[].class);

		kryo.register(java.util.ArrayList.class);
		kryo.register(java.util.HashMap.class);
		kryo.register(java.util.HashSet.class);

		// register my classes. 
		kryo.register(edu.rice.dmodel.Part.class);
		kryo.register(edu.rice.dmodel.Supplier.class);
		kryo.register(edu.rice.dmodel.Order.class);
		kryo.register(edu.rice.dmodel.LineItem.class);
		kryo.register(edu.rice.dmodel.Customer.class);
		
		kryo.register(edu.rice.dmodel.SupplierData.class);
		kryo.register(edu.rice.dmodel.PartIDCount.class);
		kryo.register(edu.rice.dmodel.Wrapper.class);

		kryo.register(byte[][].class);
		kryo.register(int[].class);
		kryo.register(Integer[].class);


		doRegistration(kryo, "scala.collection.mutable.WrappedArray$ofRef");
		doRegistration(kryo, "scala.math.LowPriorityOrderingImplicits$$anon$7");
		doRegistration(kryo, "org.spark_project.guava.collect.NaturalOrdering");
		
		//Jaccard classes
		doRegistration(kryo, "edu.rice.exp.spark_exp.JaccardSimilarityQuery");
		doRegistration(kryo, "edu.rice.exp.spark_exp.JaccardSimilarityQuery$1TupleComparator");
		doRegistration(kryo, "scala.math.Ordering");
		doRegistration(kryo, "org.apache.spark.rdd.RDD");
		doRegistration(kryo, "java.util.Comparator");		
		
		doRegistration(kryo, "org.apache.spark.sql.catalyst.InternalRow");
		doRegistration(kryo, "org.apache.spark.sql.catalyst.expressions.UnsafeRow");
		
		// register new classes beyond this point, the order matters!!!		
		doRegistration(kryo, "edu.rice.exp.spark_exp.JaccardSimilaritySimple$1TupleComparator.class");		

	}

};