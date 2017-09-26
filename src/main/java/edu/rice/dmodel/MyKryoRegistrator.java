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
		kryo.register(Object[].class, 1);
		kryo.register(scala.Tuple2[].class, 2);

		kryo.register(java.util.ArrayList.class, 3);
		kryo.register(java.util.HashMap.class, 4);
		kryo.register(java.util.HashSet.class, 5);

		// register my classes. 
		kryo.register(edu.rice.dmodel.Part.class, 6);
		kryo.register(edu.rice.dmodel.Supplier.class, 7);
		kryo.register(edu.rice.dmodel.Order.class, 8);
		kryo.register(edu.rice.dmodel.LineItem.class, 9);
		kryo.register(edu.rice.dmodel.Customer.class, 10);
		
		kryo.register(edu.rice.dmodel.SupplierData.class, 11);
		kryo.register(edu.rice.dmodel.PartIDCount.class, 12);
		kryo.register(edu.rice.dmodel.Wrapper.class, 13);

		kryo.register(byte[][].class, 14);
		kryo.register(int[].class, 15);
		kryo.register(Integer[].class, 16);


		doRegistration(kryo, "scala.collection.mutable.WrappedArray$ofRef");
		doRegistration(kryo, "scala.math.LowPriorityOrderingImplicits$$anon$7");
		doRegistration(kryo, "org.spark_project.guava.collect.NaturalOrdering");
		
		//Jaccard classes
		doRegistration(kryo, "edu.rice.exp.spark_exp.JaccardSimilarityQuery");
		doRegistration(kryo, "edu.rice.exp.spark_exp.JaccardSimilarityQuery$1TupleComparator");
		doRegistration(kryo, "scala.math.Ordering");
		doRegistration(kryo, "org.apache.spark.rdd.RDD");
		doRegistration(kryo, "java.util.Comparator");		

		//  java.lang.IllegalArgumentException: Class is not registered: scala.math.LowPriorityOrderingImplicits$$anon$7
		
		doRegistration(kryo, "org.apache.spark.sql.catalyst.InternalRow");
		doRegistration(kryo, "org.apache.spark.sql.catalyst.expressions.UnsafeRow");
		
		
		

		
		
		// doRegistration(kryo,
		// "org.systemsbiology.xtandem.scoring.VariableStatistics");
		// doRegistration(kryo,
		// "org.systemsbiology.xtandem.scoring.SpectralPeakUsage$PeakUsage");
		// // and many more similar nines

		// kryo.register(Part.class, new FieldSerializer(kryo, Part.class));
		// kryo.register(Supplier.class, new FieldSerializer(kryo,
		// Supplier.class));
		// kryo.register(Order.class, new FieldSerializer(kryo, Order.class));
		// kryo.register(LineItem.class, new FieldSerializer(kryo,
		// LineItem.class));
		// kryo.register(Customer.class, new FieldSerializer(kryo,
		// Customer.class));
		// kryo.register(SupplierData.class, new FieldSerializer(kryo,
		// SupplierData.class));

	}

};