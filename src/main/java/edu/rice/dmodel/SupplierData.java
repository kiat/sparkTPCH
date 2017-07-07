package edu.rice.dmodel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import scala.Serializable;

public class SupplierData implements Serializable {

	private static final long serialVersionUID = 3745599642319311564L;

	private int supplierKey;
	
	// for each customer the supplier sold to, the list of all partIDs sold
	private Map<String, List<Integer>> soldPartIDs;
	
	
	public int getSupplierKey() {
		return supplierKey;
	}

	public void setSupplierKey(int supplierKey) {
		this.supplierKey = supplierKey;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	


	
	public Map<String, List<Integer>> getSoldPartIDs() {
		return soldPartIDs;
	}

	
	public void setSoldPartIDs(Map<String, List<Integer>> soldPartIDs) {
		this.soldPartIDs = soldPartIDs;
	}

	public SupplierData() {
		soldPartIDs = new HashMap<String, List<Integer>>();
	}

	public void addCustomer(String customerName, Integer partID) {

		if (soldPartIDs.containsKey(customerName)) {

			List<Integer> mList = soldPartIDs.get(customerName);
			mList.add(partID);

			soldPartIDs.put(customerName, mList);

		} else {
			List<Integer> mList = new ArrayList<Integer>();
			mList.add(partID);
			soldPartIDs.put(customerName, mList);
		}
	}
	
	
	public String toString(){
		return  "("+supplierKey+", " + soldPartIDs.toString()+")";
	}

	
	public void merge(SupplierData suppData2){
		
//		Map<String, List<Integer>> other_soldPartIDs=suppData2.getSoldPartIDs();
//		
//		Iterator<String> it=other_soldPartIDs.keySet().iterator();
//		
//		while (it.hasNext()) {
//			String key = it.next();
//			
//			List<Integer> tmpIDList;
//
//			if (this.soldPartIDs.containsKey(key)) {
//				// get the List and aggregate PartID to the existing list
//				tmpIDList = soldPartIDs.get(key);
//				tmpIDList.addAll(other_soldPartIDs.get(key));
//			} else {
//				//tmp list is the other list
//				tmpIDList = other_soldPartIDs.get(key);
//			}
//			// Put it back into the list
//			this.soldPartIDs.put(key, tmpIDList);
//		}

		this.soldPartIDs.putAll(suppData2.getSoldPartIDs());
	}
	
	
	
}