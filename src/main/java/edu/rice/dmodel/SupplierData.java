package edu.rice.dmodel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Serializable;

public class SupplierData implements Serializable {

	private static final long serialVersionUID = 3745599642319311564L;

	// for each customer the supplier sold to, the list of all partIDs sold
	private Map<String, List<Integer>> soldPartIDs;

	
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
		return soldPartIDs.toString();
	}
	
	
}