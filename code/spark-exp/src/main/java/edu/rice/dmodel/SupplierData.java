package edu.rice.dmodel;

import java.util.List;

import scala.Serializable;


public class SupplierData implements Serializable   {

	// the name of a particular supplier
	private List<String> supplierNames;

	// for each customer the supplier sold to, the list of all partIDs sold
	private List<Integer> soldPartIDs; 

}
