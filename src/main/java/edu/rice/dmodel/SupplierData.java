package edu.rice.dmodel;

import java.util.List;
import java.util.Map;

import scala.Serializable;


public class SupplierData implements Serializable   {

	// the name of a particular supplier
	private String supplierNames;

	// for each customer the supplier sold to, the list of all partIDs sold
	Map <String, List <Integer>> soldPartIDs; 
}
