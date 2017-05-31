package edu.rice.dmodel;

import java.io.Serializable;

public class SupplierCustomerPartID implements Serializable {

	private static final long serialVersionUID = 4253185616069063274L;
	private String customerName;
	private String supplierName;
	private int partID;

	public SupplierCustomerPartID() {
	}

	public SupplierCustomerPartID(String customerName, String supplierName, int partID) {
		super();
		this.customerName = customerName;
		this.supplierName = supplierName;
		this.partID = partID;
	}

	public String getCustomerName() {
		return customerName;
	}

	public void setCustomerName(String customerName) {
		this.customerName = customerName;
	}

	public String getSupplierName() {
		return supplierName;
	}

	public void setSupplierName(String supplierName) {
		this.supplierName = supplierName;
	}

	public int getPartID() {
		return partID;
	}

	public void setPartID(int partID) {
		this.partID = partID;
	}
}