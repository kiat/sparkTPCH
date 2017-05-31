package edu.rice.dmodel;

import scala.Serializable;

public class TupleCustomerNameLineItem implements Serializable {

	/**
	 * This class represents a tuple of CustomerName and LineItem
	 */

	private static final long serialVersionUID = -1127231065050226045L;
	private String customerName;
	private LineItem lineItem;

	public TupleCustomerNameLineItem() {
	}

	public String getCustomerName() {
		return customerName;
	}

	public TupleCustomerNameLineItem(String customerName, LineItem lineItem) {
		super();
		this.customerName = customerName;
		this.lineItem = lineItem;
	}

	public void setCustomerName(String customerName) {
		this.customerName = customerName;
	}

	public LineItem getLineItem() {
		return lineItem;
	}

	public void setLineItem(LineItem lineItem) {
		this.lineItem = lineItem;
	}
}