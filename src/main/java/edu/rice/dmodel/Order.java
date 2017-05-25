package edu.rice.dmodel;

import java.util.List;

import scala.Serializable;

public class Order implements Serializable  {

	// CREATE TABLE ORDERS ( O_ORDERKEY INTEGER NOT NULL,
	// O_CUSTKEY INTEGER NOT NULL,
	// O_ORDERSTATUS CHAR(1) NOT NULL,
	// O_TOTALPRICE DECIMAL(15,2) NOT NULL,
	// O_ORDERDATE DATE NOT NULL,
	// O_ORDERPRIORITY CHAR(15) NOT NULL,
	// O_CLERK CHAR(15) NOT NULL,
	// O_SHIPPRIORITY INTEGER NOT NULL,
	// O_COMMENT VARCHAR(79) NOT NULL);

	// private int orderkey;
	// private int custkey;

	/**
	 * 
	 */
	private static final long serialVersionUID = 4658902771232417539L;

	private List<LineItem> lineItems;

	private int orderkey;
	private int custkey;
	private String orderstatus;
	private double totalprice;
	private String orderdate;
	private String orderpriority;
	private String clerk;
	private int shippriority;
	private String comment;

	public Order() {

	}

	public Order(List<LineItem> lineItems, int orderkey, int custkey, String orderstatus, double totalprice, String orderdate, String orderpriority, String clerk,
			int shippriority, String comment) {
		super();

		this.lineItems = lineItems;
		this.orderkey = orderkey;
		this.custkey = custkey;
		this.orderstatus = orderstatus;
		this.totalprice = totalprice;
		this.orderdate = orderdate;
		this.orderpriority = orderpriority;
		this.clerk = clerk;
		this.shippriority = shippriority;
		this.comment = comment;
	}

	public int getOrderkey() {
		return orderkey;
	}

	public void setOrderkey(int orderkey) {
		this.orderkey = orderkey;
	}

	public int getCustkey() {
		return custkey;
	}

	public void setCustkey(int custkey) {
		this.custkey = custkey;
	}

	public double getTotalprice() {
		return totalprice;
	}

	public void setTotalprice(double totalprice) {
		this.totalprice = totalprice;
	}

	public String getOrderstatus() {
		return orderstatus;
	}

	public void setOrderstatus(String orderstatus) {
		this.orderstatus = orderstatus;
	}

	public String getOrderdate() {
		return orderdate;
	}

	public void setOrderdate(String orderdate) {
		this.orderdate = orderdate;
	}

	public String getOrderpriority() {
		return orderpriority;
	}

	public void setOrderpriority(String orderpriority) {
		this.orderpriority = orderpriority;
	}

	public String getClerk() {
		return clerk;
	}

	public void setClerk(String clerk) {
		this.clerk = clerk;
	}

	public int getShippriority() {
		return shippriority;
	}

	public void setShippriority(int shippriority) {
		this.shippriority = shippriority;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public List<LineItem> getLineItems() {
		return lineItems;
	}

	public void setLineItems(List<LineItem> lineItems) {
		this.lineItems = lineItems;
	}

}
