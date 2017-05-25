package edu.rice.dmodel;

import java.util.List;

import scala.Serializable;

public class Customer implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5361303693082699460L;

	public Customer(List<Order> orders, int custkey, String name, String address, int nationkey, String phone, double accbal, String mktsegment, String comment) {
		super();
		this.orders = orders;
		this.custkey = custkey;
		this.name = name;
		this.address = address;
		this.nationkey = nationkey;
		this.phone = phone;
		this.accbal = accbal;
		this.mktsegment = mktsegment;
		this.comment = comment;
	}

	private List<Order> orders;
	private int custkey;
	private String name;
	private String address;
	private int nationkey;
	private String phone;
	private double accbal;
	private String mktsegment;
	private String comment;

	public Customer() {

	}

	public List<Order> getOrders() {
		return orders;
	}

	public void setOrders(List<Order> orders) {
		this.orders = orders;
	}

	public int getCustkey() {
		return custkey;
	}

	public void setCustkey(int custkey) {
		this.custkey = custkey;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public int getNationkey() {
		return nationkey;
	}

	public void setNationkey(int nationkey) {
		this.nationkey = nationkey;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public double getAccbal() {
		return accbal;
	}

	public void setAccbal(double accbal) {
		this.accbal = accbal;
	}

	public String getMktsegment() {
		return mktsegment;
	}

	public void setMktsegment(String mktsegment) {
		this.mktsegment = mktsegment;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}
}