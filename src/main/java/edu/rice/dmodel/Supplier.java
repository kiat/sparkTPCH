package edu.rice.dmodel;

import scala.Serializable;


public class Supplier  implements Serializable  {

	/**
	 * 
	 */
	private static final long serialVersionUID = -492779361042916008L;
	/**
	 * This class represents a supplier as it is specified in TPC-H benchmark
	 * schema
	 */

	// CREATE TABLE [dbo].[SUPPLIER](
	// [S_SUPPKEY] [int] NOT NULL,
	// [S_NAME] [char](25) NOT NULL,
	// [S_ADDRESS] [varchar](40) NOT NULL,
	// [S_NATIONKEY] [int] NOT NULL,
	// [S_PHONE] [char](15) NOT NULL,
	// [S_ACCTBAL] [decimal](15, 2) NOT NULL,
	// [S_COMMENT] [varchar](101) NOT NULL
	// );

	private int supplierKey;
	private String name;
	private String address;
	private int nationKey;
	private String phone;
	private double accbal;
	private String comment;

	public Supplier() {

	}

	public Supplier(int supplierKey, String name, String address, int nationKey, String phone, double accbal, String comment) {
		super();
		this.supplierKey = supplierKey;
		this.name = name;
		this.address = address;
		this.nationKey = nationKey;
		this.phone = phone;
		this.accbal = accbal;
		this.comment = comment;
	}
	
	public Supplier generateSupplier() {
		int i = 0;
		return	new Supplier( i, "name" + i, "address" + i, i, "phone" + i, i, "comment"+ i);
		
	}


	public int getSupplierKey() {
		return supplierKey;
	}

	public void setSupplierKey(int supplierKey) {
		this.supplierKey = supplierKey;
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

	public int getNationKey() {
		return nationKey;
	}

	public void setNationKey(int nationKey) {
		this.nationKey = nationKey;
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

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public String toString() {

		String s = this.supplierKey + " -" + this.name + " -" + this.address + " -" + this.nationKey + " -" + this.phone + " -"
				+ this.accbal + " -" + this.comment;
		return s;
	}
}