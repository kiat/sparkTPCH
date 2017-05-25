package edu.rice.dmodel;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import scala.Serializable;

//CREATE TABLE PART  ( P_PARTKEY     INTEGER NOT NULL,
//        P_NAME        VARCHAR(55) NOT NULL,
//        P_MFGR        CHAR(25) NOT NULL,
//        P_BRAND       CHAR(10) NOT NULL,
//        P_TYPE        VARCHAR(25) NOT NULL,
//        P_SIZE        INTEGER NOT NULL,
//        P_CONTAINER   CHAR(10) NOT NULL,
//        P_RETAILPRICE DECIMAL(15,2) NOT NULL,
//        P_COMMENT     VARCHAR(23) NOT NULL );

public class Part implements Serializable  {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8233453288152642686L;

	static Logger logger = Logger.getLogger(Part.class);

	private int partID;
	private String name;
	private String mfgr;
	private String brand;
	private String type;
	private int size;
	private String container;
	private double retailPrice;
	private String comment;

	public Part() {
	}

	public Part(int partID, String name, String mfgr, String brand, String type, int size, String container, double retailPrice, String comment) {
		this.partID = partID;
		this.name = name;
		this.mfgr = mfgr;
		this.brand = brand;
		this.type = type;
		this.size = size;
		this.container = container;
		this.retailPrice = retailPrice;
		this.comment = comment;

	}

	
	public Part generatePart() {
		int i = 0;
		return new Part(i, "Name-" + i, "MFGR-" + i, "Brand-" + i, "Type-" + i, i, "Container-" + i, i, "Comment-" + i);
	}

	public ArrayList<Part> generateParts(int number) {

		ArrayList<Part> objectList = new ArrayList<Part>();

		for (int i = 0; i < number; i++) {
			Part tmp = new Part(i, "Name-" + i, "MFGR-" + i, "Brand-" + i, "Type-" + i, i, "Container-" + i, i, "Comment-" + i);
			objectList.add(tmp);
		}

		return objectList;
	}

	public Part javaDefaultDeserialization(byte[] buf) {

		ByteArrayInputStream b = new ByteArrayInputStream(buf);
		ObjectInputStream objectInputStream;

		Part p = null;

		try {
			objectInputStream = new ObjectInputStream(b);
			p = (Part) objectInputStream.readObject();

		} catch (IOException | ClassNotFoundException e) {
			logger.error("Can read object Part from byteArray", e);
		}
		return p;

	}


	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getBrand() {
		return brand;
	}

	public void setBrand(String brand) {
		this.brand = brand;
	}

	public String getMfgr() {
		return mfgr;
	}

	public void setMfgr(String mfgr) {
		this.mfgr = mfgr;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public String getContainer() {
		return container;
	}

	public void setContainer(String container) {
		this.container = container;
	}

	public double getRetailPrice() {
		return retailPrice;
	}

	public void setRetailPrice(double retailPrice) {
		this.retailPrice = retailPrice;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public String toString() {
		String stringS = this.getPartID() + " - "  + this.getName() + " - " + this.getBrand() + " - " + this.getComment() + " - " + this.getContainer() + " - " + this.getMfgr() + " - " + this.getRetailPrice() + " - " + this.getSize();
		return stringS;
	}

	public int getPartID() {
		return partID;
	}

	public void setPartID(int partID) {
		this.partID = partID;
	}

	
}
