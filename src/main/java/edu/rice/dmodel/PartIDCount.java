package edu.rice.dmodel;

import scala.Serializable;

public class PartIDCount implements Serializable, Comparable<PartIDCount> {

	public PartIDCount(int partID, int count) {
		this.partID = partID;
		this.count = count;
	}

	private static final long serialVersionUID = 7539680442818555261L;

	private int partID;
	private int count;

	public int getPartID() {
		return partID;
	}

	public void setPartID(int partID) {
		this.partID = partID;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public int compareTo(PartIDCount other) {
		return Integer.compare(this.count, other.count);
	}
	
	public String toString(){
		return "("+this.partID+","+this.count+") "; 
	}
}