package edu.rice.dmodel;

import java.util.List;

import scala.Serializable;

/**
 * This is just a wrapper class to store customerID, list of partIDs, and the score of this to a specific query when we calculate the similarities.
 * 
 * @author Kia
 *
 */
public class Wrapper implements Serializable, Comparable<Wrapper> {

	private static final long serialVersionUID = 3286441147625454741L;

	private Integer customerID;
	private List<Integer> partIDs;
	private double score;

	public Wrapper() {
		super();
	}

	public Wrapper(Integer customerID, List<Integer> partIDs, double score ) {
		super();
		this.customerID = customerID;
		this.partIDs = partIDs;
		this.score=score;
	}

	public Integer getCustomerID() {
		return customerID;
	}

	public void setCustomerID(Integer customerID) {
		this.customerID = customerID;
	}

	public List<Integer> getPartIDs() {
		return partIDs;
	}

	public void setPartIDs(List<Integer> partIDs) {
		this.partIDs = partIDs;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	@Override
	public int compareTo(Wrapper o) {
		if (this.getScore() < o.getScore())
			return -1;
		if (this.getScore() > o.getScore())
			return 1;
		return 0;
	}

	/**
	 * A simple toString method to see what is going on. 	
	 */
	public String toString() {

		String myString = "";
		myString = "Score is: [" + this.score + " ] [ " + this.customerID + " ], List: " + this.partIDs + " ]";
		return myString;
	}

}