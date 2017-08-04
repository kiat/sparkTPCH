package edu.rice.dmodel;

import scala.Serializable;

public class RawBytes  implements Serializable  {


	private static final long serialVersionUID = 4400128684430455549L;
	
	public RawBytes(byte[] data) {
		super();
		this.data = data;
	}

	private byte[] data;


	public byte[] getData() {
		return data;
	}


	public void setData(byte[] data) {
		this.data = data;
	}

}
