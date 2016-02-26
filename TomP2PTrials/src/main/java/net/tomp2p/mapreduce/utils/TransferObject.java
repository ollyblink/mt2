package net.tomp2p.mapreduce.utils;

import java.io.Serializable;
import java.util.Map;

public class TransferObject implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 8971732001157216939L;
	private byte[] data;
	private Map<String, byte[]> classFiles;
	private String className;

	public TransferObject(byte[] data, Map<String, byte[]> classFiles, String className) {
		this.data = data;
		this.classFiles = classFiles;
		this.className = className;
	}

	public byte[] data() {
		return this.data;
	}

	public Map<String, byte[]> classFiles() {
		return this.classFiles;
	}

	public String className() {
		return this.className;
	}
}
