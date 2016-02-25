package net.tomp2p.mapreduce.utils;

import java.util.Map;

public class TransferObject {
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
