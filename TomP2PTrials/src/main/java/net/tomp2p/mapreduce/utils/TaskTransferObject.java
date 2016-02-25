package net.tomp2p.mapreduce.utils;

import java.util.Map;

public class TaskTransferObject {
	private byte[] taskData;
	private Map<String, byte[]> taskClassFiles;
	private String taskName;

	public TaskTransferObject(byte[] taskData, Map<String, byte[]> taskClassFiles, String taskName) {
		this.taskData = taskData;
		this.taskClassFiles = taskClassFiles;
		this.taskName = taskName;
	}

	public byte[] taskData() {
		return this.taskData;
	}

	public Map<String, byte[]> taskClassFiles() {
		return this.taskClassFiles;
	}

	public String taskName() {
		return this.taskName;
	}
}
