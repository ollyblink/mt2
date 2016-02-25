package net.tomp2p.mapreduce.utils;

import java.util.List;
import java.util.Map;

public class JobTransferObject {
	private List<TaskTransferObject> taskTransferObjects;
	private Map<String, byte[]> serializedReply;

	public void addTask(TaskTransferObject tto) {
		this.taskTransferObjects.add(tto);
	}

	public void serializedReply(Map<String, byte[]> serializedReply) {
		this.serializedReply = serializedReply;
	}

	public List<TaskTransferObject> taskTransferObjects() {
		return taskTransferObjects;
	}

	public Map<String, byte[]> serializedReply() {
		return this.serializedReply;
	}
}
