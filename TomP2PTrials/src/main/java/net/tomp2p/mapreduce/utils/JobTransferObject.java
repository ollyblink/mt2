package net.tomp2p.mapreduce.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JobTransferObject implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6702141212731486921L;
	private List<TransferObject> taskTransferObjects = new ArrayList<>();
	private TransferObject serializedReply; // first arg is

	public void addTask(TransferObject tto) {
		this.taskTransferObjects.add(tto);
	}

	public void serializedReply(Map<String, byte[]> serializedReplyClassFiles, byte[] serializedReplyData,
			String replyName) {
		this.serializedReply = new TransferObject(serializedReplyData, serializedReplyClassFiles, replyName);

	}

	public List<TransferObject> taskTransferObjects() {
		return taskTransferObjects;
	}

	public TransferObject serializedReplyTransferObject() {
		return this.serializedReply;
	}
}
