package net.tomp2p.mapreduce.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class JobTransferObject implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6702141212731486921L;
	private List<TransferObject> taskTransferObjects = new ArrayList<>();
//	private List<TransferObject> mapReduceBroadcastReceiverTransferObjects = new ArrayList<>();

	public void addTask(TransferObject tto) {
		this.taskTransferObjects.add(tto);
	}

	public List<TransferObject> taskTransferObjects() {
		return taskTransferObjects;
	}

//	public void addBroadcastReceiver(TransferObject t) {
//		this.mapReduceBroadcastReceiverTransferObjects.add(t);
//	}
//
//	public List<TransferObject> mapReduceBroadcastReceiverTransferObjects() {
//		return this.mapReduceBroadcastReceiverTransferObjects;
//	}

}
