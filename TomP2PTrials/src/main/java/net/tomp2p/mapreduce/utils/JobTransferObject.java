package net.tomp2p.mapreduce.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import net.tomp2p.peers.Number640;

public class JobTransferObject implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6702141212731486921L;
	private List<TransferObject> taskTransferObjects = new ArrayList<>();
 	private Number640 id;

	public void addTask(TransferObject tto) {
		this.taskTransferObjects.add(tto);
	}

	public List<TransferObject> taskTransferObjects() {
		return taskTransferObjects;
	}

	public void id(Number640 id) {
		this.id = id;

	}

	public Number640 id() {
		return id;
	}
 

}
