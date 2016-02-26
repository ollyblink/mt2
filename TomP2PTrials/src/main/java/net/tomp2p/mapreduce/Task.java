/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.tomp2p.mapreduce;

import java.io.Serializable;
import java.util.NavigableMap;

import mapreduce.storage.DHTConnectionProvider;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

/**
 *
 * @author draft
 */
public abstract class Task implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9198452155865807410L;
	private final Number640 previousId;
	private final Number640 currentId;

	public Task(Number640 previousId, Number640 currentId) {
		this.previousId = previousId;
		this.currentId = currentId;
	}

	public abstract void broadcastReceiver(NavigableMap<Number640, Data> input, DHTConnectionProvider dht)
			throws Exception;

	// public Task previousId(Number640 previousId) {
	// this.previousId = previousId;
	// return this;
	// }
	//
	// public Task currentId(Number640 currentId) {
	// this.currentId = currentId;
	// return this;
	// }
	//
	public Number640 currentId() {
		return this.currentId;
	}

	public Number640 previousId() {
		return this.previousId;
	}

	// public Map<String, byte[]> serialize() throws IOException {
	// return SerializeUtils.serialize(this.getClass());
	//
	// }

	// public Task deserialize() {
	// Task task = (Task)SerializeUtils.deserialize(this.serializedTask,
	// this.getClass().getName());
	// //sanity check: previous can be null, current has to be set
	// return task;
	// }
}
