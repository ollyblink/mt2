/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.tomp2p.mapreduce;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import net.tomp2p.mapreduce.utils.SerializeUtils;
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
	private Map<String, byte[]> serializedTask;
	Number640 previousId;
	Number640 currentId;

	public Task(Number640 previousId, Number640 currentId) {
		this.previousId = previousId;
		this.currentId = currentId;
	}

	public abstract void broadcastReceiver(NavigableMap<Number640, Data> input) throws Exception;

	public Task previousId(Number640 previousId) {
		this.previousId = previousId;
		return this;
	}

	public Task currentId(Number640 currentId) {
		this.currentId = currentId;
		return this;
	}

	public Number640 currentId() {
		return this.currentId;
	}

	public Number640 previousId() {
		return this.previousId;
	}

	public Task serialize() {
		this.serializedTask = SerializeUtils.serialize(this.getClass());
		return this;
	}

	public Task deserialize() {
		Map<String, Class<?>> classes = SerializeUtils.deserialize(this.serializedTask);
		Task task = null;
		for (String className : classes.keySet()) { 
			try {
				Class<?> class1 = classes.get(className);
				if (class1.isInstance(Task.class)) {
					task = (Task) Class.forName(className).newInstance();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return task;
	}
}
