/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.tomp2p.mapreduce;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.tomp2p.mapreduce.utils.SerializeUtils;
import net.tomp2p.peers.Number640;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Utils;

/**
 *
 * @author draft
 */
final public class Job implements Serializable {
	
	private class InnerTestClass implements Serializable {

	}

	private Number640 jobId;
	private List<Task> tasks;
	private ObjectDataReply objectDataReply;
	private  serializedObjectDataReply;

	public void addTask(Task task) {
		this.tasks.add(task);
	}

	public Task findTask(Task task) {
		for (Task t : tasks) {
			if (t.equals(task)) {
				return t;
			}
		}
		return null;
	}

	

	// addTask
	// jobId
	// objcetDatareply

	public Map<String, Map<String, Pair<byte[], byte[]>>> serialize() throws IOException {
		List<Map<String, byte[]>> list = new ArrayList<>();
		for (Task task : tasks) {
			Map<String, byte[]> tmp = task.serialize();
			byte[] tmp = Utils.encodeJavaObject(task);
			list.add(tmp);
		}
		this.serializedObjectDataReply = SerializeUtils.serialize(this.objectDataReply.getClass());
		return list;
	}

	public static Job deserialize(List<Map<String, byte[]>> list) {
		List<Task> tmp = new ArrayList<>();
		for (Task task : tasks) {
			tmp.add(task.deserialize());
		}
		this.tasks = tmp;
		this.objectDataReply = (ObjectDataReply) SerializeUtils.deserialize(serializedObjectDataReply,
				objectDataReply.getClass().getName());
		return this;
	}
}
