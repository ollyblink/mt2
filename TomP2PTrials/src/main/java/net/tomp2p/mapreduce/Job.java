/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.tomp2p.mapreduce;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.tomp2p.mapreduce.utils.SerializeUtils;
import net.tomp2p.peers.Number640;
import net.tomp2p.rpc.ObjectDataReply;

/**
 *
 * @author draft
 */
public class Job implements Serializable {
	
	private class InnerTestClass implements Serializable {

	}

	private Number640 jobId;
	private List<Task> tasks;
	private ObjectDataReply objectDataReply;
	private Map<String, byte[]> serializedObjectDataReply;

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

	public Number640 jobId() {
		return this.jobId;
	}

	// addTask
	// jobId
	// objcetDatareply

	public Job serialize() throws IOException {
		for (Task task : tasks) {
			task.serialize();
		}
		this.serializedObjectDataReply = SerializeUtils.serialize(this.objectDataReply.getClass());
		Runnable r = new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				
			}
		};
		r.run();

		return this;
	}

	public Job deserialize() {
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
