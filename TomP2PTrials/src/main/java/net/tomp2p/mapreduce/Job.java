/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.tomp2p.mapreduce;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.tomp2p.mapreduce.utils.SerializeUtils;
import net.tomp2p.peers.Number640;

/**
 *
 * @author draft
 */
public class Job implements Serializable {

	private Number640 jobId;
	private List<Task> tasks;
	private Map<String, byte[]> serializedJob;

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

	public Job serialize() {
		for (Task task : tasks) {
			task.serialize();
		}
		this.serializedJob = SerializeUtils.serialize(this.getClass());
		return this;
	}

	public Job deserialize() {
		List<Task> tmp = new ArrayList<>();
		for (Task task : tasks) {
			tmp.add(task.deserialize());
		}
		this.tasks = tmp;
		return this;
	}
}
