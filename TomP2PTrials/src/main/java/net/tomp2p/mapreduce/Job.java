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

import mapreduce.engine.multithreading.TaskTransferExecutor;
import net.tomp2p.mapreduce.utils.JobTransferObject;
import net.tomp2p.mapreduce.utils.SerializeUtils;
import net.tomp2p.mapreduce.utils.TaskTransferObject;
import net.tomp2p.peers.Number640;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Utils;

/**
 *
 * @author draft
 */
final public class Job implements Serializable {

	private Number640 jobId;
	private List<Task> tasks;
	private ObjectDataReply objectDataReply;

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

	public JobTransferObject serialize() throws IOException {
		JobTransferObject jTO = new JobTransferObject();
		for (Task task : tasks) {
			Map<String, byte[]> taskClassFiles = SerializeUtils.serialize(task.getClass());
			byte[] taskData = Utils.encodeJavaObject(task);
			TaskTransferObject tto = new TaskTransferObject(taskData, taskClassFiles, task.getClass().getName());
			jTO.addTask(tto);
		}
		jTO.serializedReply(SerializeUtils.serialize(this.objectDataReply.getClass()));
		return jTO;
	}

	public static Job deserialize(JobTransferObject jobToDeserialize) {
		List<Task> tmp = new ArrayList<>();
		for (TaskTransferObject task : jobToDeserialize.taskTransferObjects()) {
			tmp.add(SerializeUtils.deserialize(task.taskClassFiles(), classToInstantiate));
		}
		this.tasks = tmp;
		this.objectDataReply = (ObjectDataReply) SerializeUtils.deserialize(serializedObjectDataReply,
				objectDataReply.getClass().getName());
		return this;
	}
}
