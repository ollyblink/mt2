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
import net.tomp2p.mapreduce.utils.TransferObject;
import net.tomp2p.peers.Number640;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Utils;

/**
 *
 * @author draft
 */
final public class Job {

	private Number640 jobId;
	private List<Task> tasks;
	private ObjectDataReply objectDataReply;

	public void addTask(Task task) {
		this.tasks.add(task);
	}

	private void objectDataReply(ObjectDataReply objectDataReply) {
		this.objectDataReply = objectDataReply;
	}

	public JobTransferObject serialize() throws IOException {
		JobTransferObject jTO = new JobTransferObject();
		for (Task task : tasks) {
			Map<String, byte[]> taskClassFiles = SerializeUtils.serialize(task.getClass());
			byte[] taskData = Utils.encodeJavaObject(task);
			TransferObject tto = new TransferObject(taskData, taskClassFiles, task.getClass().getName());
			jTO.addTask(tto);
		}
		jTO.serializedReply(SerializeUtils.serialize(this.objectDataReply.getClass()),
				Utils.encodeJavaObject(this.objectDataReply), this.objectDataReply.getClass().getName());
		return jTO;
	}

	public static Job deserialize(JobTransferObject jobToDeserialize) throws ClassNotFoundException, IOException {
		Job job = new Job();
		for (TransferObject taskTransferObject : jobToDeserialize.taskTransferObjects()) {
			SerializeUtils.deserialize(taskTransferObject.classFiles());
			Task task = (Task) Utils.decodeJavaObject(taskTransferObject.data(), 0, taskTransferObject.data().length);
			job.addTask(task);
		}
		TransferObject odrT = jobToDeserialize.serializedReplyTransferObject();
		SerializeUtils.deserialize(odrT.classFiles());
		ObjectDataReply odr = (ObjectDataReply) Utils.decodeJavaObject(odrT.data(), 0, odrT.data().length);
		job.objectDataReply(odr);
		return job;
	}

}