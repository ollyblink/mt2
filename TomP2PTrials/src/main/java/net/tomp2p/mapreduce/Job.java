/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.tomp2p.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import mapreduce.storage.DHTConnectionProvider;
import net.tomp2p.mapreduce.utils.ByteObjectInputStream;
import net.tomp2p.mapreduce.utils.JobTransferObject;
import net.tomp2p.mapreduce.utils.SerializeUtils;
import net.tomp2p.mapreduce.utils.TransferObject;
import net.tomp2p.peers.Number640;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;

/**
 *
 * @author draft
 */
final public class Job {

	private List<Task> tasks;
	private ObjectDataReply objectDataReply;

	public Job() {
		this.tasks = new ArrayList<>();
	}

	public void addTask(Task task) {
		this.tasks.add(task);
	}

	private void objectDataReply(ObjectDataReply objectDataReply) {
		this.objectDataReply = objectDataReply;
	}

	public JobTransferObject serialize() throws IOException {
		JobTransferObject jTO = new JobTransferObject();
		for (Task task : tasks) {
			Map<String, byte[]> taskClassFiles = SerializeUtils.serializeClassFile(task.getClass());
			byte[] taskData = SerializeUtils.serializeJavaObject(task);
			TransferObject tto = new TransferObject(taskData, taskClassFiles, task.getClass().getName());
			jTO.addTask(tto);
		}
		if (this.objectDataReply != null) {
			jTO.serializedReply(SerializeUtils.serializeClassFile(this.objectDataReply.getClass()), Utils.encodeJavaObject(this.objectDataReply), this.objectDataReply.getClass().getName());
		}
		return jTO;
	}

	public static Job deserialize(JobTransferObject jobToDeserialize) throws ClassNotFoundException, IOException {
		Job job = new Job();
		for (TransferObject taskTransferObject : jobToDeserialize.taskTransferObjects()) {
			Map<String, Class<?>> taskClasses = SerializeUtils.deserializeClassFiles(taskTransferObject.classFiles());
			Task task = (Task) SerializeUtils.deserializeJavaObject(taskTransferObject.data(), taskClasses);
			job.addTask(task);
		}
		TransferObject odrT = jobToDeserialize.serializedReplyTransferObject();
		if (odrT != null) {
			Map<String, Class<?>> odrTClasses = SerializeUtils.deserializeClassFiles(odrT.classFiles()); 
			ObjectDataReply odr = (ObjectDataReply) SerializeUtils.deserializeJavaObject(odrT.data(), odrTClasses);
			job.objectDataReply(odr);
		}
		return job;
	}

	public void start(NavigableMap<Number640, Data> input, DHTConnectionProvider dht) throws Exception {
		Task start = this.findStartTask();
		start.broadcastReceiver(input, dht);
	}

	private Task findStartTask() {
		for (Task task : tasks) {
			if (task.previousId() == null) {// This marks the start
				return task;
			}
		}
		return null;
	}

	public Task findTask(Number640 taskId) {
		for (Task task : tasks) {
			if (task.currentId().equals(taskId)) {
				return task;
			}
		}
		return null;
	}
}