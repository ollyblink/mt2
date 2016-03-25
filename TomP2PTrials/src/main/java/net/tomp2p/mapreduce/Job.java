/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.tomp2p.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;

import net.tomp2p.mapreduce.examplejob.ExampleJobBroadcastReceiver;
import net.tomp2p.mapreduce.utils.JobTransferObject;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.mapreduce.utils.SerializeUtils;
import net.tomp2p.mapreduce.utils.TransferObject;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

/**
 *
 * @author draft
 */
final public class Job {

	private List<Task> tasks;

	// private ObjectDataReply objectDataReply;
	private List<IMapReduceBroadcastReceiver> broadcastReceivers;
	private Number640 id;

	public Job() {
		this.id = new Number640(new Random());
		this.broadcastReceivers = new ArrayList<>();
		this.tasks = new ArrayList<>();
	}

	public Number640 id() {
		return this.id;
	}

	public void addTask(Task task) {
		this.tasks.add(task);
	}

	// private void objectDataReply(ObjectDataReply objectDataReply) {
	// this.objectDataReply = objectDataReply;
	// }

	public JobTransferObject serialize() throws IOException {
		JobTransferObject jTO = new JobTransferObject();
		for (Task task : tasks) {
			Map<String, byte[]> taskClassFiles = SerializeUtils.serializeClassFile(task.getClass());
			byte[] taskData = SerializeUtils.serializeJavaObject(task);
			TransferObject tto = new TransferObject(taskData, taskClassFiles, task.getClass().getName());
			jTO.addTask(tto);
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
		return job;
	}

	public void start(NavigableMap<Number640, Data> input, PeerMapReduce pmr) throws Exception {
		List<TransferObject> broadcastReceiversTransferObjects = new ArrayList<>();
		for (IMapReduceBroadcastReceiver receiver : broadcastReceivers) {
			Map<String, byte[]> bcClassFiles = SerializeUtils.serializeClassFile(ExampleJobBroadcastReceiver.class);
			String bcClassName = ExampleJobBroadcastReceiver.class.getName();
			byte[] bcObject = SerializeUtils.serializeJavaObject(receiver);
			TransferObject t = new TransferObject(bcObject, bcClassFiles, bcClassName);
			broadcastReceiversTransferObjects.add(t);
		}
		input.put(NumberUtils.RECEIVERS, new Data(broadcastReceiversTransferObjects));
		input.put(NumberUtils.JOB_ID, new Data(id));
		input.put(NumberUtils.JOB_DATA, new Data(serialize()));
		Task startTask = this.findStartTask();
		startTask.broadcastReceiver(input, pmr);

	}

	public Task findStartTask() {
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

	public void addBroadcastReceiver(IMapReduceBroadcastReceiver receiver) {
		this.broadcastReceivers.add(receiver);
	}

}