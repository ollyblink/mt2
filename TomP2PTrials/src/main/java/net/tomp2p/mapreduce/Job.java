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

import net.tomp2p.mapreduce.utils.JobTransferObject;
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
		this.tasks = new ArrayList<>();
		this.id = new Number640(new Random());
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
		// if (this.mapReduceBroadcastHandlerClass != null) {
		// Map<String, byte[]> mapReduceBroadcastHandlerClassFiles = SerializeUtils.serializeClassFile(this.mapReduceBroadcastHandlerClass);
		// TransferObject mRBCHCFTO = new TransferObject(null, mapReduceBroadcastHandlerClassFiles, mapReduceBroadcastHandlerClass.getName());
		// jTO.mapReduceBroadcastHandler(mRBCHCFTO);
		// }
		// if (this.objectDataReply != null) {
		// jTO.serializedReply(SerializeUtils.serializeClassFile(this.objectDataReply.getClass()), Utils.encodeJavaObject(this.objectDataReply), this.objectDataReply.getClass().getName());
		// }
		return jTO;
	}

	public static Job deserialize(JobTransferObject jobToDeserialize) throws ClassNotFoundException, IOException {
		Job job = new Job();
		for (TransferObject taskTransferObject : jobToDeserialize.taskTransferObjects()) {
			Map<String, Class<?>> taskClasses = SerializeUtils.deserializeClassFiles(taskTransferObject.classFiles());
			Task task = (Task) SerializeUtils.deserializeJavaObject(taskTransferObject.data(), taskClasses);
			job.addTask(task);
		}
		// TransferObject bcHandler = jobToDeserialize.mapReduceBroadcastHandler();
		// if (bcHandler != null) {
		// Map<String, Class<?>> bcHandlerClasses = SerializeUtils.deserializeClassFiles(bcHandler.classFiles());
		// job.mapReduceBroadcastHandler(bcHandlerClasses.get(bcHandler.className()));
		// }

		// TransferObject odrT = jobToDeserialize.serializedReplyTransferObject();
		// if (odrT != null) {
		// Map<String, Class<?>> odrTClasses = SerializeUtils.deserializeClassFiles(odrT.classFiles());
		// ObjectDataReply odr = (ObjectDataReply) SerializeUtils.deserializeJavaObject(odrT.data(), odrTClasses);
		// job.objectDataReply(odr);
		// }
		return job;
	}

	public void start(NavigableMap<Number640, Data> input, PeerMapReduce pmr) throws Exception {

		// Number160 jobLocationKey = Number160.createHash("JOBKEY");
		// Number160 jobDomainKey = Number160.createHash("JOBKEY");
		//
		// // ============GET ALL THE FILES ==========
		// String filesPath = (String) input.get(NumberUtils.allSameKey("DATAFILEPATH")).object();
		// List<String> pathVisitor = Collections.synchronizedList(new ArrayList<>());
		// FileUtils.INSTANCE.getFiles(new File(filesPath), pathVisitor);
		// // ===== FINISHED GET ALL THE FILES =================
		//
		// Data jobToPut = input.get(NumberUtils.JOB_KEY);
		// pmr.put(jobLocationKey, jobDomainKey, jobToPut.object(), Integer.MAX_VALUE).start().awaitUninterruptibly();
		Task start = this.findStartTask();
		//
		// ThreadPoolExecutor e = new ThreadPoolExecutor(1, 1, 100000, TimeUnit.DAYS, new LinkedBlockingQueue<>());
		//
		// for (String filePath : pathVisitor) {
		// e.submit(new Runnable() {
		//
		// @Override
		// public void run() {
		// try {
		// NavigableMap<Number640,Data> newInput = new TreeMap<>(input);
		// newInput.put(NumberUtils.allSameKey("FILEPATH"), new Data(filePath));
		start.broadcastReceiver(input, pmr);
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		// }
		// });
		// }

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

	// public void mapReduceBroadcastHandler(Class<?> class1) {
	// this.mapReduceBroadcastHandlerClass = class1;
	// }
}