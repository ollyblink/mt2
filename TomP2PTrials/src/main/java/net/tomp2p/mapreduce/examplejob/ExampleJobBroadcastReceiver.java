package net.tomp2p.mapreduce.examplejob;

import java.util.Collections;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.mapreduce.FutureTask;
import net.tomp2p.mapreduce.IMapReduceBroadcastReceiver;
import net.tomp2p.mapreduce.Job;
import net.tomp2p.mapreduce.PeerMapReduce;
import net.tomp2p.mapreduce.Task;
import net.tomp2p.mapreduce.utils.JobTransferObject;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.message.Message;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class ExampleJobBroadcastReceiver implements IMapReduceBroadcastReceiver {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6201919213334638897L;
	private static Logger logger = LoggerFactory.getLogger(ExampleJobBroadcastReceiver.class);
	private String id;
	/** Listens to this job only */
	private Number640 jobId;
	// private Job job = null;

	public Set<Job> jobs = Collections.synchronizedSet(new HashSet<>());

	public ExampleJobBroadcastReceiver(Number640 jobId) {
		this.jobId = jobId;
		this.id = ExampleJobBroadcastReceiver.class.getSimpleName() + "_" + jobId;
	}

	@Override
	public void receive(Message message, PeerMapReduce peerMapReduce) {

		NavigableMap<Number640, Data> input = message.dataMapList().get(0).dataMap();
		try {
			Data jobData = input.get(NumberUtils.JOB_DATA);
			System.err.println("Job data : " + jobData.object().getClass());
			if (jobData != null) {
				synchronized (jobs) {
					JobTransferObject serializedJob = ((JobTransferObject) jobData.object());
					Job job = Job.deserialize(serializedJob);
					// jobs.add(j);
					if (!job.id().equals(jobId)) {
						System.err.println("Received job for wrong id: observing job [" + jobId.locationKey().shortValue() + "], received job[" + job.id().locationKey().shortValue() + "]");
						logger.info("Received job for wrong id: observing job [" + jobId.locationKey().shortValue() + "], received job[" + job.id().locationKey().shortValue() + "]");
						return;
					}

					// boolean containsJob = false;
					// for(Job j: jobs) {
					// if(j.id().equals(jobKey)){
					// containsJob = true;
					// job = j;
					// }
					// }

					if (!jobs.contains(job)) {
						jobs.add(job);
					} else {
						for (Job j : jobs) {
							if (j.id().equals(jobId)) {
								job = j;
							}
						}
					}
					// peerMapReduce.get(jobKey.locationKey(), jobKey.domainKey(), null).start().addListener(new BaseFutureAdapter<FutureTask>() {
					//
					// public void operationComplete(FutureTask future) throws Exception {
					// if (future.isSuccess()) {
					// synchronized (jobs) {
					// boolean containsJob = false;
					// for(Job job: jobs) {
					// if(job.id().equals(jobKey)){
					// containsJob = true;
					// }
					// }
					// if (!containsJob) {
					// JobTransferObject serialized = (JobTransferObject) future.data().object();
					// Job j = Job.deserialize(serialized);
					// jobs.add(j);
					// }
					// }
					//
					// }
					// if (job != null) {
					// logger.info("[" + peerMapReduce.peer().peerID().shortValue() + "]: Success on job retrieval. Job = " + job);
					if (input.containsKey(NumberUtils.NEXT_TASK)) {
						Number640 nextTaskId = (Number640) input.get(NumberUtils.NEXT_TASK).object();
						Task task = job.findTask(nextTaskId);
						task.broadcastReceiver(input, peerMapReduce);
					}
					// } else {
					// logger.info("Job was null");
					// }
				}

				// });
				// }else{
				//// logger.info("[" + peerMapReduce.peer().peerID().shortValue() + "]: Success on job retrieval. Job = " + job);
				// if (input.containsKey(NumberUtils.NEXT_TASK)) {
				// Number640 nextTaskId = (Number640) input.get(NumberUtils.NEXT_TASK).object();
				// Task task = job.findTask(nextTaskId);
				// task.broadcastReceiver(input, peerMapReduce);
				// }
				// }
				// }
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		ExampleJobBroadcastReceiver other = (ExampleJobBroadcastReceiver) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

	@Override
	public String id() {
		return id;
	}

	@Override
	public void printExecutionDetails() {
		for(Job job: jobs){
			System.err.println("DETAILS FOR JOB "+ job.id());
			for(Task task: job.tasks()){
				task.printExecutionDetails();
			}
		}
	}

}
