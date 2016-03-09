package net.tomp2p.mapreduce.examplejob;

import java.util.NavigableMap;

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
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

public class ExampleJobBroadcastReceiver implements IMapReduceBroadcastReceiver {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6201919213334638897L;
	private static Logger logger = LoggerFactory.getLogger(ExampleJobBroadcastReceiver.class);
	private String id;

	public ExampleJobBroadcastReceiver() {
		this.id = ExampleJobBroadcastReceiver.class.getSimpleName();
	}

	// private FutureTask jobFutureGet;
	private Job job = null;

	@Override
	public void receive(Message message, PeerMapReduce peerMapReduce) {
		NavigableMap<Number640, Data> input = message.dataMapList().get(0).dataMap();
		// Job job = null;
		try {
			Data jobData = input.get(NumberUtils.JOB_KEY);
			if (jobData != null) {
				Number640 jobKey = ((Number640) jobData.object());
				peerMapReduce.get(jobKey.locationKey(), jobKey.domainKey(), null).start().addListener(new BaseFutureAdapter<FutureTask>() {

					public void operationComplete(FutureTask future) throws Exception {
						if (future.isSuccess()) {
							if (job == null) {
								JobTransferObject serialized = (JobTransferObject) future.data().object();
								job = Job.deserialize(serialized);
							}
						}
						if (job != null) {
							logger.info("[" + peerMapReduce.peer().peerID().shortValue() + "]: Success on job retrieval. Job = " + job);
							PeerAddress sender = null;
							if (input.containsKey(NumberUtils.SENDER)) {
								sender = (PeerAddress) input.get(NumberUtils.SENDER).object();
							}
							// This implementation only processes messages from the same peer.
							// Exception: Initial task (announces the data) and last task (to shutdown the peers)
							if(input.containsKey(NumberUtils.CURRENT_TASK) && 
									input.containsKey(NumberUtils.NEXT_TASK) &&  
									input.containsKey(NumberUtils.allSameKey("INPUTTASKID")) && 
									input.containsKey(NumberUtils.allSameKey("SHUTDOWNTASKID"))){

								Number640 currentTaskId = (Number640) input.get(NumberUtils.CURRENT_TASK).object();
								Number640 nextTaskId = (Number640) input.get(NumberUtils.NEXT_TASK).object();
								Number640 initTaskId = (Number640) input.get(NumberUtils.allSameKey("INPUTTASKID")).object(); // All should receive this
								Number640 lastActualTask = (Number640) input.get(NumberUtils.allSameKey("SHUTDOWNTASKID")).object(); // All should receive this

								Task task = job.findTask(nextTaskId);

								logger.info("I [" + peerMapReduce.peer().peerID().shortValue() + "] received next task to execute from peerid [" + sender.peerId().shortValue() + "]: " + task.getClass().getName());
								if ((job != null) || (currentTaskId.equals(initTaskId)) || nextTaskId.equals(lastActualTask)) {
									task.broadcastReceiver(input, peerMapReduce);
								} else {
									logger.info("(job != null && dht.peer().peerAddress().equals(sender))" + (job != null && peerMapReduce.peer().peerAddress().equals(sender)) + "|| (currentTaskId.equals(initTaskId)) " + (currentTaskId.equals(initTaskId)) + " || currentTaskId.equals(lastActualTask) "
											+ currentTaskId.equals(lastActualTask));
								}
							}else{
								logger.info("Did not contain one of the following keys in input: CURRENT_TASK[" +(input.containsKey(NumberUtils.CURRENT_TASK) +"] or NEXT_TASK["+ 
										input.containsKey(NumberUtils.NEXT_TASK)  +"] or INPUTTASKID["+  
										input.containsKey(NumberUtils.allSameKey("INPUTTASKID")) +"] or SHUTDOWNTASKID["+ 
										input.containsKey(NumberUtils.allSameKey("SHUTDOWNTASKID"))) +"]");
							}
						} else {
							logger.info("Job was null");
						}
					}

				});
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	// private void tryExecuteTask(NavigableMap<Number640, Data> input, PeerMapReduce peerMapReduce) {
	//
	// try {
	// PeerAddress sender = null;
	// if (input.containsKey(NumberUtils.SENDER)) {
	// sender = (PeerAddress) input.get(NumberUtils.SENDER).object();
	// }
	// // This implementation only processes messages from the same peer.
	// // Excecption: Initial task (announces the data) and last task (to shutdown the peers)
	// Number640 currentTaskId = (Number640) input.get(NumberUtils.CURRENT_TASK).object();
	// Number640 initTaskId = (Number640) input.get(NumberUtils.allSameKey("INPUTTASKID")).object(); // All should receive this
	// Number640 lastActualTask = (Number640) input.get(NumberUtils.allSameKey("WRITETASKID")).object(); // All should receive this
	//
	// Task task = job.findTask((Number640) input.get(NumberUtils.NEXT_TASK).object());
	//
	// logger.info("I " + peerMapReduce.peer().peerID().shortValue() + " received next task to execute from peerid [" + sender.peerId().shortValue() + "]: " + task.getClass().getName());
	// if ((job != null /* && peerMapReduce.peer().peerAddress().equals(sender) */) || (currentTaskId.equals(initTaskId)) || currentTaskId.equals(lastActualTask)) {
	// task.broadcastReceiver(input, peerMapReduce);
	// } else {
	// logger.info("(job != null && dht.peer().peerAddress().equals(sender))" + (job != null && peerMapReduce.peer().peerAddress().equals(sender)) + "|| (currentTaskId.equals(initTaskId)) " + (currentTaskId.equals(initTaskId)) + " || currentTaskId.equals(lastActualTask) "
	// + currentTaskId.equals(lastActualTask));
	// }
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	//
	// }

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
		// TODO Auto-generated method stub
		return id;
	}

	// private synchronized FutureGet getJobIfNull(NavigableMap<Number640, Data> dataMap, DHTWrapper dht) throws ClassNotFoundException, IOException {
	//
	// // Number160 jobKey = (Number160) dataMap.get(NumberUtils.allSameKey("JOBKEY")).object();
	// return dht.get(Number160.createHash("JOBKEY")).addListener(new BaseFutureAdapter<FutureGet>() {
	//
	// @Override
	// public void operationComplete(FutureGet future) throws Exception {
	// if (future.isSuccess()) {
	// if (jobFutureGet == null) {
	// JobTransferObject serialized = (JobTransferObject) future.data().object();
	// job = Job.deserialize(serialized);
	// logger.info("Found job " + job);
	// }
	// } else {
	// logger.info("Could not find job");
	// }
	// }
	//
	// });
	//
	// }
}
