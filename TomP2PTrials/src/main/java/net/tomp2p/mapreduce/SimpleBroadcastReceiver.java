package net.tomp2p.mapreduce;

import java.util.NavigableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.storage.DHTWrapper;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.mapreduce.utils.JobTransferObject;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.message.Message;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

public class SimpleBroadcastReceiver implements IMapReduceBroadcastReceiver {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6201919213334638897L;
	private static Logger logger = LoggerFactory.getLogger(SimpleBroadcastReceiver.class);
	// private ThreadPoolExecutor executor;
	private String id;

	public SimpleBroadcastReceiver() {
		this.id = SimpleBroadcastReceiver.class.getSimpleName();
		// this.executor = new ThreadPoolExecutor(1, 1, Long.MAX_VALUE, TimeUnit.DAYS, new LinkedBlockingQueue<>());
	}

	// public SimpleBroadcastReceiver(ThreadPoolExecutor executor) {
	// this.executor = executor;
	// }

	private FutureTask jobFutureGet;
	private Job job = null;

	@Override
	public void receive(Message message, DHTWrapper dht) {

		// synchronized (jobFutureGet) {
		NavigableMap<Number640, Data> input = message.dataMapList().get(0).dataMap();
		try {

			if (jobFutureGet == null) {
				jobFutureGet = dht.get(Number160.createHash("JOBKEY"), Number160.createHash("JOBKEY"), input);
				jobFutureGet.addListener(new BaseFutureAdapter<FutureTask>() {

					@Override
					public void operationComplete(FutureTask future) throws Exception {
						if (future.isSuccess()) {
							JobTransferObject serialized = (JobTransferObject) future.data().object();
							job = Job.deserialize(serialized);
							logger.info("Success on job retrieval. Job = " + job);
							tryExecuteTask(input, dht, message.sender());
						} else {
							logger.info("no success on retrieving job. Failed reason: " + future.failedReason());
						}
					}

				});

			} else {
				if (jobFutureGet.isCompleted()) {
					logger.info("JobFutureGet.isCompleted()? " + jobFutureGet.isCompleted());
					if (job != null) {
						logger.info("Job != null? " + (job != null));
						tryExecuteTask(input, dht, message.sender());
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		// }
	}

	private void tryExecuteTask(NavigableMap<Number640, Data> input, DHTWrapper dht, PeerAddress sender) {

		try {
			// This implementation only processes messages from the same peer.
			// Excecption: Initial task (announces the data) and last task (to shutdown the peers)
			// Number160 senderId = (Number160) (input.get(NumberUtils.allSameKey("SENDERID")).object());
			Number640 currentTaskId = (Number640) input.get(NumberUtils.allSameKey("CURRENTTASK")).object();
			Number640 initTaskId = (Number640) input.get(NumberUtils.allSameKey("INPUTTASKID")).object(); // All should receive this
			Number640 lastActualTask = (Number640) input.get(NumberUtils.allSameKey("WRITETASKID")).object(); // All should receive this
			// if (currentTaskId.equals(lastActualTask)) {
			Task task = job.findTask((Number640) input.get(NumberUtils.allSameKey("NEXTTASK")).object());
			System.out.println("I " + dht.peer().peerAddress() + " received next task to execute from peerid [" + sender + "]: " + task.getClass().getName());
			// }
			if ((job != null && dht.peer().peerAddress().equals(sender)) || (currentTaskId.equals(initTaskId)) || currentTaskId.equals(lastActualTask)) {

				// Task task = job.findTask((Number640) input.get(NumberUtils.allSameKey("NEXTTASK")).object());
				task.broadcastReceiver(input, dht);
			} else {
				logger.info("(job != null && dht.peer().peerAddress().equals(sender))" + (job != null && dht.peer().peerAddress().equals(sender)) + "|| (currentTaskId.equals(initTaskId)) " + (currentTaskId.equals(initTaskId)) + " || currentTaskId.equals(lastActualTask) "
						+ currentTaskId.equals(lastActualTask));
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
		if (getClass() != obj.getClass())
			return false;
		SimpleBroadcastReceiver other = (SimpleBroadcastReceiver) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
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
