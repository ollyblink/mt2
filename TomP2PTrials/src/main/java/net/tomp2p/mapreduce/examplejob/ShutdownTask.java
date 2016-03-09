package net.tomp2p.mapreduce.examplejob;

import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.storage.DHTWrapper;
import net.tomp2p.mapreduce.PeerMapReduce;
import net.tomp2p.mapreduce.Task;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class ShutdownTask extends Task {
	private static Logger logger = LoggerFactory.getLogger(ShutdownTask.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = -5543401293112052880L;

	private int retrievalCounter = 0;
	private int nrOfParticipatingPeers;
	public AtomicBoolean shutdownInitiated = new AtomicBoolean(false);

	public ShutdownTask(Number640 previousId, Number640 currentId, int nrOfParticipatingPeers) {
		super(previousId, currentId);
		this.nrOfParticipatingPeers = nrOfParticipatingPeers;
	}

	@Override
	public void broadcastReceiver(NavigableMap<Number640, Data> input, PeerMapReduce pmr) throws Exception {
		logger.info(">>>>>>>>>>>>>>>>>>>> EXECUTING SHUTDOWN TASK");
		if (shutdownInitiated.get()) {
			logger.info("Shutdown already initiated. ignored");
			return;
		}
		++retrievalCounter;
		logger.info("Retrieval counter: " + retrievalCounter + ", (" + retrievalCounter + " >= " + nrOfParticipatingPeers + ")? " + (retrievalCounter >= nrOfParticipatingPeers));
		if (retrievalCounter >= nrOfParticipatingPeers) {
			shutdownInitiated.set(true);
			logger.info("Received shutdown message. Counter is: " + retrievalCounter + ": SHUTDOWN IN 5 SECONDS");
			new Thread(new Runnable() {

				@Override
				public void run() {
					// TODO Auto-generated method stub
					try {
						Thread.sleep(5000);

						// t.shutdown();
						pmr.peer().shutdown();
						logger.info("Shutdown peer.");
						pmr.broadcastHandler().shutdown();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}).start();
		} else {
			logger.info("RetrievalCounter is only: " + retrievalCounter);
		}
	}

}