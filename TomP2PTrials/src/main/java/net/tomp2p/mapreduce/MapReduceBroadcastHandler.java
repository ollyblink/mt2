package net.tomp2p.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.storage.DHTWrapper;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.mapreduce.utils.SerializeUtils;
import net.tomp2p.mapreduce.utils.TransferObject;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.StructuredBroadcastHandler;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

public class MapReduceBroadcastHandler extends StructuredBroadcastHandler {
	private static Logger logger = LoggerFactory.getLogger(MapReduceBroadcastHandler.class);

	// private PeerMapReduce peerMapReduce;
	private List<IMapReduceBroadcastReceiver> receivers = Collections.synchronizedList(new ArrayList<>());;
	private Set<Triple> receivedButNotFound = Collections.synchronizedSet(new HashSet<>());
	private List<PeerConnectionActiveFlagRemoveListener> peerConnectionActiveFlagRemoveListeners = Collections.synchronizedList(new ArrayList<>());;

	private ThreadPoolExecutor executor;

	private PeerMapReduce peerMapReduce;

	public MapReduceBroadcastHandler() {
		this.executor = new ThreadPoolExecutor(1, 1, Long.MAX_VALUE, TimeUnit.DAYS, new LinkedBlockingQueue<>());

	}

	@Override
	public StructuredBroadcastHandler receive(Message message) {
		System.err.println("RECEIVED BROADCAST");
		NavigableMap<Number640, Data> input = message.dataMapList().get(0).dataMap();
		// inform peerConnectionActiveFlagRemoveListeners about completed/finished data processing
		try {
			informPeerConnectionActiveFlagRemoveListeners(message.sender(), (Number640) input.get(NumberUtils.STORAGE_KEY).object());
			// Receivers need to be generated and added if they did not exist yet
			if (input.containsKey(NumberUtils.RECEIVERS)) {
				instantiateReceivers(((List<TransferObject>) input.remove(NumberUtils.RECEIVERS)));
			} // Call receivers with new input data...
		} catch (Exception e) {
			logger.info("Exception caught", e);
		}
		passMessageToBroadcastReceivers(message);
		logger.info("After starting receiver.receive(message, dht), before return super.receive(message)");
		return super.receive(message);
	}

	private void instantiateReceivers(List<TransferObject> receiverClasses) {
		for (TransferObject o : receiverClasses) {
			Map<String, Class<?>> rClassFiles = SerializeUtils.deserializeClassFiles(o.classFiles());
			IMapReduceBroadcastReceiver receiver = (IMapReduceBroadcastReceiver) SerializeUtils.deserializeJavaObject(o.data(), rClassFiles);
			this.receivers.add(receiver);
		}
	}

	private void passMessageToBroadcastReceivers(Message message) {
		synchronized (receivers) {
			for (IMapReduceBroadcastReceiver receiver : receivers) {
				if (!executor.isShutdown()) {
					executor.execute(new Runnable() {

						@Override
						public void run() {
							receiver.receive(message, peerMapReduce);
						}
					});
				}
			}
		}
	}

	private void informPeerConnectionActiveFlagRemoveListeners(PeerAddress sender, Number640 storageKey) throws ClassNotFoundException, IOException {
		List<PeerConnectionActiveFlagRemoveListener> toRemove = Collections.synchronizedList(new ArrayList<>());
		boolean successOnTurnOff = false;
		Triple triple = new Triple(sender, storageKey);
		synchronized (peerConnectionActiveFlagRemoveListeners) {
			for (PeerConnectionActiveFlagRemoveListener bL : peerConnectionActiveFlagRemoveListeners) {
				System.err.println(bL + " invoked");
				try {
					successOnTurnOff = bL.turnOffActiveOnDataFlag(triple);

					if (successOnTurnOff) {
						logger.info("Listener to remove: " + bL);
						toRemove.add(bL);
					}

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			peerConnectionActiveFlagRemoveListeners.removeAll(toRemove);
		}

		if (!successOnTurnOff) {
			// Needs to save and check that for future RPCs
			logger.info("Possibly received triple before listener was added... triple[" + triple + "]");
			receivedButNotFound.add(triple);
		}
	}

	public void shutdown() throws InterruptedException {
		executor.shutdown();
		int cnt = 0;
		while (!executor.awaitTermination(6, TimeUnit.SECONDS) && cnt++ >= 2) {
			logger.info("Await thread completion");
		}
		executor.shutdownNow();
	}

	public void addPeerConnectionRemoveActiveFlageListener(PeerConnectionActiveFlagRemoveListener peerConnectionActiveFlagRemoveListener) {
		this.peerConnectionActiveFlagRemoveListeners.add(peerConnectionActiveFlagRemoveListener);
	}

	// public DHTWrapper dht() {
	// return this.dht;
	// }
	//
	// public MapReduceBroadcastHandler dht(DHTWrapper dht) {
	// this.dht = dht;
	// return this;
	// }

	public MapReduceBroadcastHandler threadPoolExecutor(ThreadPoolExecutor e) {
		this.executor = e;
		return this;
	}

	public Set<Triple> receivedButNotFound() {
		return this.receivedButNotFound;
	}

	public void peerMapReduce(PeerMapReduce peerMapReduce) {
		this.peerMapReduce = peerMapReduce;
	}
}