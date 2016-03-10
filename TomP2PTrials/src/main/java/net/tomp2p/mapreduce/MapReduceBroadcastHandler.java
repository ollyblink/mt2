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
//		int threads = Runtime.getRuntime().availableProcessors();
		int threads = 1;
		this.executor = new ThreadPoolExecutor(threads, threads, Long.MAX_VALUE, TimeUnit.DAYS, new LinkedBlockingQueue<>());
	}

	@Override
	public StructuredBroadcastHandler receive(Message message) {
 		if (!executor.isShutdown()) {
			executor.execute(new Runnable() {

				@Override
				public void run() {
					try {
						NavigableMap<Number640, Data> input = message.dataMapList().get(0).dataMap();
						// inform peerConnectionActiveFlagRemoveListeners about completed/finished data processing newInput.put(NumberUtils.SENDER, new Data(pmr.peer().peerAddress()));
						if (input.containsKey(NumberUtils.SENDER) && input.containsKey(NumberUtils.INPUT_STORAGE_KEY)) { // Skips in first execution where there is no input
							PeerAddress peerAddress = (PeerAddress) input.get(NumberUtils.SENDER).object();
//							logger.info("Received input storage key: [" + input.get(NumberUtils.INPUT_STORAGE_KEY).object() + "]");
							Number640 storageKey = (Number640) input.get(NumberUtils.INPUT_STORAGE_KEY).object();
							// if (!peerAddress.equals(peerMapReduce.peer().peerAddress())) {
							informPeerConnectionActiveFlagRemoveListeners(peerAddress, storageKey);
							// } else {
							// logger.info("Received message from myself I[" + peerMapReduce.peer().peerID().shortValue() + "]/Rec[" + peerAddress.peerId().shortValue() + "]... Ignores flag remove listener");
							// }
						}
						// Receivers need to be generated and added if they did not exist yet
						if (input.containsKey(NumberUtils.RECEIVERS)) {
							instantiateReceivers(((List<TransferObject>) input.get(NumberUtils.RECEIVERS).object()));
						}
						// Call receivers with new input data...
						// if (message.sender() != null) {
						passMessageToBroadcastReceivers(message);
						// }

					} catch (Exception e) {
						logger.info("Exception caught", e);
					}
				}
			});
		}
//		try {
//			NavigableMap<Number640, Data> input = message.dataMapList().get(0).dataMap();
//			// inform peerConnectionActiveFlagRemoveListeners about completed/finished data processing newInput.put(NumberUtils.SENDER, new Data(pmr.peer().peerAddress()));
//			if (input.containsKey(NumberUtils.SENDER) && input.containsKey(NumberUtils.INPUT_STORAGE_KEY)) { // Skips in first execution where there is no input
//				PeerAddress peerAddress = (PeerAddress) input.get(NumberUtils.SENDER).object();
//				logger.info("Received input storage key: [" + input.get(NumberUtils.INPUT_STORAGE_KEY).object() + "]");
//				Number640 storageKey = (Number640) input.get(NumberUtils.INPUT_STORAGE_KEY).object();
//				// if (!peerAddress.equals(peerMapReduce.peer().peerAddress())) {
//				informPeerConnectionActiveFlagRemoveListeners(peerAddress, storageKey);
//				// } else {
//				// logger.info("Received message from myself I[" + peerMapReduce.peer().peerID().shortValue() + "]/Rec[" + peerAddress.peerId().shortValue() + "]... Ignores flag remove listener");
//				// }
//			}
//			// Receivers need to be generated and added if they did not exist yet
//			if (input.containsKey(NumberUtils.RECEIVERS)) {
//				instantiateReceivers(((List<TransferObject>) input.get(NumberUtils.RECEIVERS).object()));
//			}
//			// Call receivers with new input data...
//			// if (message.sender() != null) {
//			passMessageToBroadcastReceivers(message);
//			// }
//
//		} catch (Exception e) {
//			logger.info("Exception caught", e);
//		}
		return super.receive(message);
	}

	private void informPeerConnectionActiveFlagRemoveListeners(PeerAddress sender, Number640 storageKey) throws ClassNotFoundException, IOException {
		List<PeerConnectionActiveFlagRemoveListener> toRemove = Collections.synchronizedList(new ArrayList<>());
		boolean successOnTurnOff = false;
		Triple triple = new Triple(sender, storageKey);
//		logger.info("I [" + peerMapReduce.peer().peerID().shortValue() + "] received triple [" + triple + "]. Look for active flag remove listener");
		if (peerMapReduce.peer().peerAddress().equals(sender)) {
			logger.info("I [" + peerMapReduce.peer().peerID().shortValue() + "] received bc from myself [" + triple + "]. Ignore");
			return;
		}
		synchronized (peerConnectionActiveFlagRemoveListeners) {
			for (PeerConnectionActiveFlagRemoveListener bL : peerConnectionActiveFlagRemoveListeners) {
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
			boolean removed = peerConnectionActiveFlagRemoveListeners.removeAll(toRemove);
			logger.info("Could remove listener on triple [" + triple + "]? [" + removed + "]");
		}

		if (!successOnTurnOff) {
			// Needs to save and check that for future RPCs
			logger.info("Possibly received triple before listener was added... triple[" + triple + "]");
			receivedButNotFound.add(triple);
		}
	}

	private void instantiateReceivers(List<TransferObject> receiverClasses) {
		for (TransferObject o : receiverClasses) {
			Map<String, Class<?>> rClassFiles = SerializeUtils.deserializeClassFiles(o.classFiles());
			IMapReduceBroadcastReceiver receiver = (IMapReduceBroadcastReceiver) SerializeUtils.deserializeJavaObject(o.data(), rClassFiles);
			synchronized (receivers) {
				for (IMapReduceBroadcastReceiver r : receivers) {
					if (r.id().equals(receiver.id())) {
						return;
					}
				}
				this.receivers.add(receiver);
			}
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

	public void shutdown() {
		try {
			executor.shutdown();
			int cnt = 0;
			while (!executor.awaitTermination(6, TimeUnit.SECONDS) && cnt++ >= 2) {
				logger.info("Await thread completion");
			}
			executor.shutdownNow();
		} catch (InterruptedException e) {
			logger.warn("Exception caught", e);
		}
	}

	public void addPeerConnectionRemoveActiveFlageListener(PeerConnectionActiveFlagRemoveListener peerConnectionActiveFlagRemoveListener) {
		logger.info("added listener for connection " + peerConnectionActiveFlagRemoveListener.triple());
		this.peerConnectionActiveFlagRemoveListeners.add(peerConnectionActiveFlagRemoveListener);
	}

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