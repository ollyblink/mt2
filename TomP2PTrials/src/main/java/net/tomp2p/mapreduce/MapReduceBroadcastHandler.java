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
import net.tomp2p.storage.Data;

public class MapReduceBroadcastHandler extends StructuredBroadcastHandler {
	private static Logger logger = LoggerFactory.getLogger(MapReduceBroadcastHandler.class);

	private DHTWrapper dht;

	private Set<BroadcastReceiver> receivers;
	private List<PeerConnectionActiveFlagRemoveListener> peerConnectionActiveFlagRemoveListeners;

	private ThreadPoolExecutor executor;

	public MapReduceBroadcastHandler(DHTWrapper dht, ThreadPoolExecutor executor) {
		this.dht = dht;
		this.executor = executor;
		this.receivers = Collections.synchronizedSet(new HashSet<>());
		this.peerConnectionActiveFlagRemoveListeners = Collections.synchronizedList(new ArrayList<>());
	}

	public MapReduceBroadcastHandler(DHTWrapper dht) {
		this(dht, new ThreadPoolExecutor(1, 1, Long.MAX_VALUE, TimeUnit.DAYS, new LinkedBlockingQueue<>()));

	}

	@Override
	public StructuredBroadcastHandler receive(Message message) {

		NavigableMap<Number640, Data> input = message.dataMapList().get(0).dataMap();

		Data allReceivers = input.get(NumberUtils.RECEIVERS);
		if (allReceivers != null) {
			// Receivers need to be generated and added if they did not exist yet
			try {
				List<TransferObject> receiverClasses = (List<TransferObject>) allReceivers.object();

				for (TransferObject o : receiverClasses) {
					Map<String, Class<?>> rClassFiles = SerializeUtils.deserializeClassFiles(o.classFiles());
					BroadcastReceiver receiver = (BroadcastReceiver) SerializeUtils.deserializeJavaObject(o.data(), rClassFiles);
					this.receivers.add(receiver);
				}
			} catch (ClassNotFoundException | IOException e) {
				e.printStackTrace();
			}
		}
		// inform peerConnectionActiveFlagRemoveListeners about completed/finished data processing
		try {
			Number640 storageKey = (Number640) input.get(NumberUtils.STORAGE_KEY).object();
			List<PeerConnectionActiveFlagRemoveListener> toRemove = Collections.synchronizedList(new ArrayList<>());
			synchronized (peerConnectionActiveFlagRemoveListeners) {
				for (PeerConnectionActiveFlagRemoveListener bL : peerConnectionActiveFlagRemoveListeners) {
					try {
						boolean successOnTurnOff = bL.turnOffActiveOnDataFlag(message.sender(), storageKey);
						if (successOnTurnOff) {
							toRemove.add(bL);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				peerConnectionActiveFlagRemoveListeners.removeAll(toRemove);
			}
		} catch (ClassNotFoundException | IOException e1) {
			e1.printStackTrace();
		}

		// Call receivers with new input data...
		synchronized (receivers) {
			for (BroadcastReceiver receiver : receivers) {
				if (!executor.isShutdown()) {
					executor.execute(new Runnable() {

						@Override
						public void run() {
							receiver.receive(message, dht);
						}
					});
				}
			}
		}
		logger.info("After starting receiver.receive(message, dht), before return super.receive(message)");
		return super.receive(message);
	}

	public void shutdown() throws InterruptedException {

		// if (senderId.equals(peerID) && currentTaskId.equals(lastActualTask)) {
		executor.shutdown();
		int cnt = 0;
		while (!executor.awaitTermination(6, TimeUnit.SECONDS) && cnt++ >= 2) {
			logger.info("Await thread completion");
		}
		executor.shutdownNow();
		// }
	}

	public void addPeerConnectionRemoveActiveFlageListener(PeerConnectionActiveFlagRemoveListener peerConnectionActiveFlagRemoveListener) {
		this.peerConnectionActiveFlagRemoveListeners.add(peerConnectionActiveFlagRemoveListener);
	}

	public DHTWrapper dht() {
		return this.dht;
	}
	//
	// public void addBroadcastReceiver(BroadcastReceiver receiver) {
	// if (receiver != null) {
	// this.receivers.add(receiver);
	// }
	// }
	//
	// public void addBroadcastReceivers(List<BroadcastReceiver> receivers, boolean clearBeforeAdding) {
	// if (clearBeforeAdding) {
	// this.receivers.clear();
	// }
	// this.receivers.addAll(receivers);
	// }
}
