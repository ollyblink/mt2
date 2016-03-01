package net.tomp2p.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
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
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class MapReduceBroadcastHandler extends StructuredBroadcastHandler {
	private static Logger logger = LoggerFactory.getLogger(MapReduceBroadcastHandler.class);

	private DHTWrapper dht;

	private List<BroadcastReceiver> receivers;
	private List<IBroadcastListener> broadcastListeners;

	private ThreadPoolExecutor executor;

	public MapReduceBroadcastHandler(DHTWrapper dht, ThreadPoolExecutor executor) {
		this.dht = dht;
		this.executor = executor;
		this.receivers = new ArrayList<>();
	}

	public MapReduceBroadcastHandler(DHTWrapper dht) {
		this(dht, new ThreadPoolExecutor(1, 1, Long.MAX_VALUE, TimeUnit.DAYS, new LinkedBlockingQueue<>()));
		this.receivers = new ArrayList<>();
	}

	@Override
	public StructuredBroadcastHandler receive(Message message) {

		// for (int i = 0; i < message.dataMapList().size(); ++i) {
		// NavigableMap<Number640, Data> input = message.dataMapList().get(i).dataMap();
		// for (Number640 n : input.keySet()) {
		// if (input.get(n) != null) {
		// try {
		// logger.info(input.get(n).object() + "");
		// } catch (ClassNotFoundException | IOException e) {
		// e.printStackTrace();
		// }
		// }
		// }
		//
		// }
		NavigableMap<Number640, Data> input = message.dataMapList().get(0).dataMap();
		Data allReceivers = input.get(NumberUtils.allSameKey("RECEIVERS"));

		if (allReceivers != null) {
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
		Number160 inputKey = (Number160) input.get(NumberUtils.allSameKey("INPUTKEY")).object();
		Number160 domainKey = (Number160) input.get(NumberUtils.allSameKey("DOMAINKEY")).object();
		for (IBroadcastListener bL : broadcastListeners) {
			try {
				bL.inform(message.sender(), inputKey, domainKey);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
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

	public void addBroadcastListener(IBroadcastListener listener) {
		this.broadcastListeners.add(listener);
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
