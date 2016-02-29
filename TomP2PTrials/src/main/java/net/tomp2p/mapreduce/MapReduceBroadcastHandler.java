package net.tomp2p.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.swing.plaf.synth.SynthSpinnerUI;

import mapreduce.storage.DHTWrapper;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.mapreduce.utils.SerializeUtils;
import net.tomp2p.mapreduce.utils.TransferObject;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.StructuredBroadcastHandler;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class MapReduceBroadcastHandler extends StructuredBroadcastHandler {
	private DHTWrapper dht;

	private List<BroadcastReceiver> receivers;

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

		for (BroadcastReceiver receiver : receivers) {
			receiver.receive(message, dht, executor);
		}
		return super.receive(message);
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
