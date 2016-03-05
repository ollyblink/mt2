package net.tomp2p.mapreduce;

import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.dht.Storage;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.mapreduce.utils.MapReduceValue;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class PeerConnectionCloseListener extends BaseFutureAdapter<BaseFuture> {
	private static final Logger LOG = LoggerFactory.getLogger(PeerConnectionCloseListener.class);

	private AtomicBoolean activeOnDataFlag;

	private Storage storage;

	private Number640 storageKey;

	private NavigableMap<Number640, Data> broadcastData;

	private MapReduceBroadcastHandler bcHandler;
	
	

	public PeerConnectionCloseListener(AtomicBoolean activeOnDataFlag, Storage storage, Number640 storageKey, NavigableMap<Number640, Data> broadcastData, MapReduceBroadcastHandler bcHandler) {
		this.activeOnDataFlag = activeOnDataFlag;
		this.storage = storage;
		this.storageKey = storageKey;
		this.broadcastData = broadcastData;
		this.bcHandler = bcHandler;
	}



	@Override
	public void operationComplete(BaseFuture future) throws Exception {
		if (future.isSuccess()) {
			if (activeOnDataFlag.get()) {
				synchronized (storage) {
					Data data = storage.get(storageKey);
					if (data != null) {
						MapReduceValue dST = (MapReduceValue) data.object();
						dST.tryDecrementCurrentNrOfExecutions(); // Makes sure the data is available again to another peer that tries to get it.
						storage.put(storageKey, new Data(dST));
						bcHandler.dht().broadcast(Number160.createHash(new Random().nextLong()), broadcastData);
						LOG.info("active is true: dST.tryDecrementCurrentNrOfExecutions() plus broadcast convertedOldBCInput with #values: " + broadcastData.values().size());
					}
				}
			} else {
				LOG.info("active was already set to false: " + activeOnDataFlag.get());
			}
		} else {
			LOG.warn("!future.isSuccess() on PeerConnectionCloseListener, failed reason: " + future.failedReason());
		}
	}
}
