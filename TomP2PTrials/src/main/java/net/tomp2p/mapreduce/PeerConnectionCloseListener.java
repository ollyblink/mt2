package net.tomp2p.mapreduce;

import java.util.NavigableMap;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.dht.Storage;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.mapreduce.utils.MapReduceValue;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

public class PeerConnectionCloseListener extends BaseFutureAdapter<BaseFuture> {
	private static final Logger LOG = LoggerFactory.getLogger(PeerConnectionCloseListener.class);

	private static final long WAITING_TIME = 10000;

	private AtomicBoolean activeOnDataFlag;

	private Storage storage;

	private NavigableMap<Number640, Data> broadcastData;

	private Peer peer;

	private Object value;

	private Triple requester;

	public PeerConnectionCloseListener(AtomicBoolean activeOnDataFlag, Triple requester, Storage storage, NavigableMap<Number640, Data> broadcastData, Peer peer, Object value) {
		this.activeOnDataFlag = activeOnDataFlag;
		this.requester = requester;
		this.storage = storage;
		this.broadcastData = broadcastData;
		this.peer = peer;
		this.value = value;
	}

	@Override
	public void operationComplete(BaseFuture future) throws Exception {
		if (future.isSuccess()) {
			Timer timer = new Timer();
			timer.schedule(new TimerTask() {

				@Override
				public void run() {
					if (activeOnDataFlag.get()) {
						try {
							synchronized (storage) {
								Data data = storage.get(requester.storageKey);
								if (data != null) {
									MapReduceValue dST = (MapReduceValue) data.object();
									dST.tryDecrementCurrentNrOfExecutions(); // Makes sure the data is available again to another peer that tries to get it.
									storage.put(requester.storageKey, new Data(dST));
									LOG.info("active is true: dST.tryDecrementCurrentNrOfExecutions() for peer [" + requester.peerAddress.peerId().shortValue() + "]on data item with key[" + requester.storageKey.locationAndDomainKey().intValue() + "] and value[" + value + "]");
									peer.broadcast(new Number160(new Random())).dataMap(broadcastData).start();
								}
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					} else {
						LOG.info("active was already set to false: " + activeOnDataFlag.get());
					}
				}

			}, WAITING_TIME);

		} else {
			LOG.warn("!future.isSuccess() on PeerConnectionCloseListener, failed reason: " + future.failedReason());
		}
	}
}
