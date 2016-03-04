package net.tomp2p.mapreduce;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.RequestHandler;
import net.tomp2p.connection.Responder;
import net.tomp2p.dht.Storage;
import net.tomp2p.dht.StorageMemory;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.mapreduce.utils.MapReduceValue;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.message.DataMap;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;
import net.tomp2p.storage.Data;

public class TaskRPC extends DispatchHandler {

	private static final Logger LOG = LoggerFactory.getLogger(TaskRPC.class);
	public Storage storage = new StorageMemory();
	private MapReduceBroadcastHandler bcHandler;

	public TaskRPC(final PeerBean peerBean, final ConnectionBean connectionBean, MapReduceBroadcastHandler bcHandler) {
		super(peerBean, connectionBean);
		this.bcHandler = bcHandler;
		register(RPC.Commands.GCM.getNr());
	}

	public void storage(Storage storage) {
		this.storage = storage;
	}

	public FutureResponse putTaskData(final PeerAddress remotePeer, final MapReducePutBuilder taskDataBuilder, final ChannelCreator channelCreator) {
		final Message message = createMessage(remotePeer, RPC.Commands.GCM.getNr(), Type.REQUEST_1);// TODO: replace GCM with TASK
		DataMap requestDataMap = new DataMap(new TreeMap<>());
		try {
			// will become storage.put(taskBuilder.key(), taskBuilder.dataStorageTriple());
			requestDataMap.dataMap().put(NumberUtils.STORAGE_KEY, new Data(new Number640(taskDataBuilder.locationKey(), taskDataBuilder.domainKey(), Number160.ZERO, Number160.ZERO))); // the key for the values to put
			requestDataMap.dataMap().put(NumberUtils.VALUE, new Data(taskDataBuilder.data())); // The actual values to put
		} catch (IOException e) {
			e.printStackTrace();
		}

		message.setDataMap(requestDataMap);
		FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(futureResponse, peerBean(), connectionBean(), taskDataBuilder);

		if (taskDataBuilder.isForceUDP()) {
			return requestHandler.fireAndForgetUDP(channelCreator);
		} else {
			return requestHandler.sendTCP(channelCreator);
		}
	}

	public FutureResponse getTaskData(final PeerAddress remotePeer, final MapReduceGetBuilder taskDataBuilder, final ChannelCreator channelCreator) {
		final Message message = createMessage(remotePeer, RPC.Commands.GCM.getNr(), Type.REQUEST_2).keepAlive(true);// TODO: replace GCM with TASK

		DataMap requestDataMap = new DataMap(new TreeMap<>());
		try {
			requestDataMap.dataMap().put(NumberUtils.STORAGE_KEY, new Data(new Number640(taskDataBuilder.locationKey(), taskDataBuilder.domainKey(), Number160.ZERO, Number160.ZERO))); // the key for the values to put
			requestDataMap.dataMap().put(NumberUtils.OLD_BROADCAST, new Data(taskDataBuilder.broadcastInput())); // Used to send the broadcast again if this connection fails
		} catch (IOException e) {
			e.printStackTrace();
		}

		message.setDataMap(requestDataMap);
		FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(futureResponse, peerBean(), connectionBean(), taskDataBuilder);

		if (taskDataBuilder.isForceUDP()) {
			return requestHandler.fireAndForgetUDP(channelCreator);
		} else {
			return requestHandler.sendTCP(channelCreator);
		}
	}

	@Override
	public void handleResponse(final Message message, PeerConnection peerConnection, final boolean sign, Responder responder) throws Exception {
		System.out.println("Handle Response");
		if (!((message.type() == Type.REQUEST_1 || message.type() == Type.REQUEST_2) && message.command() == RPC.Commands.GCM.getNr())) {
			throw new IllegalArgumentException("Message content is wrong for this handler.");
		}
		Message responseMessage = null;
		NavigableMap<Number640, Data> dataMap = message.dataMap(0).dataMap();
		if (message.type() == Type.REQUEST_1) { // Put
			Number640 storageKey = (Number640) dataMap.get(NumberUtils.STORAGE_KEY).object();
			Data valueData = dataMap.get(NumberUtils.VALUE);
			storage.put(storageKey, valueData);
			responseMessage = createResponseMessage(message, Type.OK);

		} else if (message.type() == Type.REQUEST_2) {// Get
			Number640 storageKey = (Number640) dataMap.get(NumberUtils.STORAGE_KEY).object();
			// System.err.println("Storage key: " + storageKey);
			Object value = null;
			synchronized (storage) {
				// Try to acquire the value
				Data valueData = storage.get(storageKey);
				if (valueData != null) {
					MapReduceValue dST = (MapReduceValue) valueData.object();
					value = dST.tryIncrementCurrentNrOfExecutions();
					if (value == null) {
						responseMessage = createResponseMessage(message, Type.NOT_FOUND);// Not okay
					} else {
						responseMessage = createResponseMessage(message, Type.OK);
					}
					storage.put(storageKey, new Data(dST));
				}
			}
			if (value != null) {
				// Add the value to the response message
				DataMap responseDataMap = new DataMap(new TreeMap<>());
				responseDataMap.dataMap().put(storageKey, new Data(value));
				responseMessage.setDataMap(responseDataMap);
				/*
				 * Add listener to peer connection such that if the connection dies, the broadcast is sent once again Add a broadcast listener that, in case it receives the broadcast, sets the flag of the peer connection listener to false, such that the connection listener is not invoked anymore
				 */

				if (peerConnection == null) { // This means its directly connected to himself
					// Do nothing, data on this peer is lost anyways

				} else {
					final AtomicBoolean activeOnDataFlag = new AtomicBoolean(true);
					peerConnection.closeFuture().addListener(getPeerConnectionCloseListener(dataMap, storageKey, activeOnDataFlag));
//					bcHandler.addPeerConnectionRemoveActiveFlageListener(new PeerConnectionActiveFlagRemoveListener(peerConnection.remotePeer(), storageKey, activeOnDataFlag));
				}

			}
		}
		if (responseMessage == null) {
			responseMessage = createResponseMessage(message, Type.NOT_FOUND);// Not okay
		}
		if (message.isUdp()) {
			responder.responseFireAndForget();
		} else {
			responder.response(responseMessage);
		}
	}

	private BaseFutureAdapter<BaseFuture> getPeerConnectionCloseListener(NavigableMap<Number640, Data> dataMap, Number640 storageKey, final AtomicBoolean activeOnDataFlag) {
		return new BaseFutureAdapter<BaseFuture>() {

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
								NavigableMap<Number640, byte[]> oldBroadcastInput = (NavigableMap<Number640, byte[]>) dataMap.get(NumberUtils.OLD_BROADCAST).object();
								NavigableMap<Number640, Data> convertedOldBCInput = new TreeMap<>();
								for (Number640 n : oldBroadcastInput.keySet()) {
									Data dataFile = new Data(oldBroadcastInput.get(n));
									LOG.info("converted data is : " + data.object());
									convertedOldBCInput.put(n, dataFile);
								}
//								bcHandler.dht().broadcast(Number160.createHash(new Random().nextLong()), convertedOldBCInput);
								LOG.info("active is true: dST.tryDecrementCurrentNrOfExecutions() plus broadcast convertedOldBCInput with #values: " + convertedOldBCInput.values().size());
							}
						}
					} else {
						LOG.info("active was already set to false: " + activeOnDataFlag.get());
					}
				} else {
					LOG.warn("!future.isSuccess() on PeerConnectionCloseListener, failed reason: " + future.failedReason());
				}
			}
		};
	}

	public Storage storage() {
		return storage;
	}
}
