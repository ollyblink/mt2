package net.tomp2p.mapreduce;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.Random;
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
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.mapreduce.utils.DataStorageObject;
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
	private static Storage storage;
	private MapReduceBroadcastHandler bcHandler;

	public TaskRPC(final PeerBean peerBean, final ConnectionBean connectionBean, MapReduceBroadcastHandler bcHandler) {
		super(peerBean, connectionBean);
		this.bcHandler = bcHandler;
	}

	public FutureResponse putTask(final PeerAddress remotePeer, final TaskBuilder taskBuilder, final ChannelCreator channelCreator) {
		final Message message = createMessage(remotePeer, RPC.Commands.GCM.getNr(), Type.REQUEST_1);// TODO: replace GCM with TASK
		DataMap requestDataMap = new DataMap(new TreeMap<>());
		try {
			// will become storage.put(taskBuilder.key(), taskBuilder.dataStorageTriple());
			requestDataMap.dataMap().put(NumberUtils.STORAGE_KEY, new Data(taskBuilder.key())); // the key for the values to put
			requestDataMap.dataMap().put(NumberUtils.VALUE, new Data(taskBuilder.dataStorageTriple())); // The actual values to put
		} catch (IOException e) {
			e.printStackTrace();
		}

		message.setDataMap(requestDataMap);
		FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(futureResponse, peerBean(), connectionBean(), taskBuilder);

		if (!taskBuilder.isForceTCP()) {
			return requestHandler.fireAndForgetUDP(channelCreator);
		} else {
			return requestHandler.sendTCP(channelCreator);
		}
	}

	public FutureResponse getTask(final PeerAddress remotePeer, final TaskBuilder taskBuilder, final ChannelCreator channelCreator) {
		final Message message = createMessage(remotePeer, RPC.Commands.GCM.getNr(), Type.REQUEST_2).keepAlive(true);// TODO: replace GCM with TASK
		DataMap requestDataMap = new DataMap(new TreeMap<>());
		try {
			// will become storage.get(taskBuilder.key()): dataStorageTriple();
			requestDataMap.dataMap().put(NumberUtils.STORAGE_KEY, new Data(taskBuilder.key())); // The values to retrieve
			requestDataMap.dataMap().put(NumberUtils.OLD_BROADCAST, new Data(taskBuilder.broadcastInput())); // Used to send the broadcast again if this connection fails
		} catch (IOException e) {
			e.printStackTrace();
		}

		message.setDataMap(requestDataMap);

		FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(futureResponse, peerBean(), connectionBean(), taskBuilder);

		if (!taskBuilder.isForceTCP()) {
			return requestHandler.fireAndForgetUDP(channelCreator);
		} else {
			return requestHandler.sendTCP(channelCreator);
		}
	}

	@Override
	public void handleResponse(final Message message, PeerConnection peerConnection, final boolean sign, Responder responder) throws Exception {
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
			Object value = null;
			synchronized (storage) {
				// Try to acquire the value
				Data valueData = storage.get(storageKey);
				if (valueData != null) {
					DataStorageObject dST = (DataStorageObject) valueData.object();
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
				/*
				 * Add listener to peer connection such that if the connection dies, the broadcast is sent once again Add a broadcast listener that, in case it receives the broadcast, sets the flag of the peer connection listener to false, such that the connection listener is not invoked anymore
				 */
				final AtomicBoolean activeOnDataFlag = new AtomicBoolean(true);
				peerConnection.closeFuture().addListener(peerConnectionListener(dataMap, storageKey, activeOnDataFlag));
				bcHandler.addPeerConnectionRemoveActiveFlageListener(peerConnectionActiveFlagRemoveListener(peerConnection, storageKey, activeOnDataFlag));
				DataMap responseDataMap = new DataMap(new TreeMap<>());
				responseDataMap.dataMap().put(storageKey, new Data(value));
				responseMessage.setDataMap(responseDataMap);
			}
		} else {
			LOG.info("Wrong Request type: " + message.type());
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

	private IPeerConnectionActiveFlagRemoveListener peerConnectionActiveFlagRemoveListener(PeerConnection peerConnection, Number640 storageKey, final AtomicBoolean activeOnDataFlag) {
		return new IPeerConnectionActiveFlagRemoveListener() {

			@Override
			public void turnOffActiveOnDataFlag(PeerAddress p, Number640 recKey) throws Exception {
				if (peerConnection.remotePeer().equals(p) && storageKey.equals(recKey)) {
					activeOnDataFlag.set(false);
					LOG.info("active set to false!");
				} else {
					LOG.info("Not correct peer or key. Ignored. this listener's PeerConnection to observe: [" + peerConnection.remotePeer().peerId().intValue() + "]");
				}
			}
		};
	}

	private BaseFutureAdapter<BaseFuture> peerConnectionListener(NavigableMap<Number640, Data> dataMap, Number640 storageKey, final AtomicBoolean activeOnDataFlag) {
		return new BaseFutureAdapter<BaseFuture>() {

			@Override
			public void operationComplete(BaseFuture future) throws Exception {
				if (activeOnDataFlag.get()) {
					synchronized (storage) {
						Data data = storage.get(storageKey);
						if (data != null) {
							DataStorageObject dST = (DataStorageObject) data.object();
							dST.tryDecrementCurrentNrOfExecutions(); // Makes sure the data is available again to another peer that tries to get it.
							storage.put(storageKey, new Data(dST));
							NavigableMap<Number640, Data> oldBroadcastInput = (NavigableMap<Number640, Data>) dataMap.get(NumberUtils.OLD_BROADCAST).object();
							bcHandler.dht().broadcast(Number160.createHash(new Random().nextLong()), oldBroadcastInput);
							LOG.info("active is true: dST.tryDecrementCurrentNrOfExecutions() plus broadcast");
						}
					}
				} else {
					LOG.info("active was already set to false: " + activeOnDataFlag.get());
				}
			}
		};
	}
}
