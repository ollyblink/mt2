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
			requestDataMap.dataMap().put(taskBuilder.key(), new Data(taskBuilder.dataStorageTriple()));
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
		final Message message = createMessage(remotePeer, RPC.Commands.GCM.getNr(), Type.REQUEST_2);// TODO: replace GCM with TASK
		DataMap requestDataMap = new DataMap(new TreeMap<>());
		try {
			requestDataMap.dataMap().put(taskBuilder.key(), new Data(taskBuilder.key()));
			requestDataMap.dataMap().put(NumberUtils.allSameKey("OLD_BROADCAST"), new Data(taskBuilder.broadcastInput()));
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
		// synchronized (peerBean().peerStatusListeners()) {
		// for (PeerStatusListener peerStatusListener : peerBean().peerStatusListeners()) {
		// peerStatusListener.peerFailed(message.sender(), new PeerException(AbortCause.SHUTDOWN, "shutdown"));
		// }
		// }
		Message responseMessage = null;
		NavigableMap<Number640, Data> dataMap = message.dataMap(0).dataMap();
		if (dataMap.size() > 1) {
			throw new Exception("DataMap should have size 1 for this handler but was [" + dataMap.size() + "]");
		} else {
			if (message.type() == Type.REQUEST_1) { // Put
				for (Number640 key : dataMap.keySet()) {
					storage.put(key, dataMap.get(key));
					responseMessage = createResponseMessage(message, Type.OK);
				}
			} else if (message.type() == Type.REQUEST_2) {// Get
				for (Number640 key : dataMap.keySet()) {
					Object value = null;
					synchronized (storage) {
						Data data = storage.get(key);
						if (data != null) {
							DataStorageObject dST = (DataStorageObject) data.object();
							value = dST.tryIncrementCurrentNrOfExecutions();
							storage.put(key, new Data(dST));
						}
					}

					if (value == null) {
						responseMessage = createResponseMessage(message, Type.NOT_FOUND);// Not okay
					} else {
						responseMessage = createResponseMessage(message, Type.OK);
					}

					final AtomicBoolean active = new AtomicBoolean(true);

					BaseFutureAdapter<BaseFuture> connectionListener = new BaseFutureAdapter<BaseFuture>() {

						@Override
						public void operationComplete(BaseFuture future) throws Exception {
							if (active.get()) {
								synchronized (storage) {
									Data data = storage.get(key);
									if (data != null) {
										DataStorageObject dST = (DataStorageObject) data.object();
										dST.tryDecrementCurrentNrOfExecutions();
										storage.put(key, new Data(dST));
										NavigableMap<Number640, Data> input = (NavigableMap<Number640, Data>) dataMap.get(NumberUtils.allSameKey("OLD_BROADCAST")).object();
										bcHandler.dht().broadcast(Number160.createHash(new Random().nextLong()), input);
									}
								}
							} else {
								// Nothing to do
							}
						}

					};

					// final AtomicBoolean active;
					bcHandler.addBroadcastListener(new IBroadcastListener() {

						@Override
						public void inform(PeerAddress p) throws Exception {
							NavigableMap<Number640, Data> input = (NavigableMap<Number640, Data>) dataMap.get(NumberUtils.allSameKey("OLD_BROADCAST")).object();
							input.get(NumberUtils.allSameKey("))
							if(peerConnection.remotePeer().equals(p)){
								active.set(false);
								 
							}
						}
					});
					peerConnection.closeFuture().addListener(connectionListener);
					DataMap responseDataMap = new DataMap(new TreeMap<>());
					responseDataMap.dataMap().put(key, new Data(value));
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
}
