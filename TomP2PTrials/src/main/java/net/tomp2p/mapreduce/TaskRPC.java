package net.tomp2p.mapreduce;

import java.io.IOException;
import java.lang.reflect.Field;
import java.text.DateFormat;
import java.util.Date;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.RequestHandler;
import net.tomp2p.connection.Reservation;
import net.tomp2p.connection.Responder;
import net.tomp2p.dht.Storage;
import net.tomp2p.dht.StorageMemory;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.mapreduce.utils.MapReduceValue;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.message.DataMap;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;
import net.tomp2p.storage.Data;

public class TaskRPC extends DispatchHandler {

	private static final Logger LOG = LoggerFactory.getLogger(TaskRPC.class);
	private Storage storage = new StorageMemory();
	// private Set<Triple> locallyCreatedTriples = Collections.synchronizedSet(new HashSet<>());
	private PeerMapReduce peerMapReduce;

	public TaskRPC(final PeerMapReduce peerMapReduce) {
		super(peerMapReduce.peer().peerBean(), peerMapReduce.peer().connectionBean());
		this.peerMapReduce = peerMapReduce;
		register(RPC.Commands.GCM.getNr());
	}

	public void storage(Storage storage) {
		this.storage = storage;
	}

	public FutureResponse putTaskData(final PeerAddress remotePeer, final MapReducePutBuilder taskDataBuilder, final ChannelCreator channelCreator) {
		// long start = System.currentTimeMillis();

		// try {
		// Field f = Reservation.class.getDeclaredField("semaphoreTCP");
		// f.setAccessible(true);
		//
		// Semaphore semaphoreTCP = (Semaphore) f.get(peerMapReduce.peer().connectionBean().reservation());
		// System.err.println("TASKRPC: available permits TCP " + semaphoreTCP.availablePermits());
		// } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		final Message message = createMessage(remotePeer, RPC.Commands.GCM.getNr(), Type.REQUEST_1)
		// .keepAlive(true)
		;// TODO: replace GCM with TASK
		DataMap requestDataMap = new DataMap(new TreeMap<>());
		try {
			// will become storage.put(taskBuilder.key(), taskBuilder.dataStorageTriple());
			// LOG.info("putTaskData(k[" + taskDataBuilder.locationKey() + "] d[" + taskDataBuilder.domainKey() + "], v[ ])");
			requestDataMap.dataMap().put(NumberUtils.OUTPUT_STORAGE_KEY, new Data(new Number640(taskDataBuilder.locationKey(), taskDataBuilder.domainKey(), Number160.ZERO, Number160.ZERO))); // the key for the values to put
			requestDataMap.dataMap().put(NumberUtils.VALUE, new Data(taskDataBuilder.data())); // The actual values to put
		} catch (IOException e) {
			e.printStackTrace();
		}

		message.setDataMap(requestDataMap);
		FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(futureResponse, peerBean(), connectionBean(), taskDataBuilder);
		// long end = System.currentTimeMillis();

		// LOG.info("PUT TASK DATA BEFORE SEND MESSAGE: ["+message.messageId()+"] @ ["+ DateFormat.getDateTimeInstance().format(new Date())+"]");
		if (taskDataBuilder.isForceUDP()) {
			return requestHandler.fireAndForgetUDP(channelCreator);
		} else {
			return requestHandler.sendTCP(channelCreator);
		}
	}

	public FutureResponse getTaskData(final PeerAddress remotePeer, final MapReduceGetBuilder taskDataBuilder, final ChannelCreator channelCreator) {
		// long start = System.currentTimeMillis();

		final Message message = createMessage(remotePeer, RPC.Commands.GCM.getNr(), Type.REQUEST_2)
				// ;
				.keepAlive(true);// TODO: replace GCM with TASK
		// LOG.info("getTaskData(k[" + taskDataBuilder.locationKey() + "] d[" + taskDataBuilder.domainKey() + "], v[])");

		DataMap requestDataMap = new DataMap(new TreeMap<>());
		try {
			requestDataMap.dataMap().put(NumberUtils.OUTPUT_STORAGE_KEY, new Data(new Number640(taskDataBuilder.locationKey(), taskDataBuilder.domainKey(), Number160.ZERO, Number160.ZERO))); // the key for the values to put
			if (taskDataBuilder.broadcastInput() != null) { // Maybe null e.g. when job is used
				requestDataMap.dataMap().put(NumberUtils.OLD_BROADCAST, new Data(taskDataBuilder.broadcastInput())); // Used to send the broadcast again if this connection fails
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		message.setDataMap(requestDataMap);
		FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(futureResponse, peerBean(), connectionBean(), taskDataBuilder);

		// long end = System.currentTimeMillis();
		// LOG.info("getTaskData before send time for message " + message.messageId() + ": " + ((end - start) / 1000) + "secs");

		// LOG.info("GET TASK DATA BEFORE SEND MESSAGE: ["+message.messageId()+"] @ ["+ DateFormat.getDateTimeInstance().format(new Date())+"]");
		if (taskDataBuilder.isForceUDP()) {
			return requestHandler.fireAndForgetUDP(channelCreator);
		} else {
			return requestHandler.sendTCP(channelCreator);
		}
	}

	@Override
	public void handleResponse(final Message message, PeerConnection peerConnection, final boolean sign, Responder responder) throws Exception {
//		long start = System.currentTimeMillis();
//		LOG.info("HANDLE RESPONSE RECEIVED MESSAGE: ["+message.messageId()+"] @ ["+ DateFormat.getDateTimeInstance().format(new Date())+"]");
		if (!((message.type() == Type.REQUEST_1 || message.type() == Type.REQUEST_2) && message.command() == RPC.Commands.GCM.getNr())) {
			throw new IllegalArgumentException("Message content is wrong for this handler.");
		}
		Message responseMessage = createResponseMessage(message, Type.NOT_FOUND);
		NavigableMap<Number640, Data> dataMap = message.dataMap(0).dataMap();

		Number640 storageKey = (Number640) dataMap.get(NumberUtils.OUTPUT_STORAGE_KEY).object();
		if (message.type() == Type.REQUEST_1) { // Put
			Data valueData = dataMap.get(NumberUtils.VALUE);
			storage.put(storageKey, valueData);
			responseMessage = createResponseMessage(message, Type.OK);
//			LOG.info("PUT handle Response: [requestor: " + (peerConnection != null ? peerConnection.remotePeer().peerId().shortValue() : peerMapReduce.peer().peerID().shortValue()) + "] put(k[" + storageKey.locationKey() + "],d[" + storageKey.domainKey() + "], v[ ");
		} else if (message.type() == Type.REQUEST_2) {// Get
			// System.err.println("Storage key: " + storageKey);
			Object value = null;
			// Try to acquire the value
			synchronized (storage) {
				Data valueData = storage.get(storageKey);
				if (valueData != null) {
					MapReduceValue dST = (MapReduceValue) valueData.object();
					value = dST.tryAcquireValue();
					storage.put(storageKey, new Data(dST));
					if(value == null){
						responseMessage = createResponseMessage(message, Type.DENIED);
					}
					if(dST.nrOfExecutions() < 3){
						LOG.info("value for key ["+storageKey.locationKey().shortValue()+"] requested by peer ["+(peerConnection!=null?peerConnection.remotePeer().peerId().shortValue():peerMapReduce.peer().peerID().shortValue())+"] is "+(value == null? "[null]":"[not null]")+", dst.currentNrOfExecutions(): "+ dST.currentNrOfExecutions()+", possible nr of executions: ["+ dST.nrOfExecutions()+"]");
					}
//					LOG.info("GET handle Response [requestor: " + (peerConnection != null ? peerConnection.remotePeer().peerId().shortValue() : peerMapReduce.peer().peerID().shortValue()) + "]: get(k[" + storageKey.locationKey() + "],d[" + storageKey.domainKey() + "]):v ]");
				}
			}
			if (value != null) {
				// LOG.info("Value is :" + value);
				responseMessage = createResponseMessage(message, Type.OK);
				// Add the value to the response message
				DataMap responseDataMap = new DataMap(new TreeMap<>());
				responseDataMap.dataMap().put(storageKey, new Data(value));
				responseMessage.setDataMap(responseDataMap);
				/*
				 * Add listener to peer connection such that if the connection dies, the broadcast is sent once again Add a broadcast listener that, in case it receives the broadcast, sets the flag of the peer connection listener to false, such that the connection listener is not invoked anymore
				 */
				if (peerConnection == null) { // This means its directly connected to himself
					// Do nothing, data on this peer is lost anyways if this peer dies
//					LOG.info("Acquired data from myself");
				} else {
					if (peerMapReduce.broadcastHandler() != null) {

						Triple senderTriple = new Triple(peerConnection.remotePeer(), storageKey);
						Set<Triple> receivedButNotFound = peerMapReduce.broadcastHandler().receivedButNotFound();
						synchronized (receivedButNotFound) {
							if (receivedButNotFound.contains(senderTriple)) { // this means we received the broadcast before we received the get request for this item from this sender --> invalid/outdated request
//								LOG.info("Received Get request from [" + senderTriple + "], but was already received once and not found. responseMessage = createResponseMessage(message, Type.NOT_FOUND);");
								responseMessage = createResponseMessage(message, Type.NOT_FOUND);
								// senderTriple.nrOfAcquires--;
							} else {// Only here it is valid
								if (dataMap.containsKey(NumberUtils.OLD_BROADCAST)) { // If it is null, there is no sense in decrementing it again as nobody will receive a broadcast... -->e.g in case of job
//									LOG.info("Will add senderTriple [" + senderTriple + "] to bc handler");
									final AtomicBoolean activeOnDataFlag = new AtomicBoolean(true);
									peerMapReduce.broadcastHandler().addPeerConnectionRemoveActiveFlageListener(new PeerConnectionActiveFlagRemoveListener(senderTriple, activeOnDataFlag));
									NavigableMap<Number640, Data> oldBCInput = MapReduceGetBuilder.reconvertByteArrayToData((NavigableMap<Number640, byte[]>) dataMap.get(NumberUtils.OLD_BROADCAST).object());
									peerConnection.closeFuture().addListener(new PeerConnectionCloseListener(activeOnDataFlag, senderTriple, storage, oldBCInput, peerMapReduce.peer(), value));
								}
							}
						}
					}
				}

			}
		}
		// message.keepAlive(true);
		if (message.isUdp()) {
			responder.responseFireAndForget();
		} else {
			responder.response(responseMessage);
		}
//		long end = System.currentTimeMillis();
//		LOG.info("handleResponse time for message " + message.messageId() + ": " + ((end - start) / 1000) + "secs");
	}

	public PeerMapReduce peerMapReduce(){
		return this.peerMapReduce;
	}
	public Storage storage() {
		return storage;
	}
}
