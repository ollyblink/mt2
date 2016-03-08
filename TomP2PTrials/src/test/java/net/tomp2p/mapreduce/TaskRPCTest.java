package net.tomp2p.mapreduce;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.mockito.Mockito;

import mapreduce.storage.DHTWrapper;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.dht.Storage;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.mapreduce.utils.MapReduceValue;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

public class TaskRPCTest {
	public static final int PORT_TCP = 5001;

	public static final int PORT_UDP = 5002;

	@Rule
	public TestRule watcher = new TestWatcher() {
		protected void starting(Description description) {
			System.out.println("Starting test: " + description.getMethodName());
		}
	};

	@Test
	public void testPutDataRequest() throws Exception {
		Peer se = null;
		Peer recv1 = null;
		ChannelCreator cc = null;
		try {
			se = new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start();
			recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();

			FutureChannelCreator fcc = recv1.connectionBean().reservation().create(0, 1);
			fcc.awaitUninterruptibly();
			cc = fcc.channelCreator();

			PeerMapReduce receiver = new PeerMapReduce(recv1);
			PeerMapReduce sender = new PeerMapReduce(se);
			// new TaskRPC(sender.peerBean(), sender.connectionBean(), mrBCHandler2);
			Number160 key = Number160.createHash("VALUE TO STORE");
			Number640 actualKey = new Number640(key, key, Number160.ZERO, Number160.ZERO);
			assertEquals(false, receiver.taskRPC().storage().contains(actualKey));
			String value = "VALUE TO STORE";
			MapReducePutBuilder taskDataBuilder = receiver.put(key, key, value, 3);

			// Await future response...
			FutureResponse fr = receiver.taskRPC().putTaskData(se.peerAddress(), taskDataBuilder, cc);
			fr.awaitUninterruptibly();
			assertEquals(true, fr.isSuccess());

			// Test request msgs content
			Message rM = fr.request();
			assertEquals(Type.REQUEST_1, rM.type());
			assertEquals(actualKey, (Number640) rM.dataMap(0).dataMap().get(NumberUtils.STORAGE_KEY).object());
			assertEquals("VALUE TO STORE", (String) ((MapReduceValue) rM.dataMap(0).dataMap().get(NumberUtils.VALUE).object()).tryAcquireValue());

			// Test response msgs content
			Message roM = fr.responseMessage();
			assertEquals(Type.OK, roM.type());

			// Storage content
			assertEquals(true, sender.taskRPC().storage().contains(actualKey));
			assertEquals("VALUE TO STORE", (String) ((MapReduceValue) sender.taskRPC().storage().get(actualKey).object()).tryAcquireValue());
		} finally {
			if (cc != null) {
				cc.shutdown().await();
			}
			if (se != null) {
				se.shutdown().await();
			}
			if (recv1 != null) {
				recv1.shutdown().await();
			}
		}

	}

	@Test
	public void testGetDataRequest() {
		Peer se = null;
		Peer recv1 = null;
		ChannelCreator cc = null;
		PeerMapReduce receiver = null;
		// MapReduceBroadcastHandler mrBCHandler1 = ;
		int nrOfTests = 11;
		String value1 = "VALUE1";
		try {
			// Store some data for the test directly
			// Just for information: I create a Number640 key based on the data here for simplicty...
			se = new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start();
			recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();

			FutureChannelCreator fcc = recv1.connectionBean().reservation().create(0, nrOfTests);
			fcc.awaitUninterruptibly();
			cc = fcc.channelCreator();
			DHTWrapper mockdht1 = Mockito.mock(DHTWrapper.class);
			DHTWrapper mockdht2 = Mockito.mock(DHTWrapper.class);
			receiver = new PeerMapReduce(recv1);
			PeerMapReduce sender = new PeerMapReduce(se);
			// new TaskRPC(sender.peerBean(), sender.connectionBean(), mrBCHandler2);
			Number160 key = Number160.createHash(value1);
			Number640 actualKey = new Number640(key, key, Number160.ZERO, Number160.ZERO);
			assertEquals(false, receiver.taskRPC().storage().contains(actualKey));
			receiver.taskRPC().storage().put(actualKey, new Data(new MapReduceValue(value1, 3)));

			// Check that the count was 0 in the beginning and that the data is correct
			checkStoredObjectState(receiver.taskRPC().storage(), value1, 3, 0);

			// ==========================================================
			// TEST 1 Not in the dht --> NOT FOUND
			// ==========================================================
			Number160 getKey160 = Number160.createHash("XYZ");
			Number640 getKey640 = new Number640(getKey160, getKey160, Number160.ZERO, Number160.ZERO);
			MapReduceGetBuilder taskDataBuilder = new MapReduceGetBuilder(sender, getKey160, getKey160);
			FutureResponse fr = sender.taskRPC().getTaskData(recv1.peerAddress(), taskDataBuilder, cc);
			fr.awaitUninterruptibly();
			assertEquals(true, fr.isSuccess());
			assertEquals(getKey640, (Number640) fr.request().dataMap(0).dataMap().get(NumberUtils.STORAGE_KEY).object());
			assertEquals(Type.NOT_FOUND, fr.responseMessage().type());
			// ==========================================================

			// ==========================================================
			// TEST 2 in the dht --> Acquire until it is not possible anymore (3 times you can acquire the resource, afterwards should be null)
			// ==========================================================
			taskDataBuilder.locationKey(key).domainKey(key);
			// Just some simple bc input
			TreeMap<Number640, Data> broadcastInput = new TreeMap<>();
			broadcastInput.put(NumberUtils.allSameKey("SENDERID"), new Data(se.peerID()));
			taskDataBuilder.broadcastInput(broadcastInput);

			// Try to acquire the data
			for (int i = 0; i < 10; ++i) { // Overdue it a bit... can only be used 3 times, the other 7 times should return null...
				// Actual call to TaskRPC
				fr = sender.taskRPC().getTaskData(recv1.peerAddress(), taskDataBuilder, cc);
				fr.awaitUninterruptibly();
				assertEquals(true, fr.isSuccess());

				// Request data
				NavigableMap<Number640, Data> requestDataMap = (NavigableMap<Number640, Data>) fr.request().dataMap(0).dataMap();
				assertEquals(actualKey, (Number640) requestDataMap.get(NumberUtils.STORAGE_KEY).object());
				assertEquals(se.peerID(), (Number160) new Data(((NavigableMap<Number640, byte[]>) requestDataMap.get(NumberUtils.OLD_BROADCAST).object()).get(NumberUtils.allSameKey("SENDERID"))).object());
				assertEquals(Type.REQUEST_2, fr.request().type());
				// Response data
				System.out.println(i);
				if (i >= 0 && i < 3) { // only here it should retrive the data.
					System.err.println("If " + i);

					NavigableMap<Number640, Data> responseDataMap = (NavigableMap<Number640, Data>) fr.responseMessage().dataMap(0).dataMap();
					assertEquals(value1, (String) responseDataMap.get(actualKey).object());
					assertEquals(Type.OK, fr.responseMessage().type());
					// Local storage --> check that the count was increased and put increased into the storage
					checkStoredObjectState(receiver.taskRPC().storage(), value1, 3, (i + 1));
					checkListeners(se, receiver.broadcastHandler(), value1, (i + 1));
				} else { // Here data should be null...
					System.err.println("Else " + i);
					assertEquals(null, fr.responseMessage().dataMap(0));
					assertEquals(Type.NOT_FOUND, fr.responseMessage().type());
					// Local storage --> check that the count stays up at max
					checkStoredObjectState(receiver.taskRPC().storage(), value1, 3, 3);
					checkListeners(se, receiver.broadcastHandler(), value1, 3);
				}
			}

			// Now try to invoke one listener and then try to get the data again
			Field peerConnectionActiveFlagRemoveListenersField = MapReduceBroadcastHandler.class.getDeclaredField("peerConnectionActiveFlagRemoveListeners");
			peerConnectionActiveFlagRemoveListenersField.setAccessible(true);
			List<PeerConnectionActiveFlagRemoveListener> listeners = (List<PeerConnectionActiveFlagRemoveListener>) peerConnectionActiveFlagRemoveListenersField.get(receiver.broadcastHandler());
			int listenerIndex = new Random().nextInt(listeners.size());
			listeners.get(listenerIndex).turnOffActiveOnDataFlag(new Triple(se.peerAddress(), actualKey));
			listeners.remove(listeners.get(listenerIndex));
			// ==========================================================

			cc.shutdown().await();
			Thread.sleep(11000);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (cc != null) {
					cc.shutdown().await();
				}
				if (se != null) {
					se.shutdown().await();
				}
				if (recv1 != null) {
					recv1.shutdown().await();
				}
				// Now all close listener should get invoked that still can be invoked --> should release the value and make it available again for all those connections who's activeFlag is true (2 connections)
				checkStoredObjectState(receiver.taskRPC().storage(), value1, 3, 1);
				checkListeners(se, receiver.broadcastHandler(), value1, 2);

				// FutureChannelCreator fcc = recv1.connectionBean().reservation().create(0, 1);
				// fcc.awaitUninterruptibly();
				// cc = fcc.channelCreator();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private void checkListeners(Peer sender, MapReduceBroadcastHandler mrBCHandler1, String value, int nrOfListeners) throws NoSuchFieldException, IllegalAccessException {
		// also check the state of the listeners...
		Field peerConnectionActiveFlagRemoveListenersField = MapReduceBroadcastHandler.class.getDeclaredField("peerConnectionActiveFlagRemoveListeners");
		peerConnectionActiveFlagRemoveListenersField.setAccessible(true);
		List<PeerConnectionActiveFlagRemoveListener> listeners = (List<PeerConnectionActiveFlagRemoveListener>) peerConnectionActiveFlagRemoveListenersField.get(mrBCHandler1);
		System.err.println("checkListeners:" + listeners);
		assertEquals(nrOfListeners, listeners.size());
		/*
		 * private AtomicBoolean activeOnDataFlag; private Number640 keyToObserve; private PeerAddress peerAddressToObserve;
		 */
		for (PeerConnectionActiveFlagRemoveListener l : listeners) {
			Field toAcquireField = l.getClass().getDeclaredField("toAcquire");
			toAcquireField.setAccessible(true);
			Field activeOnDataFlagField = l.getClass().getDeclaredField("activeOnDataFlag");
			activeOnDataFlagField.setAccessible(true);
			// Field peerAddressToObserveField = l.getClass().getDeclaredField("peerAddressToObserve");
			// peerAddressToObserveField.setAccessible(true);
			Triple toAcquire = (Triple) toAcquireField.get(l);

			assertEquals(true, ((AtomicBoolean) activeOnDataFlagField.get(l)).get());
			assertEquals(new Number640(Number160.createHash(value), Number160.createHash(value), Number160.ZERO, Number160.ZERO), toAcquire.storageKey);
			assertEquals(sender.peerAddress(), toAcquire.peerAddress);
		}
	}

	private void checkStoredObjectState(Storage storage, String value, int nrOfExecutions, int currentNrOfExecutions) throws NoSuchFieldException, ClassNotFoundException, IOException, IllegalAccessException {
		Field valueField = MapReduceValue.class.getDeclaredField("value");
		valueField.setAccessible(true);
		Field nrOfExecutionsField = MapReduceValue.class.getDeclaredField("nrOfExecutions");
		nrOfExecutionsField.setAccessible(true);
		Field currentnrOfExecutionsField = MapReduceValue.class.getDeclaredField("currentNrOfExecutions");
		currentnrOfExecutionsField.setAccessible(true);
		MapReduceValue dst = (MapReduceValue) storage.get(new Number640(Number160.createHash(value), Number160.createHash(value), Number160.ZERO, Number160.ZERO)).object();
		assertEquals(value, (String) valueField.get(dst));
		assertEquals(nrOfExecutions, (int) nrOfExecutionsField.get(dst));
		assertEquals(currentNrOfExecutions, (int) currentnrOfExecutionsField.get(dst));
		System.err.println("TaskRPCTest.checkStoredObjectState(): dst.toString(): " + dst.toString());
	}

}
