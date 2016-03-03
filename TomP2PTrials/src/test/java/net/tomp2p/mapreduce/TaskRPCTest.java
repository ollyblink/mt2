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
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.mapreduce.utils.DataStorageObject;
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
		Peer sender = null;
		Peer recv1 = null;
		ChannelCreator cc = null;
		try {
			sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start();
			recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();

			FutureChannelCreator fcc = recv1.connectionBean().reservation().create(0, 1);
			fcc.awaitUninterruptibly();
			cc = fcc.channelCreator();

			MapReduceBroadcastHandler mrBCHandler1 = Mockito.mock(MapReduceBroadcastHandler.class);
			MapReduceBroadcastHandler mrBCHandler2 = Mockito.mock(MapReduceBroadcastHandler.class);
			TaskRPC taskRPC = new TaskRPC(recv1.peerBean(), recv1.connectionBean(), mrBCHandler1);
			new TaskRPC(sender.peerBean(), sender.connectionBean(), mrBCHandler2);

			assertEquals(false, TaskRPC.storage().contains(NumberUtils.allSameKey("VALUE TO STORE")));
			String value = "VALUE TO STORE";
			Number640 key = NumberUtils.allSameKey(value);
			DataStorageObject dataStorageTriple = new DataStorageObject(value, 3);
			TaskDataBuilder taskDataBuilder = new TaskDataBuilder(null, null).storageKey(key).dataStorageObject(dataStorageTriple).isForceTCP(true);

			// Await future response...
			FutureResponse fr = taskRPC.putTaskData(sender.peerAddress(), taskDataBuilder, cc);
			fr.awaitUninterruptibly();
			assertEquals(true, fr.isSuccess());

			// Test request msgs content
			Message rM = fr.request();
			assertEquals(Type.REQUEST_1, rM.type());
			assertEquals(NumberUtils.allSameKey("VALUE TO STORE"), (Number640) rM.dataMap(0).dataMap().get(NumberUtils.STORAGE_KEY).object());
			assertEquals("VALUE TO STORE", (String) ((DataStorageObject) rM.dataMap(0).dataMap().get(NumberUtils.VALUE).object()).tryIncrementCurrentNrOfExecutions());

			// Test response msgs content
			Message roM = fr.responseMessage();
			assertEquals(Type.OK, roM.type());

			// Storage content
			assertEquals(true, TaskRPC.storage().contains(NumberUtils.allSameKey("VALUE TO STORE")));
			assertEquals("VALUE TO STORE", (String) ((DataStorageObject) TaskRPC.storage().get(NumberUtils.allSameKey("VALUE TO STORE")).object()).tryIncrementCurrentNrOfExecutions());
		} finally {
			if (cc != null) {
				cc.shutdown().await();
			}
			if (sender != null) {
				sender.shutdown().await();
			}
			if (recv1 != null) {
				recv1.shutdown().await();
			}
		}

	}

	@Test
	public void testGetDataRequest() throws Exception {
		Peer sender = null;
		Peer recv1 = null;
		ChannelCreator cc = null;
		MapReduceBroadcastHandler mrBCHandler1 = null;
		int nrOfTests = 11;
		try {
			// Store some data for the test directly
			// Just for information: I create a Number640 key based on the data here for simplicty...

			TaskRPC.storage().put(NumberUtils.allSameKey("VALUE1"), new Data(new DataStorageObject("VALUE1", 3)));
			// Check that the count was 0 in the beginning and that the data is correct
			checkStoredObjectState("VALUE1", 3, 0);

			sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start();
			recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();

			FutureChannelCreator fcc = recv1.connectionBean().reservation().create(0, nrOfTests);
			fcc.awaitUninterruptibly();
			cc = fcc.channelCreator();
			DHTWrapper dht1 = Mockito.mock(DHTWrapper.class);
			mrBCHandler1 = new MapReduceBroadcastHandler(dht1);
			DHTWrapper dht2 = Mockito.mock(DHTWrapper.class);
			MapReduceBroadcastHandler mrBCHandler2 = Mockito.mock(MapReduceBroadcastHandler.class);
			Mockito.when(mrBCHandler2.dht()).thenReturn(dht2);

			new TaskRPC(recv1.peerBean(), recv1.connectionBean(), mrBCHandler1);
			TaskRPC taskRPC2 = new TaskRPC(sender.peerBean(), sender.connectionBean(), mrBCHandler2);
			// ==========================================================
			// TEST 1 Not in the dht --> NOT FOUND
			// ==========================================================
			TaskDataBuilder taskDataBuilder = new TaskDataBuilder(null, null).storageKey(NumberUtils.allSameKey("XYZ")).isForceTCP(true);
			FutureResponse fr = taskRPC2.getTaskData(recv1.peerAddress(), taskDataBuilder, cc);
			fr.awaitUninterruptibly();
			assertEquals(true, fr.isSuccess());
			assertEquals(NumberUtils.allSameKey("XYZ"), (Number640) fr.request().dataMap(0).dataMap().get(NumberUtils.STORAGE_KEY).object());
			assertEquals(Type.NOT_FOUND, fr.responseMessage().type());
			// ==========================================================

			// ==========================================================
			// TEST 2 in the dht --> Acquire until it is not possible anymore (3 times you can acquire the resource, afterwards should be null)
			// ==========================================================
			taskDataBuilder.storageKey(NumberUtils.allSameKey("VALUE1"));
			// Just some simple bc input
			TreeMap<Number640, byte[]> broadcastInput = new TreeMap<>();
			broadcastInput.put(NumberUtils.allSameKey("SENDERID"), new Data(sender.peerID()).toBytes());
			taskDataBuilder.broadcastInput(broadcastInput);

			// Try to acquire the data
			for (int i = 0; i < 10; ++i) { // Overdue it a bit... can only be used 3 times, the other 7 times should return null...
				// Actual call to TaskRPC
				fr = taskRPC2.getTaskData(recv1.peerAddress(), taskDataBuilder, cc);
				fr.awaitUninterruptibly();
				assertEquals(true, fr.isSuccess());

				// Request data
				NavigableMap<Number640, Data> requestDataMap = (NavigableMap<Number640, Data>) fr.request().dataMap(0).dataMap();
				assertEquals(NumberUtils.allSameKey("VALUE1"), (Number640) requestDataMap.get(NumberUtils.STORAGE_KEY).object());
				assertEquals(sender.peerID(), (Number160) new Data(((NavigableMap<Number640, byte[]>) requestDataMap.get(NumberUtils.OLD_BROADCAST).object()).get(NumberUtils.allSameKey("SENDERID"))).object());
				assertEquals(Type.REQUEST_2, fr.request().type());

				// Response data
				if (i >= 0 && i < 3) { // only here it should retrive the data.
					NavigableMap<Number640, Data> responseDataMap = (NavigableMap<Number640, Data>) fr.responseMessage().dataMap(0).dataMap();
					assertEquals("VALUE1", (String) responseDataMap.get(NumberUtils.allSameKey("VALUE1")).object());
					assertEquals(Type.OK, fr.responseMessage().type());
					// Local storage --> check that the count was increased and put increased into the storage
					checkStoredObjectState("VALUE1", 3, (i + 1));
					checkListeners(sender, mrBCHandler1, (i + 1));
				} else { // Here data should be null...
					assertEquals(null, fr.responseMessage().dataMap(0));
					assertEquals(Type.NOT_FOUND, fr.responseMessage().type());
					// Local storage --> check that the count stays up at max
					checkStoredObjectState("VALUE1", 3, 3);
					checkListeners(sender, mrBCHandler1, 3);
				}
			}

			// Now try to invoke one listener and then try to get the data again
			Field peerConnectionActiveFlagRemoveListenersField = MapReduceBroadcastHandler.class.getDeclaredField("peerConnectionActiveFlagRemoveListeners");
			peerConnectionActiveFlagRemoveListenersField.setAccessible(true);
			List<PeerConnectionActiveFlagRemoveListener> listeners = (List<PeerConnectionActiveFlagRemoveListener>) peerConnectionActiveFlagRemoveListenersField.get(mrBCHandler1);
			int listenerIndex = new Random().nextInt(listeners.size());
			listeners.get(listenerIndex).turnOffActiveOnDataFlag(sender.peerAddress(), NumberUtils.allSameKey("VALUE1"));
			listeners.remove(listeners.get(listenerIndex));
			// ==========================================================

			cc.shutdown().await();

		} finally {
			if (cc != null) {
				cc.shutdown().await();
			}
			if (sender != null) {
				sender.shutdown().await();
			}
			if (recv1 != null) {
				recv1.shutdown().await();
			}
			// Now all close listener should get invoked that still can be invoked --> should release the value and make it available again for all those connections who's activeFlag is true (2 connections)
			checkStoredObjectState("VALUE1", 3, 1);
			checkListeners(sender, mrBCHandler1, 2);
			FutureChannelCreator fcc = recv1.connectionBean().reservation().create(0, 1);
			fcc.awaitUninterruptibly();
			cc = fcc.channelCreator();
		}
	}

	private void checkListeners(Peer sender, MapReduceBroadcastHandler mrBCHandler1, int nrOfListeners) throws NoSuchFieldException, IllegalAccessException {
		// also check the state of the listeners...
		Field peerConnectionActiveFlagRemoveListenersField = MapReduceBroadcastHandler.class.getDeclaredField("peerConnectionActiveFlagRemoveListeners");
		peerConnectionActiveFlagRemoveListenersField.setAccessible(true);
		List<PeerConnectionActiveFlagRemoveListener> listeners = (List<PeerConnectionActiveFlagRemoveListener>) peerConnectionActiveFlagRemoveListenersField.get(mrBCHandler1);
		assertEquals(nrOfListeners, listeners.size());
		/*
		 * private AtomicBoolean activeOnDataFlag; private Number640 keyToObserve; private PeerAddress peerAddressToObserve;
		 */
		for (PeerConnectionActiveFlagRemoveListener l : listeners) {
			Field activeOnDataFlagField = l.getClass().getDeclaredField("activeOnDataFlag");
			activeOnDataFlagField.setAccessible(true);
			Field keyToObserveField = l.getClass().getDeclaredField("keyToObserve");
			keyToObserveField.setAccessible(true);
			Field peerAddressToObserveField = l.getClass().getDeclaredField("peerAddressToObserve");
			peerAddressToObserveField.setAccessible(true);

			assertEquals(true, ((AtomicBoolean) activeOnDataFlagField.get(l)).get());
			assertEquals(NumberUtils.allSameKey("VALUE1"), ((Number640) keyToObserveField.get(l)));
			assertEquals(sender.peerAddress(), (PeerAddress) peerAddressToObserveField.get(l));
		}
	}

	private void checkStoredObjectState(String value, int nrOfExecutions, int currentNrOfExecutions) throws NoSuchFieldException, ClassNotFoundException, IOException, IllegalAccessException {
		Field valueField = DataStorageObject.class.getDeclaredField("value");
		valueField.setAccessible(true);
		Field nrOfExecutionsField = DataStorageObject.class.getDeclaredField("nrOfExecutions");
		nrOfExecutionsField.setAccessible(true);
		Field currentnrOfExecutionsField = DataStorageObject.class.getDeclaredField("currentNrOfExecutions");
		currentnrOfExecutionsField.setAccessible(true);
		DataStorageObject dst = (DataStorageObject) TaskRPC.storage().get(NumberUtils.allSameKey(value)).object();
		assertEquals(value, (String) valueField.get(dst));
		assertEquals(nrOfExecutions, (int) nrOfExecutionsField.get(dst));
		assertEquals(currentNrOfExecutions, (int) currentnrOfExecutionsField.get(dst));
		System.err.println("TaskRPCTest.checkStoredObjectState(): dst.toString(): " + dst.toString());
	}

}
