package net.tomp2p.mapreduce;

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.mockito.Mockito;

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
			TaskDataBuilder taskDataBuilder = new TaskDataBuilder().storageKey(key).dataStorageObject(dataStorageTriple).isForceTCP(true);

			// Await future response...
			FutureResponse fr = taskRPC.putTaskData(sender.peerAddress(), taskDataBuilder, cc);
			fr.awaitUninterruptibly();
			assertEquals(true, fr.isSuccess());

			// Test request msgs content
			Message rM = fr.request();
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
		try {
			// Store some data for the test directly
			TaskRPC.storage().put(NumberUtils.allSameKey("VALUE1"), new Data(new DataStorageObject("VALUE1", 3)));

			sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start();
			recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();

			FutureChannelCreator fcc = recv1.connectionBean().reservation().create(0,2);
			fcc.awaitUninterruptibly();
			cc = fcc.channelCreator();

			MapReduceBroadcastHandler mrBCHandler1 = Mockito.mock(MapReduceBroadcastHandler.class);
			MapReduceBroadcastHandler mrBCHandler2 = Mockito.mock(MapReduceBroadcastHandler.class);
			TaskRPC taskRPC = new TaskRPC(recv1.peerBean(), recv1.connectionBean(), mrBCHandler1);
			new TaskRPC(sender.peerBean(), sender.connectionBean(), mrBCHandler2);
			// Not in the dht --> NOT FOUND
			TaskDataBuilder taskDataBuilder = new TaskDataBuilder().storageKey(NumberUtils.allSameKey("XYZ")).isForceTCP(true);
			FutureResponse fr = taskRPC.getTaskData(sender.peerAddress(), taskDataBuilder, cc);
			fr.awaitUninterruptibly();
			assertEquals(true, fr.isSuccess());
			assertEquals(NumberUtils.allSameKey("XYZ"), (Number640) fr.request().dataMap(0).dataMap().get(NumberUtils.STORAGE_KEY).object());
			assertEquals(Type.NOT_FOUND, fr.responseMessage().type());

			// in the dht --> Acquire once
			taskDataBuilder.storageKey(NumberUtils.allSameKey("VALUE1"));
			fr = taskRPC.getTaskData(sender.peerAddress(), taskDataBuilder, cc);
			fr.awaitUninterruptibly();
			assertEquals(true, fr.isSuccess());
			assertEquals(NumberUtils.allSameKey("VALUE1"), (Number640) fr.request().dataMap(0).dataMap().get(NumberUtils.STORAGE_KEY).object());
			assertEquals(Type.OK, fr.responseMessage().type());

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

}
