package net.tomp2p.mapreduce;

import mapreduce.storage.DHTWrapper;

public class Main2 {
	public static void main(String[] args) throws Exception {

		DHTWrapper dht = DHTWrapper.create("192.168.1.147", 4003, 4003);
		MapReduceBroadcastHandler broadcastHandler = new MapReduceBroadcastHandler(dht);
		dht.broadcastHandler(broadcastHandler);
//		try {
			dht.connect();

//			while (!dht.peerDHT().peer().isShutdown()) {
//				System.out.println("Sleeping 1s til shutdown");
//				Thread.sleep(1000);
//			}
//		} catch (Exception e) {
//			dht.shutdown();
//		} finally {
//			dht.shutdown();
//		}
	}

}
