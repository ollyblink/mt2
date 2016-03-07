package net.tomp2p.mapreduce;

import java.util.NavigableMap;
import java.util.TreeMap;

import mapreduce.storage.DHTWrapper;
import net.tomp2p.mapreduce.examplejob.MapTask;
import net.tomp2p.mapreduce.examplejob.PrintTask;
import net.tomp2p.mapreduce.examplejob.ReduceTask;
import net.tomp2p.mapreduce.examplejob.ShutdownTask;
import net.tomp2p.mapreduce.examplejob.StartTask;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class Main {

	// public static class StartTask

	// public static class MapTask

	// public static class ReduceTask

	// public static class PrintTask

	// public static class ShutdownTask

	public static void main(String[] args) throws Exception {
		int nrOfShutdownMessagesToAwait = 2;

		// String filesPath = new File("").getAbsolutePath() + "/src/test/java/net/tomp2p/mapreduce/testfiles/";
		String filesPath = "/home/ozihler/Desktop/files/splitFiles/testfiles";
		Job job = new Job();
		Task startTask = new StartTask(null, NumberUtils.next());
		Task mapTask = new MapTask(startTask.currentId(), NumberUtils.next());
		Task reduceTask = new ReduceTask(mapTask.currentId(), NumberUtils.next());
		Task writeTask = new PrintTask(reduceTask.currentId(), NumberUtils.next());
		Task initShutdown = new ShutdownTask(writeTask.currentId(), NumberUtils.next(), nrOfShutdownMessagesToAwait);

		job.addTask(startTask);
		job.addTask(mapTask);
		job.addTask(reduceTask);
		job.addTask(writeTask);
		job.addTask(initShutdown);

		NavigableMap<Number640, Data> input = new TreeMap<>();
		input.put(NumberUtils.allSameKey("INPUTTASKID"), new Data(startTask.currentId()));
		input.put(NumberUtils.allSameKey("MAPTASKID"), new Data(mapTask.currentId()));
		input.put(NumberUtils.allSameKey("REDUCETASKID"), new Data(reduceTask.currentId()));
		input.put(NumberUtils.allSameKey("WRITETASKID"), new Data(writeTask.currentId()));
		input.put(NumberUtils.allSameKey("SHUTDOWNTASKID"), new Data(initShutdown.currentId()));
		input.put(NumberUtils.allSameKey("DATAFILEPATH"), new Data(filesPath));
		input.put(NumberUtils.allSameKey("JOBKEY"), new Data(job.serialize()));

//		DHTWrapper dht = DHTWrapper.create("192.168.1.147", 4003, 4004);
//		// DHTWrapper dht = DHTWrapper.create("192.168.1.171", 4004, 4004);
//		MapReduceBroadcastHandler broadcastHandler = new MapReduceBroadcastHandler(dht);
//		dht.broadcastHandler(broadcastHandler);
//		dht.connect();

		// job.mapReduceBroadcastHandler(MapReduceBroadcastHandler.class);
//		job.start(input, dht);

	}

}
