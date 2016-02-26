package net.tomp2p.mapreduce;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import mapreduce.storage.DHTConnectionProvider;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.Futures;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class Main {
	public static void main(String[] args) throws Exception {
		Task startTask = new Task(null, NumberUtils.next()) {
			DHTConnectionProvider dht = DHTConnectionProvider.create("", 1, 1);

			@Override
			public void broadcastReceiver(NavigableMap<Number640, Data> input) throws Exception {

				final List<FuturePut> puts = new ArrayList<>();
				// Put data
				String text = (String) input.get(NumberUtils.allSameKey("DATA1")).object();
				Number160 dataKey = Number160.createHash(text);

				puts.add(dht.put(dataKey, text));

				// Put job
				Number160 jobKey = Number160.createHash("JOBKEY");
				puts.add(dht.put(jobKey, input.get(NumberUtils.allSameKey("JOBKEY"))));

				Futures.whenAllSuccess(puts).addListener(new BaseFutureAdapter<BaseFuture>() {

					@Override
					public void operationComplete(BaseFuture future) throws Exception {
						if (future.isSuccess()) {
							NavigableMap<Number640, Data> newInput = new TreeMap<>(); 
							newInput.put(NumberUtils.allSameKey("NEXTTASK"), input.get("MAPTASKID"));
							newInput.put(NumberUtils.allSameKey("DATA1"), new Data(dataKey));
							newInput.put(NumberUtils.allSameKey("JOBKEY"), new Data(jobKey));
							dht.broadcast(Number160.createHash("NEW JOB"), newInput);
						} else {
							// Do nothing
						}
					}
				});
			}

		};

		Task mapTask = new Task(startTask.currentId(), NumberUtils.next()) {

			DHTConnectionProvider dht = DHTConnectionProvider.create("", 1, 1);
			@Override
			public void broadcastReceiver(NavigableMap<Number640, Data> input) throws Exception {
				Number160 dataKey= (Number160) input.get(NumberUtils.allSameKey("DATA1")).object();
				
				dht.get(dataKey).addListener(new BaseFutureAdapter<FutureGet>(){

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						if(future.isSuccess()){
							String text = (String) future.data().object();
							String[] tokens = text.split(" ");
							List<FuturePut> futurePuts = new ArrayList<>();
							for(String token: tokens){
								futurePuts.add(dht.add(token, 1, dht.peerDHT().peer().peerID()));
							}
							
						}
					}
					
				});
			}

		};

		Task reduceTask = new Task(mapTask.currentId(), NumberUtils.next()) {

			@Override
			public void broadcastReceiver(NavigableMap<Number640, Data> input) throws Exception {
				// TODO Auto-generated method stub

			}

		};

		Task writeTask = new Task(reduceTask.currentId(), NumberUtils.next()) {

			@Override
			public void broadcastReceiver(NavigableMap<Number640, Data> input) throws Exception {
				// TODO Auto-generated method stub

			}

		};

		Job job = new Job();
		job.addTask(startTask);
		job.addTask(mapTask);
		job.addTask(reduceTask);
		job.addTask(writeTask);

		NavigableMap<Number640, Data> input = new TreeMap<>();
		input.put(NumberUtils.allSameKey("MAPTASKID"), new Data(mapTask.currentId()));
		input.put(NumberUtils.allSameKey("DATA1"), new Data("this is a text file"));
		input.put(NumberUtils.allSameKey("JOBKEY"), new Data(job.serialize()));
		job.start(input);
	}

}
