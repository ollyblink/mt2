package net.tomp2p.mapreduce;

import java.io.IOException;
import java.util.NavigableMap;

import mapreduce.storage.DHTConnectionProvider;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.mapreduce.utils.JobTransferObject;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.StructuredBroadcastHandler;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class MapReduceBroadcastHandler extends StructuredBroadcastHandler {
	private DHTConnectionProvider dht;
	private Job job = null;

	public MapReduceBroadcastHandler(DHTConnectionProvider dht) {
		super();
		this.dht = dht;
	}

	@Override
	public StructuredBroadcastHandler receive(Message message) {

		NavigableMap<Number640, Data> input = message.dataMapList().get(0).dataMap();
		try {
			getJobIfNull(input);
			if (job != null) {
				Task task = job.findTask((Number640) input.get(NumberUtils.allSameKey("NEXTTASK")).object());
				System.out.println("Current Task:" + task.getClass().getSimpleName());
				task.broadcastReceiver(input, dht);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return super.receive(message);
	}

	private void getJobIfNull(NavigableMap<Number640, Data> dataMap) throws ClassNotFoundException, IOException {
		if (job == null) {
			Number160 jobKey = (Number160) dataMap.get(NumberUtils.allSameKey("JOBKEY")).object();
			dht.get(jobKey).addListener(new BaseFutureAdapter<FutureGet>() {

				@Override
				public void operationComplete(FutureGet future) throws Exception {
					if (future.isSuccess()) {
						JobTransferObject serialized = (JobTransferObject) future.data().object();
						job = Job.deserialize(serialized);
						System.err.println("Found job " + job);
					} else {
						System.err.println("Could not find job");
					}
				}

			}).awaitUninterruptibly();
		}
	}
 

}
