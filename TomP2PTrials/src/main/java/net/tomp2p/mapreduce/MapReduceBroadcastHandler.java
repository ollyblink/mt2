package net.tomp2p.mapreduce;

import java.io.IOException;
import java.util.NavigableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.storage.DHTConnectionProvider;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.Futures;
import net.tomp2p.mapreduce.utils.JobTransferObject;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.StructuredBroadcastHandler;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class MapReduceBroadcastHandler extends StructuredBroadcastHandler {
	private static Logger logger = LoggerFactory.getLogger(MapReduceBroadcastHandler.class);

	private DHTConnectionProvider dht;
	private Job job = null;

	public MapReduceBroadcastHandler(DHTConnectionProvider dht) {
		this.dht = dht;
	}

	@Override
	public StructuredBroadcastHandler receive(Message message) {

		NavigableMap<Number640, Data> input = message.dataMapList().get(0).dataMap();
		try {
			if (job == null) {
				Futures.whenAllSuccess(getJobIfNull(input)).addListener(new BaseFutureAdapter<BaseFuture>() {

					@Override
					public void operationComplete(BaseFuture future) throws Exception {
						if (future.isSuccess()) {
							tryExecuteTask(input);
						} else {
							logger.info("No success on job retrieval");
						}
					}

				});
			} else {
				tryExecuteTask(input);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return super.receive(message);
	}

	private void tryExecuteTask(NavigableMap<Number640, Data> input) throws ClassNotFoundException, IOException, Exception {
		Number160 senderId = (Number160) (input.get(NumberUtils.allSameKey("SENDERID")).object());
		Number640 currentTaskId = (Number640) input.get(NumberUtils.allSameKey("CURRENTTASKID")).object();
		Number640 initTaskId = (Number640) input.get(NumberUtils.allSameKey("INPUTTASKID")).object();

		if ((job != null && senderId.equals(dht.peerDHT().peer().peerID())) || (currentTaskId.equals(initTaskId))) {
			Task task = job.findTask((Number640) input.get(NumberUtils.allSameKey("NEXTTASK")).object());
			task.broadcastReceiver(input, dht);
		} else {
			logger.info("job==null? " + (job == null) + " || !(" + senderId + ").equals(" + dht.peerDHT().peer().peerID() + ")?" + (!input.get(NumberUtils.allSameKey("SENDERID")).equals(dht.peerDHT().peer().peerID())) + "||!currentTaskId.equals(initTaskId)?" + (!currentTaskId.equals(initTaskId)));
		}
	}

	private FutureGet getJobIfNull(NavigableMap<Number640, Data> dataMap) throws ClassNotFoundException, IOException {
		if (job == null) {
			Number160 jobKey = (Number160) dataMap.get(NumberUtils.allSameKey("JOBKEY")).object();
			return dht.get(jobKey).addListener(new BaseFutureAdapter<FutureGet>() {

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

			});
		} else {
			return null;
		}
	}

}
