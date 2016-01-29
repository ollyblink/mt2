package generictests;

import java.io.IOException;
import java.util.NavigableMap;

import mapreduce.engine.broadcasting.broadcasthandlers.JobCalculationBroadcastHandler;
import mapreduce.engine.broadcasting.messages.IBCMessage;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.StructuredBroadcastHandler;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class MyBroadcastHandler extends JobCalculationBroadcastHandler {

	public IBCMessage bcMessage;

	@Override
	public StructuredBroadcastHandler receive(Message message) {
		try {
			NavigableMap<Number640, Data> dataMap = message.dataMapList().get(0).dataMap();
			for (Number640 nr : dataMap.keySet()) {
				bcMessage = (IBCMessage) dataMap.get(nr).object();
			}
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		}
		return super.receive(message);
	}

	// Setter, Getter, Creator, Constructor follow below..
	public MyBroadcastHandler(int nrOfConcurrentlyExecutedBCMessages) {
		super(nrOfConcurrentlyExecutedBCMessages);
	}
}
