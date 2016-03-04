package net.tomp2p.mapreduce;

import java.io.Serializable;

import mapreduce.storage.DHTWrapper;
import net.tomp2p.message.Message;

public interface BroadcastReceiver extends Serializable {

	public void receive(Message message, DHTWrapper dht);

}
