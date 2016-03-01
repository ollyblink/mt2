package net.tomp2p.mapreduce;

import net.tomp2p.peers.Number640;

public interface IBroadcastListener {

	public void inform(Number640 key) throws Exception;

}
