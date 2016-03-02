package net.tomp2p.mapreduce;

import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;

public interface IPeerConnectionActiveFlagRemoveListener {

	public void turnOffActiveOnDataFlag(PeerAddress p, Number640 storageKey) throws Exception;

}
