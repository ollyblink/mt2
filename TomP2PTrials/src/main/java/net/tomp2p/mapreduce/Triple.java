package net.tomp2p.mapreduce;

import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;

public class Triple {
	PeerAddress peerAddress;
	Number640 storageKey;
//	int nrOfAcquires;

	public Triple(PeerAddress peerAddress, Number640 storageKey) {
 		this.peerAddress = peerAddress;
		this.storageKey = storageKey;
//		this.nrOfAcquires = 0;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((peerAddress == null) ? 0 : peerAddress.hashCode());
		result = prime * result + ((storageKey == null) ? 0 : storageKey.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Triple other = (Triple) obj;
		if (peerAddress == null) {
			if (other.peerAddress != null)
				return false;
		} else if (!peerAddress.equals(other.peerAddress))
			return false;
		if (storageKey == null) {
			if (other.storageKey != null)
				return false;
		} else if (!storageKey.equals(other.storageKey))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Triple [peerAddress=" + peerAddress + ", storageKey=" + storageKey + "]";
	}
	
	

 
}
