package net.tomp2p.mapreduce;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;

public class PeerConnectionActiveFlagRemoveListener {
	private static final Logger LOG = LoggerFactory.getLogger(PeerConnectionActiveFlagRemoveListener.class);

	private AtomicBoolean activeOnDataFlag;

	private Triple toAcquire;
	// private Number640 keyToObserve;
	// private PeerAddress peerAddressToObserve;
	// private int acquireNumber;

	// peerAddressToObserve, Number640 keyToObserve, int acquireNumber,
	public PeerConnectionActiveFlagRemoveListener(Triple toAcquire, AtomicBoolean activeOnDataFlag) {
		this.toAcquire = toAcquire;
		// this.peerAddressToObserve = peerAddressToObserve;
		// this.keyToObserve = keyToObserve;
		this.activeOnDataFlag = activeOnDataFlag;
		// this.acquireNumber = acquireNumber;
	}

	public boolean turnOffActiveOnDataFlag(Triple toAcquire) throws Exception {
		if (this.toAcquire.equals(toAcquire)) {
			LOG.info("active set to false for triple [" + toAcquire + "]!");
			activeOnDataFlag.set(false);
			return true;
		} else {
			// LOG.info("Wrong peer or key. Ignored. this listener's PeerConnection observes: [" + peerAddressToObserve.peerId().intValue() + "] but received peer: [" + peerAddress.peerId().intValue() + "] and key [" + keyToObserve.intValue() + "] but received key: [" + recKey.intValue()
			// + "]");
			return false;
		}
	}

}
