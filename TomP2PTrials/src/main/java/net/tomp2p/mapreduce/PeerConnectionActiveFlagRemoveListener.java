package net.tomp2p.mapreduce;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;

public class PeerConnectionActiveFlagRemoveListener {
	private static final Logger LOG = LoggerFactory.getLogger(PeerConnectionActiveFlagRemoveListener.class);

	private AtomicBoolean activeOnDataFlag;
	private Number640 keyToObserve;
	private PeerAddress peerAddressToObserve;

	public PeerConnectionActiveFlagRemoveListener(PeerAddress peerAddressToObserve, Number640 keyToObserve, AtomicBoolean activeOnDataFlag) {
		this.peerAddressToObserve = peerAddressToObserve;
		this.keyToObserve = keyToObserve;
		this.activeOnDataFlag = activeOnDataFlag;
	}

	public boolean turnOffActiveOnDataFlag(PeerAddress peerAddress, Number640 recKey) throws Exception {
		if (peerAddressToObserve.equals(peerAddress) && keyToObserve.equals(recKey)) {
			LOG.info("active set to false for peer connection [" + peerAddressToObserve.peerId().intValue() + "] and key [" + keyToObserve.intValue() + "]!");
			activeOnDataFlag.set(false);
			return true;
		} else {
			LOG.info("Not correct peer or key. Ignored. this listener's PeerConnection to observe: [" + peerAddressToObserve.peerId().intValue() + "], received peer: [" + peerAddress.peerId().intValue() + "] and key to observe [" + keyToObserve.intValue() + "], received key: [" + recKey.intValue()
					+ "]");
			return false;
		}
	}

}
