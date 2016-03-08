package net.tomp2p.mapreduce;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeerConnectionActiveFlagRemoveListener {
	private static final Logger LOG = LoggerFactory.getLogger(PeerConnectionActiveFlagRemoveListener.class);

	private AtomicBoolean activeOnDataFlag;
	private Triple toAcquire;

	public PeerConnectionActiveFlagRemoveListener(Triple toAcquire, AtomicBoolean activeOnDataFlag) {
		this.toAcquire = toAcquire;
		this.activeOnDataFlag = activeOnDataFlag;
	}

	public boolean turnOffActiveOnDataFlag(Triple received) throws Exception {
		if (this.toAcquire.equals(received)) {
			LOG.info("Received triple I'm observing: active set to false for triple [" + toAcquire + "]!");
			activeOnDataFlag.set(false);
			return true;
		} else {
			LOG.info("Ignored triple: listener observes: [" + toAcquire + "] but received: [" + received + "]");
			return false;
		}
	}

	public Triple triple() { 
		return toAcquire;
	}

}
