package net.tomp2p.mapreduce.utils;

import java.io.Serializable;
import java.util.NavigableMap;

import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public final class DataStorageObject implements Serializable {
	private static final long serialVersionUID = 4597498234385313114L;

	private final Object value;
	private final int nrOfExecutions;
	private int currentNrOfExecutions;
	private NavigableMap<Number640, Data> broadcast;

	public DataStorageObject(final Object value, final int nrOfExecutions) {
		this.value = value;
		this.nrOfExecutions = nrOfExecutions;
		this.currentNrOfExecutions = 0;
	}

	/**
	 * 
	 * @return the actual value if it can be executed. Else returns null.
	 */
	public Object tryIncrementCurrentNrOfExecutions() {
		if (nrOfExecutions < (this.currentNrOfExecutions)) {
			++this.currentNrOfExecutions;
			return value;
		} else {
			return null;
		}
	}

	/**
	 * Decrements the number of executions of this value. Allows a value to become executable again. Used when a peer does not complete execution. Should not be used when peer completed execution (number of execution should stay as high as the successful execution).
	 */
	public void tryDecrementCurrentNrOfExecutions() {
		if (this.currentNrOfExecutions > 0) {
			--this.currentNrOfExecutions;
		}
	}

}