package net.tomp2p.mapreduce.utils;

import java.io.Serializable;

public final class DataStorageTriple implements Serializable {
	private static final long serialVersionUID = 4597498234385313114L;

	private final Object value;
	private final int nrOfExecutions;
	private int currentNrOfExecutions;

	public DataStorageTriple(final Object value, final int nrOfExecutions, final int currentNrOfExecutions) {

		this.value = value;
		this.nrOfExecutions = nrOfExecutions;
		this.currentNrOfExecutions = currentNrOfExecutions;
	}

	public Object value() {
		return this.value;
	}

	public int nrOfExecutions() {
		return this.nrOfExecutions;
	}

	public int currentNrOfExecutions() {
		return this.currentNrOfExecutions;
	}

	/**
	 * 
	 * @return the actual value if it can be executed. Else returns null.
	 */
	public Object tryIncrementCurrentNrOfExecutions() {
		if (nrOfExecutions < this.currentNrOfExecutions) {
			++this.currentNrOfExecutions;
			return value;
		} else {
			return null;
		}
	}

	/**
	 * Decrements the number of executions of this value. Allows a value to become executable again. Used when a peer does not complete execution. 
	 * Should not be used when peer completed execution (number of execution should stay as high as the successful execution).
	 */
	public void tryDecrementCurrentNrOfExecutions() {
		if (this.currentNrOfExecutions > 0) {
			--this.currentNrOfExecutions;
		}
	}

}
