package net.tomp2p.mapreduce.utils;

import java.io.Serializable;

import mapreduce.utils.IDCreator;

public final class MapReduceValue implements Serializable {
	private static final long serialVersionUID = 4597498234385313114L;
	private final String id;
	private final Object value;
	private final int nrOfExecutions;
	private int currentNrOfExecutions;

	public MapReduceValue(final Object value, final int nrOfExecutions) {
		if (value == null) {
			throw new NullPointerException("Value cannot be null");
		}
		this.value = value;
		this.id = IDCreator.INSTANCE.createTimeRandomID(MapReduceValue.class.getSimpleName() + "_" + value);
		this.nrOfExecutions = (nrOfExecutions <= 1 ? 1 : nrOfExecutions);
		this.currentNrOfExecutions = 0;
	}

	/**
	 * 
	 * @return the actual value if it can be executed. Else returns null.
	 */
	public Object tryAcquireValue() {
		if (nrOfExecutions > this.currentNrOfExecutions) {
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

	@Override
	public String toString() {
		return "MapReduceValue([" + value + "], #execs[" + nrOfExecutions + "], #current[" + currentNrOfExecutions + "])";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + currentNrOfExecutions;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + nrOfExecutions;
		result = prime * result + ((value == null) ? 0 : value.hashCode());
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
		MapReduceValue other = (MapReduceValue) obj;
		if (currentNrOfExecutions != other.currentNrOfExecutions)
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (nrOfExecutions != other.nrOfExecutions)
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

}
