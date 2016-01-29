package mapreduce.utils;

import java.io.Serializable;

/**
 * To avoid problem of add().list() in dht that doesn't work
 * 
 * @author ozihler
 *
 */
public final class Value implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6638101010868136679L;
	/** An ID String to distinguish the different values in case there are multiple ones*/
	private final String id;
	private final Object value;

	public Value(final Object value) {
		this.id = IDCreator.INSTANCE.createTimeRandomID(getClass().getSimpleName())+"_"+IDCreator.INSTANCE.createTimeRandomID(value.getClass().getSimpleName());
		this.value = value;
	}

	public Object value() {
		return this.value;
	}

	@Override
	public String toString() {
		return "<" + id + ", " + value + ">";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
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
		Value other = (Value) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

}
