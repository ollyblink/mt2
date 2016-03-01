package net.tomp2p.mapreduce;

import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.mapreduce.utils.DataStorageTriple;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;

public class TaskBuilder extends DefaultConnectionConfiguration {

	private boolean isForceTCP;
	private Number640 key;
	private DataStorageTriple dataStorageTriple;

	public TaskBuilder() {
	}

	public TaskBuilder key(Number160 locationKey, Number160 domainKey) {
		this.key = new Number640(locationKey, domainKey, null, null);
		return this;
	}

	public Number640 key() {
		return this.key;
	}

	public TaskBuilder dataStorageTriple(DataStorageTriple dataStorageTriple) {
		this.dataStorageTriple = dataStorageTriple;
		return this;
	}

	public DataStorageTriple dataStorageTriple() {
		return this.dataStorageTriple;
	}

	public TaskBuilder isForceTCP(boolean isForceTCP) {
		this.isForceTCP = isForceTCP;
		return this;
	}

	public boolean isForceTCP() {
		return this.isForceTCP;
	}

}
