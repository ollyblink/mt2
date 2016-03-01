package net.tomp2p.mapreduce;

import java.util.NavigableMap;

import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.mapreduce.utils.DataStorageObject;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class TaskBuilder extends DefaultConnectionConfiguration {

	private boolean isForceTCP;
	private Number640 key;
	private DataStorageObject dataStorageTriple;
	private NavigableMap<Number640, Data> broadcastInput;

	public TaskBuilder() {
	}

	public TaskBuilder key(Number160 locationKey, Number160 domainKey) {
		this.key = new Number640(locationKey, domainKey, null, null);
		return this;
	}

	public Number640 key() {
		return this.key;
	}

	public TaskBuilder dataStorageTriple(DataStorageObject dataStorageTriple) {
		this.dataStorageTriple = dataStorageTriple;
		return this;
	}

	public DataStorageObject dataStorageTriple() {
		return this.dataStorageTriple;
	}

	public TaskBuilder isForceTCP(boolean isForceTCP) {
		this.isForceTCP = isForceTCP;
		return this;
	}

	public boolean isForceTCP() {
		return this.isForceTCP;
	}

	public TaskBuilder broadcastInput(NavigableMap<Number640, Data> broadcastInput) {
		this.broadcastInput = broadcastInput;
		return this;
	}

	public NavigableMap<Number640, Data> broadcastInput() {
		return broadcastInput;
	}

}
