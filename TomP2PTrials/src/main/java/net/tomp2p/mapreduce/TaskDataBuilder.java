package net.tomp2p.mapreduce;

import java.util.NavigableMap;

import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.mapreduce.utils.DataStorageObject;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class TaskDataBuilder extends DefaultConnectionConfiguration {

	private boolean isForceTCP;
	private Number640 storageKey;
	private DataStorageObject dataStorageTriple;
	private NavigableMap<Number640, Data> broadcastInput;

	public TaskDataBuilder() {
	}

	public TaskDataBuilder storageKey(Number640 storageKey) {
		this.storageKey = storageKey;
		return this;
	}

	public Number640 key() {
		return this.storageKey;
	}

	public TaskDataBuilder dataStorageTriple(DataStorageObject dataStorageTriple) {
		this.dataStorageTriple = dataStorageTriple;
		return this;
	}

	public DataStorageObject dataStorageTriple() {
		return this.dataStorageTriple;
	}

	public TaskDataBuilder isForceTCP(boolean isForceTCP) {
		this.isForceTCP = isForceTCP;
		return this;
	}

	public boolean isForceTCP() {
		return this.isForceTCP;
	}

	public TaskDataBuilder broadcastInput(NavigableMap<Number640, Data> broadcastInput) {
		this.broadcastInput = broadcastInput;
		return this;
	}

	public NavigableMap<Number640, Data> broadcastInput() {
		return broadcastInput;
	}

}