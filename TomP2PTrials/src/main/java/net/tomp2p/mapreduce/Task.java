/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.tomp2p.mapreduce;

import java.io.Serializable;
import java.util.Map;
import java.util.NavigableMap;

import mapreduce.storage.DHTWrapper;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

/**
 *
 * @author draft
 */
public abstract class Task implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9198452155865807410L;
	private final Number640 previousId;
	private final Number640 currentId;

	public Task(Number640 previousId, Number640 currentId) {
		this.previousId = previousId;
		this.currentId = currentId;
	}

	public abstract void broadcastReceiver(NavigableMap<Number640, Data> input, PeerMapReduce pmr) throws Exception;

	public Number640 currentId() {
		return this.currentId;
	}

	public Number640 previousId() {
		return this.previousId;
	}

	public static void keepInputKeyValuePairs(NavigableMap<Number640, Data> input, Map<Number640, Data> keptInput, String[] keyStringsToKeep) {
		for (String keyString : keyStringsToKeep) {
			keptInput.put(NumberUtils.allSameKey(keyString), input.get(NumberUtils.allSameKey(keyString)));
		}
	}
}
