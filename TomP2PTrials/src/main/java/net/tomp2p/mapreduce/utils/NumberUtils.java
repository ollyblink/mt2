package net.tomp2p.mapreduce.utils;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;

public class NumberUtils {
	public static final Number640 STORAGE_KEY = NumberUtils.allSameKey("STORAGE_KEY");
	public static final Number640 VALUE = NumberUtils.allSameKey("VALUE");
	public static final Number640 OLD_BROADCAST = NumberUtils.allSameKey("OLD_BROADCAST");
	public static final Number640 RECEIVERS = allSameKey("RECEIVERS");
	public static final Number640 CURRENT_TASK = allSameKey("CURRENT_TASK");
	public static final Number640 NEXT_TASK = allSameKey("NEXT_TASK");
	private static int counter = 0;

	public static Number640 next() {
		++counter;
		return new Number640(Number160.createHash(counter), Number160.createHash(counter), Number160.createHash(counter), Number160.createHash(counter));
	}

	public static void reset() {
		counter = 0;
	}

	public static Number640 allSameKey(String string) {
		return new Number640(Number160.createHash(string), Number160.createHash(string), Number160.createHash(string), Number160.createHash(string));
	}

	public static Number640 allSameKey(int nr) {
		return new Number640(Number160.createHash(nr), Number160.createHash(nr), Number160.createHash(nr), Number160.createHash(nr));
	}
}
