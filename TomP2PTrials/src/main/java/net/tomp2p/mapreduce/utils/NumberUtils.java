package net.tomp2p.mapreduce.utils;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;

public class NumberUtils {
	private static int counter = 0;

	public static Number640 next() {
		++counter;
		return new Number640(Number160.createHash(counter), Number160.createHash(counter),
				Number160.createHash(counter), Number160.createHash(counter));
	}

	public static void reset() {
		counter = 0;
	}

	public static Number640 allSameKey(String string) {
		return new Number640(Number160.createHash(string), Number160.createHash(string), Number160.createHash(string),
				Number160.createHash(string));
	}
}
