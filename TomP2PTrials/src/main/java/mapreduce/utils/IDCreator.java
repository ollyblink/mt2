package mapreduce.utils;

import java.util.Random;

/**
 * More or less impossible to ever have colliding id's if I do it like that...
 * 
 * @author Oliver
 *
 */
public enum IDCreator {
	INSTANCE;
	private static long yetAnotherLocalCounter = 0;
	private final Random random = new Random();

	public String createTimeRandomID(final String name) { 
		// TS == Timestamp
		// RND == Random long
		// LC == local counter... just such that at least locally, the id's are counted
		return name.toUpperCase() + 
				"[TS(" + System.currentTimeMillis() + ")_RND(" + random.nextLong() + ")"
						+ "_LC(" + yetAnotherLocalCounter++ + ")]";
	}

}
