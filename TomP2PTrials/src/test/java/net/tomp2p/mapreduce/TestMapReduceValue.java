package net.tomp2p.mapreduce;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;

import org.junit.Test;

import net.tomp2p.mapreduce.utils.MapReduceValue;

public class TestMapReduceValue {

	@Test(expected = NullPointerException.class)
	public void testLargerZeroNrOfExecutions() {
		// Tests if the nr is executions is always larger than one in case it is set to <= 0 in the constructor
		for (int i = -100; i <= 100; ++i) {
			try {
				MapReduceValue o = new MapReduceValue("VALUE", i);
				Field field = MapReduceValue.class.getDeclaredField("nrOfExecutions");
				field.setAccessible(true);
				int nrOfExecutions = (int) field.get(o);
				if (i <= 0) {
					assertEquals(1, nrOfExecutions);
				} else {
					assertEquals(i, nrOfExecutions);
				}
			} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		// Throws null pointer
		new MapReduceValue(null, 1);

	}

	@Test
	public void testTryIncrementDecrement() {

		// Object should be retrievable 3 times, not more
		String storageValue = "VALUE";
		MapReduceValue object = new MapReduceValue(storageValue, 3);
		assertEquals(storageValue, (String) object.tryIncrementCurrentNrOfExecutions());
		assertEquals(storageValue, (String) object.tryIncrementCurrentNrOfExecutions());
		assertEquals(storageValue, (String) object.tryIncrementCurrentNrOfExecutions());
		assertEquals(null, object.tryIncrementCurrentNrOfExecutions());
		assertEquals(null, object.tryIncrementCurrentNrOfExecutions());
		assertEquals(null, object.tryIncrementCurrentNrOfExecutions());
		assertEquals(null, object.tryIncrementCurrentNrOfExecutions());

		// Decrement once allows for one more increment
		object.tryDecrementCurrentNrOfExecutions();
		assertEquals(storageValue, (String) object.tryIncrementCurrentNrOfExecutions());
		assertEquals(null, object.tryIncrementCurrentNrOfExecutions());
		assertEquals(null, object.tryIncrementCurrentNrOfExecutions());
		assertEquals(null, object.tryIncrementCurrentNrOfExecutions());
		assertEquals(null, object.tryIncrementCurrentNrOfExecutions());

		// Decrement twice allows for two more increments
		object.tryDecrementCurrentNrOfExecutions();
		object.tryDecrementCurrentNrOfExecutions();
		assertEquals(storageValue, (String) object.tryIncrementCurrentNrOfExecutions());
		assertEquals(storageValue, (String) object.tryIncrementCurrentNrOfExecutions());
		assertEquals(null, object.tryIncrementCurrentNrOfExecutions());
		assertEquals(null, object.tryIncrementCurrentNrOfExecutions());
		assertEquals(null, object.tryIncrementCurrentNrOfExecutions());
		assertEquals(null, object.tryIncrementCurrentNrOfExecutions());

		// Decrement 3 times allows for 3 more increments
		object.tryDecrementCurrentNrOfExecutions();
		object.tryDecrementCurrentNrOfExecutions();
		object.tryDecrementCurrentNrOfExecutions();
		assertEquals(storageValue, (String) object.tryIncrementCurrentNrOfExecutions());
		assertEquals(storageValue, (String) object.tryIncrementCurrentNrOfExecutions());
		assertEquals(storageValue, (String) object.tryIncrementCurrentNrOfExecutions());
		assertEquals(null, object.tryIncrementCurrentNrOfExecutions());
		assertEquals(null, object.tryIncrementCurrentNrOfExecutions());
		assertEquals(null, object.tryIncrementCurrentNrOfExecutions());
		assertEquals(null, object.tryIncrementCurrentNrOfExecutions());

		// Should not go below zero... Thus, decrementing more than three times should still only allow for 3 increments
		object.tryDecrementCurrentNrOfExecutions();
		object.tryDecrementCurrentNrOfExecutions();
		object.tryDecrementCurrentNrOfExecutions();
		object.tryDecrementCurrentNrOfExecutions();
		object.tryDecrementCurrentNrOfExecutions();
		object.tryDecrementCurrentNrOfExecutions();
		object.tryDecrementCurrentNrOfExecutions();
		assertEquals(storageValue, (String) object.tryIncrementCurrentNrOfExecutions());
		assertEquals(storageValue, (String) object.tryIncrementCurrentNrOfExecutions());
		assertEquals(storageValue, (String) object.tryIncrementCurrentNrOfExecutions());
		assertEquals(null, object.tryIncrementCurrentNrOfExecutions());
		assertEquals(null, object.tryIncrementCurrentNrOfExecutions());
		assertEquals(null, object.tryIncrementCurrentNrOfExecutions());
		assertEquals(null, object.tryIncrementCurrentNrOfExecutions());

	}

}
