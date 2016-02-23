package mapreduce.engine.executors.performance;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import mapreduce.utils.PerformanceInfo;

public class PerformanceInfoTest {

	@Test
	public void test() {
		// Hard to test this on a computer with the same execution, so the only thing to test here is if the two info items are NOT the same, such that it can be used to distinguish data sets.
		// The criteria is that these two are never equal, meaning the comparison will (close to never) never return 0.
		// It's a very rare case where this is actually used anyways so shouldn't make such a difference
		for (int i = 0; i < 1; ++i) {
			PerformanceInfo info1 = PerformanceInfo.create();
			PerformanceInfo info2 = PerformanceInfo.create();

			assertEquals(true, info1.compareTo(info2) != 0);
			System.err.println("Tested " + i + " times: info1: " + info1 + " vs. info2: " + info2);
		}
	}

}
