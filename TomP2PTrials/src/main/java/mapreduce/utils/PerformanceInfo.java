package mapreduce.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

/**
 * 
 * @author Oliver
 *
 */
public class PerformanceInfo implements Comparable<PerformanceInfo>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5508383518176166845L;
	/** Value calculated from executing a certain benchmark using the performBenchmark() method. The smaller the value, the better*/
	private Long benchmarkResultValue;
	/** A random number assigned to minimise probability of undecidability if the benchmark value for both computers are the same */
	private Long randomNr;

	private PerformanceInfo() {
		performBenchmark();
	}

	private void performBenchmark() {
		int size = 1000000;
		ArrayList<Integer> values = new ArrayList<>(size);
		Random generator = new Random();
		for (int i = 0; i < size; i++) {
			values.add(generator.nextInt(size));
		}
		Long start = System.currentTimeMillis();
		Collections.sort(values);
		Long end = System.currentTimeMillis();
		benchmarkResultValue = end - start;
		randomNr = generator.nextLong();
	}

	public static PerformanceInfo create() {
		return new PerformanceInfo();
	}

	@Override
	public int compareTo(PerformanceInfo o2) {
		int comparisonResult = benchmarkResultValue.compareTo(o2.benchmarkResultValue);
		if (comparisonResult == 0) {
			return randomNr.compareTo(o2.randomNr);
		} else {
			return comparisonResult;
		}
	}

	@Override
	public String toString() {
		return "b(" + benchmarkResultValue + "), rnd(" + randomNr + ")";
	}

}
