package net.tomp2p.mapreduce.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

public class TestInformationGatherUtils {

	/// home/ozihler/git/mt2/TomP2PTrials/src/main/java/net/tomp2p/mapreduce/examplejob
	private static String path = new File("").getAbsolutePath() + "/src/main/java/net/tomp2p/mapreduce/outputfiles/";
	private static Map<Long, String> info = Collections.synchronizedMap(new TreeMap<>());

	public static void addLogEntry(String entry) {
		info.put(System.currentTimeMillis(), entry);
	}

	public static void writeOut() {
		try {
			String fileName = path + "log_[" + System.currentTimeMillis() + "].txt";
			if (!new File(fileName).exists()) {
				new File(fileName).createNewFile();
			}
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File(fileName)));
			int cntr = 0;
			long start = -1, startMap = -1, end = 0;
			synchronized (info) {
				for (Long i : info.keySet()) {
					if (cntr == 0) {
						start = i;
					}
					if (cntr++ == info.keySet().size() - 1) {
						end = i;
					}
					if (info.get(i).contains("MAPTASK") && startMap == -1) {
						startMap = i;
					}
					writer.write("[" + DateFormat.getDateTimeInstance().format(new Date(i)) + "]" + info.get(i) + "\n");
					System.err.println("[" + DateFormat.getDateTimeInstance().format(new Date(i)) + "]" + info.get(i));
				}
			}
			writer.write("Job execution time: " + (end - start) + "ms \n");
			writer.write("Job execution time from Map: " + (end - startMap) + "ms\n");
			writer.close();
			System.err.println("Job execution time: " + (end - start) + "ms");
			System.err.println("Job execution time from Map: " + (end - startMap) + "ms");

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
