package net.tomp2p.mapreduce.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class TestInformationGatherUtils {

	/// home/ozihler/git/mt2/TomP2PTrials/src/main/java/net/tomp2p/mapreduce/examplejob
	private static String path = new File("").getAbsolutePath() + "/src/main/java/net/tomp2p/mapreduce/examplejob/";
	private static List<String> info = Collections.synchronizedList(new ArrayList<>());

	public static void addLogEntry(String entry) {
		info.add("[" + DateFormat.getDateTimeInstance().format(new Date()) + "] " + entry);
	}

	public static void writeOut() {
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File(path+"log_[" + DateFormat.getDateTimeInstance().format(new Date()) + "].txt")));
			for (String i : info) {
				writer.write(i + "\n");
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
