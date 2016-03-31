package net.tomp2p.mapreduce.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import mapreduce.utils.FileUtils;
import net.tomp2p.peers.Number640;

public class TestInformationGatherUtils {

	/// home/ozihler/git/mt2/TomP2PTrials/src/main/java/net/tomp2p/mapreduce/examplejob
	private static String path = new File("").getAbsolutePath() + "/src/main/java/net/tomp2p/mapreduce/outputfiles/";
	private static Map<Long, String> info = Collections.synchronizedMap(new TreeMap<>());

	public static void addLogEntry(String entry) {
		info.put(System.currentTimeMillis(), entry);
	}

	public static void writeOut(String jobId, List<String> taskDetails) {
		// String data = "";
		try {
			String fileName = path + "Job[" + jobId + "] log_[" + DateFormat.getDateTimeInstance().format(new Date()) + "].txt";
			fileName = fileName.replace(":", "_").replace(",", "_").replace(" ", "_");
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
					// writer.write("[" + DateFormat.getDateTimeInstance().format(new Date(i)) + "]" + info.get(i) + "\n");
					System.err.println("[" + DateFormat.getDateTimeInstance().format(new Date(i)) + "]" + info.get(i));
				}
			}
			// writer.write("Job execution time: " + (end - start) + "ms \n");
			writer.write("Job execution time from Map: " + (end - startMap) + "ms\n");
			// data += "startMapEndTime=" + (end - startMap) + ",";
			ReadLog.readLog(writer);
			ReadLog.readLog2(writer);
			if (taskDetails != null) {
				synchronized (taskDetails) {
					writer.write("======================Jobdetails for job [" + jobId + "]\n");
					System.err.println("Jobdetails for job [" + jobId + "]");
					for (String taskDetail : taskDetails) {
						writer.write(taskDetail + "\n");
						System.err.println(taskDetail);
					}
				}
			}
			writer.close();
			System.err.println("Job execution time: " + (end - start) + "ms");
			System.err.println("Job execution time from Map: " + (end - startMap) + "ms");
			// synchronized (Sniffer.allVals) {
			// float sum = 0;
			// for (Integer i : Sniffer.allVals) {
			// sum += i;
			// }
			// System.err.println("Overall TCP packages size in MB: " + (sum / (1024f * 1024f)));
			// Sniffer.allVals.clear();
			// }

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	//
	// public static String readLog() {
	// Charset charset = Charset.forName("UTF-8");
	// ArrayList<String> logLines = FileUtils.INSTANCE.readLinesFromFile(new File("").getAbsolutePath() + "/p2p.log", charset);
	// Map<String, Map<String, List<Long>>> msgCounter = Collections.synchronizedMap(new TreeMap<>());
	// // for (int i = 0; i < logLines.size(); ++i) {
	// // String line = logLines.get(i);
	// // if (line.contains("DEBUG net.tomp2p.message.Decoder - Decoding of TomP2P starts now. Readable:")) {
	// // long size = Integer.parseInt(line.substring(line.lastIndexOf("Readable: ") + "Readable: ".length(), line.lastIndexOf(".")));
	// // String threadPart = line.substring(line.indexOf("[NETTY-TOMP2P - worker-client/server - -1-"), line.indexOf("DEBUG"));
	// // // System.err.println("Size: " + size);
	// String data = "";
	// for (int j = 0; j < logLines.size(); ++j) {
	// String nextLine = logLines.get(j);
	// if (nextLine.contains("DEBUG net.tomp2p.message.Decoder") && nextLine.contains("About to")) {
	// String startOfData = nextLine.substring(nextLine.indexOf("msgid="), nextLine.length());
	// String[] cnt = startOfData.split(",");
	// // System.err.println(startOfData);
	// String requestType = "";
	// String c = "";
	// long size = Long.parseLong(nextLine.substring(nextLine.lastIndexOf("Buffer to read: ") + "Buffer to read: ".length(), nextLine.lastIndexOf(".")));
	// String msgId = "";
	// for (String s : cnt) {
	// if (s.contains("msgid=")) {
	// msgId = s.replace("msgid=", "");
	// }
	//
	// if (s.contains("t=")) {
	// // System.err.println();
	// requestType = s.replace("t=", "");
	//
	// }
	// if (s.contains("c=")) {
	// c = s.replace("c=", "");
	// }
	// // if (s.contains("s=")) {
	// // System.err.println("s:" + s.replace("s=", ""));
	// // }
	// if (requestType.length() > 0 && c.length() > 0 && msgId.length() > 0) {
	// String concat = requestType + "_" + c;
	// // if (requestType.equals("REQUEST_1") && c.equals("GCM")) {
	// // concat = "PUT_REQUEST";
	// // }
	// // if (requestType.equals("REQUEST_2") && c.equals("GCM")) {
	// // concat = "GET_REQUEST";
	// // }
	// Map<String, List<Long>> map = msgCounter.get(concat);
	// if (map == null) {
	// map = Collections.synchronizedMap(new HashMap<>());
	// msgCounter.put(concat, map);
	// }
	// List<Long> vals = map.get(msgId);
	// if (vals == null) {
	// vals = Collections.synchronizedList(new ArrayList<>());
	// map.put(msgId, vals);
	// }
	// vals.add(size);
	// c = "";
	// requestType = "";
	// msgId = "";
	// }
	// }
	// // System.err.println();
	// // int msgId = Integer.parseInt(nextLine.substring(nextLine.indexOf("msgid=") + "msgid=", nextLine.indexOf("msgid=") + "msgid="));
	// // break;
	// // }
	// // }
	// }
	// }
	//
	// // Map<String, List<Long>> tree = Collections.synchronizedMap();
	//
	// long overallSize = 0;
	// for (String requestType : msgCounter.keySet()) {
	// long requestSize = 0;
	// Map<String, List<Long>> map = msgCounter.get(requestType);
	// for (String msgId : map.keySet()) {
	// List<Long> list = map.get(msgId);
	// long sum = 0;
	// for (Long l : list) {
	// sum += l;
	// }
	// overallSize += sum;
	// requestSize += sum;
	// }
	// data += "requesttype{"+requestType+",}";
	// System.err.println(requestType + ": #msgs[" + map.keySet().size() + "], overall size[" + (requestSize / (1024d * 1024d)) + "]MB");
	// }
	// System.err.println("Overall size: [" + (overallSize / (1024d * 1024d)) + "]MB");
	// }
}
