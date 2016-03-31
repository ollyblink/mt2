package net.tomp2p.mapreduce.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mapreduce.utils.FileUtils;

public class DataExtractionParser {
	private static class Pair {
		String req;
		int msgs;
		double mb;
	}

	public static void main(String[] args) throws IOException {
		List<String> pathVisitor = new ArrayList<String>();
		FileUtils.INSTANCE.getFiles(new File(new File("").getAbsolutePath() + "/src/main/java/net/tomp2p/mapreduce/outputfiles"), pathVisitor);
		Map<String, List<Pair>> vals = new HashMap<>();
		for (String filePath : pathVisitor) {
			List<Pair> fileVals = new ArrayList<>();
			vals.put(filePath.substring(filePath.indexOf("Job[") + "Job[".length(), filePath.indexOf("]_log")), fileVals);
			ArrayList<String> lines = FileUtils.INSTANCE.readLinesFromFile(filePath, Charset.forName("UTF-8"));
			for (int i = 0; i < lines.size(); ++i) {
				String line = lines.get(i);
				if (line.contains("READLOG.READLOG2()")) {
					for (int j = i + 1; j < lines.size(); ++j) {
						String line2 = lines.get(j);
						System.err.println("LINE2 " + line2);
						if (!line2.startsWith("Overall size: [")) {
							if (line2.contains("#msgs")) {
								Pair pair = new Pair();
								pair.req = line2.substring(0, line2.indexOf(": #msgs["));
								pair.msgs = Integer.parseInt(line2.substring(line2.indexOf("#msgs[") + "#msgs[".length(), line2.indexOf("], overall")));
								pair.mb = Double.parseDouble(line2.substring(line2.indexOf("overall size[") + "overall size[".length(), line2.indexOf("]MB")));
								fileVals.add(pair);
							}
						} else {
							System.err.println("LINE2: " + line2);
							Pair pair = new Pair();
							pair.req = "OVERALL_SIZE";
							pair.mb = Double.parseDouble(line2.substring(line2.indexOf("Overall size: [") + "Overall size: [".length(), line2.indexOf("]MB")));
							fileVals.add(pair);
							i = j + 2;
							break;
						}
					}
				}
				if (line.contains("[MapTask]")) {
					Pair pair = new Pair();
					pair.req = "#MAPTASKS";
					pair.mb = Integer.parseInt(line.substring(line.indexOf("and finished #[") + "and finished #[".length(), line.indexOf("].")));
					fileVals.add(pair);
				}
			}
		}

		String title = "jobid,";
		String subtitle = " ,";
		int counter = 0;
		Map<String, String> jobLines = new HashMap<>();
		for (String jobID : vals.keySet()) {
			String line = "";
			for (Pair p : vals.get(jobID)) {
				if (counter++ <= vals.get(jobID).size()) {
					title += p.req + ",";
					subtitle += "#msgs;;;mb,";

				}
				line += (p.msgs != 0 ? p.msgs + ";;;" : ";;;") + (p.mb > 0.0 ? p.mb + "," : ",");

			}
			jobLines.put(jobID, line);
		}

		BufferedWriter writer = new BufferedWriter(new FileWriter(new File("JOBSOUT.txt")));
		writer.write(title + "\n");
		writer.write(subtitle + "\n");
		System.err.println(title);
		System.err.println(subtitle);
		for (String j : jobLines.keySet()) {
			writer.write(j + "," + jobLines.get(j) + "\n");
			System.err.println(j + "," + jobLines.get(j));
		}
		writer.close();
	}

}
