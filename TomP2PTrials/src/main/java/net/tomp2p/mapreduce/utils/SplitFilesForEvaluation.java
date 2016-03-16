package net.tomp2p.mapreduce.utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import mapreduce.utils.FileSize;
import mapreduce.utils.FileUtils;

public class SplitFilesForEvaluation {
	public static void main(String[] args) throws Exception {
		// String inputLocation = "/home/ozihler/Desktop/files/toSplit/";
		String outputLocation = "/home/ozihler/Desktop/files/evaluation/";
		// List<String> pathVisitor = new ArrayList<>();
		// FileUtils.INSTANCE.getFiles(new File(inputLocation), pathVisitor);
		// List<String> splitFiles = new ArrayList<>();
		Charset charset = Charset.forName("ISO-8859-1");
		// for (String file : pathVisitor) {
		// splitFiles.addAll(
		String fileToSplit = FileUtils.INSTANCE.readLines(outputLocation + "1File/initial.txt", charset);
		// );
		// }
		//
		for (int i = 12; i <= 12; i = i * 2) {
			int currentFileSize = i * FileSize.MEGA_BYTE.value();
			System.out.println(i + " MB");
			// String all = ""; 
			// for (String s : splitFiles) {
			File file = new File(outputLocation + "1File/" + (currentFileSize / FileSize.MEGA_BYTE.value()) + "mb.txt");
//			long length = file.length();
//			if (length > currentFileSize) {
//				break;
//			}
			RandomAccessFile raf = new RandomAccessFile(file, "rw");
//			raf.seek(length);
			raf.write(fileToSplit.getBytes(charset), 0, currentFileSize);
			raf.close();
		}
		System.out.println("Finished");
		// if (all.getBytes(charset).length > currentFileSize) {

		// all = "";
		// break;
		// }
		// }
	}

	// public static void main(String[] args) throws Exception {
	// RandomAccessFile raf = new RandomAccessFile("test.csv", "r");
	// long numSplits = 10; // from user input, extract it from args
	// long sourceSize = raf.length();
	// long bytesPerSplit = sourceSize / numSplits;
	// long remainingBytes = sourceSize % numSplits;
	//
	// int maxReadBufferSize = 8 * 1024; // 8KB
	// for (int destIx = 1; destIx <= numSplits; destIx++) {
	// BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream("split." + destIx));
	// if (bytesPerSplit > maxReadBufferSize) {
	// long numReads = bytesPerSplit / maxReadBufferSize;
	// long numRemainingRead = bytesPerSplit % maxReadBufferSize;
	// for (int i = 0; i < numReads; i++) {
	// readWrite(raf, bw, maxReadBufferSize);
	// }
	// if (numRemainingRead > 0) {
	// readWrite(raf, bw, numRemainingRead);
	// }
	// } else {
	// readWrite(raf, bw, bytesPerSplit);
	// }
	// bw.close();
	// }
	// if (remainingBytes > 0) {
	// BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream("split." + (numSplits + 1)));
	// readWrite(raf, bw, remainingBytes);
	// bw.close();
	// }
	// raf.close();
	// }
	//
	// static void readWrite(RandomAccessFile raf, BufferedOutputStream bw, long numBytes) throws IOException {
	// byte[] buf = new byte[(int) numBytes];
	// int val = raf.read(buf);
	// if (val != -1) {
	// bw.write(buf);
	// }
	// }
}
