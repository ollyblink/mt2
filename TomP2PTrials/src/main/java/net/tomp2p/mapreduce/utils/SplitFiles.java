package net.tomp2p.mapreduce.utils;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import mapreduce.utils.FileSize;
import mapreduce.utils.FileUtils;
import net.tomp2p.peers.Number160;

public class SplitFiles {
	public static void main(String[] args) throws Exception {
		String inputFilePath = "/home/ozihler/Desktop/files/toSplit";
		String outputLocation = "/home/ozihler/Desktop/files/evaluation/8MB";
		// List<Integer> splitSizes = new ArrayList<>();
		Charset charset = Charset.forName("ISO-8859-1");

		List<String> pathVisitor = new ArrayList<>();
		FileUtils.INSTANCE.getFiles(new File(inputFilePath), pathVisitor);
		int cntr = 0;
		String split = ""; 

		for (String filePath : pathVisitor) {
			System.err.println(filePath);
//			FileUtils.INSTANCE.readLinesFromFile(filePath, charset);
			try {
				RandomAccessFile aFile = new RandomAccessFile(filePath, "r");
				FileChannel inChannel = aFile.getChannel();
				ByteBuffer buffer = ByteBuffer.allocate((int) (FileSize.EIGHT_MEGA_BYTES.value()));
				while (inChannel.read(buffer) > 0) {
					buffer.flip();
					byte[] data = new byte[buffer.limit()];
					buffer.get(data);
					System.err.println("split.getBytes(charset).length?" +(split.getBytes(charset).length+">="+ FileSize.EIGHT_MEGA_BYTES.value()));
					split += new String(data);
					if (split.getBytes(charset).length >= FileSize.EIGHT_MEGA_BYTES.value()) {
						File file = new File(outputLocation + "/file_" + (cntr++) + ".txt");
						RandomAccessFile raf = new RandomAccessFile(file, "rw");
						raf.write(split.getBytes(charset), 0, FileSize.EIGHT_MEGA_BYTES.value());
						raf.close();
						buffer.clear();
						split = "";
					}
				}
				inChannel.close();
				aFile.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// return dataKeysAndFuturePuts;
	}
}
