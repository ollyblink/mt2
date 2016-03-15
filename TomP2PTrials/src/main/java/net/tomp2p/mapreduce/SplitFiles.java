package net.tomp2p.mapreduce;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import mapreduce.utils.FileSize;
import net.tomp2p.peers.Number160;

public class SplitFiles {
	public static void main(String[] args) throws Exception {
		String inputFilePath = "/home/ozihler/Desktop/files/evaluation/1File/";
		String outputLocation = "/home/ozihler/Desktop/files/evaluation/512kb/";
		// List<Integer> splitSizes = new ArrayList<>();
		Charset charset = Charset.forName("ISO-8859-1");

		for (int i = 1; i <= 32; i = i * 2) {
			// System.err.println("Filepath: " + keyfilePath);
			String keyfilePath = inputFilePath + i + "mb.txt";
			try {
				RandomAccessFile aFile = new RandomAccessFile(keyfilePath, "r");
				FileChannel inChannel = aFile.getChannel();
				ByteBuffer buffer = ByteBuffer.allocate(FileSize.MEGA_BYTE.value()/2);
				// int filePartCounter = 0;
				String split = "";
				String actualData = "";
				String remaining = "";
				int splitCount = 1;
				while (inChannel.read(buffer) > 0) {
					buffer.flip();
					// String all = "";
					// for (int i = 0; i < buffer.limit(); i++) {
					byte[] data = new byte[buffer.limit()];
					buffer.get(data);
					// }
					// System.out.println(all);
					split = new String(data);
					split = remaining += split;

					remaining = "";
					// System.out.println(all);
					// Assure that words are not split in parts by the buffer: only
					// take the split until the last occurrance of " " and then
					// append that to the first again

					if (split.getBytes(charset).length >= FileSize.MEGA_BYTE.value()/2) {
						actualData = split.substring(0, split.lastIndexOf(" ")).trim();
						remaining = split.substring(split.lastIndexOf(" ") + 1, split.length()).trim();

						File file = new File(outputLocation + (i + "mb_" + (splitCount++) + ".txt"));
						RandomAccessFile raf = new RandomAccessFile(file, "rw");
						raf.write(actualData.getBytes(charset));
						raf.close();
						actualData = "";
					} else {
						actualData = split.trim();
						remaining = "";
					}

					buffer.clear();
				}
//				if (remaining.getBytes(charset).length >= 0) {
////					actualData = split.substring(0, split.lastIndexOf(" ")).trim();
////					remaining = split.substring(split.lastIndexOf(" ") + 1, split.length()).trim();
//
//					File file = new File(outputLocation + (i + "mb_" + (splitCount++) + ".txt"));
//					RandomAccessFile raf = new RandomAccessFile(file, "rw");
//					raf.write(remaining.getBytes(charset));
//					raf.close();
////					actualData = "";
//				}
				inChannel.close();
				aFile.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// return dataKeysAndFuturePuts;
	}
}
