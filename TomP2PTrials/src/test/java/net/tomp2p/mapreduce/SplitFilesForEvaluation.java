package net.tomp2p.mapreduce;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.List;

public class SplitFilesForEvaluation {
	public static void main(String[] args) {
//		String inputLocation = //
		
	}
	
	public void split(String keyfilePath, int maxFileSize, String fileEncoding, List<String> splitFiles){
		try {
			RandomAccessFile aFile = new RandomAccessFile(keyfilePath, "r");
			FileChannel inChannel = aFile.getChannel();
			ByteBuffer buffer = ByteBuffer.allocate(maxFileSize);
			// int filePartCounter = 0;
			String split = "";
			String actualData = "";
			String remaining = "";
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

				if (split.getBytes(Charset.forName(fileEncoding)).length >= maxFileSize) {
					actualData = split.substring(0, split.lastIndexOf(" ")).trim();
					remaining = split.substring(split.lastIndexOf(" ") + 1, split.length()).trim();
				} else {
					actualData = split.trim();
					remaining = "";
				} 

				splitFiles.add(actualData);
				buffer.clear();
				split = "";
				actualData = "";
			}
			inChannel.close();
			aFile.close();
		} catch (Exception e) {
			System.out.println("Exception on reading file at location: " + keyfilePath);
			e.printStackTrace();
		}
	}
}
