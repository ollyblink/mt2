package net.tomp2p.mapreduce.utils;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.NavigableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.executors.JobSubmissionExecutor;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class FileReaderUtils {
	private static Logger logger = LoggerFactory.getLogger(FileReaderUtils.class);

//	public static NavigableMap<Number640, Data> readData(String location) {
//		try {
//			RandomAccessFile aFile = new RandomAccessFile(keyfilePath, "r");
//			FileChannel inChannel = aFile.getChannel();
//			ByteBuffer buffer = ByteBuffer.allocate(maxFileSize);
//			int filePartCounter = 0;
//			String split = "";
//			String actualData = "";
//			String remaining = "";
//			while (inChannel.read(buffer) > 0) {
//				buffer.flip();
//				// String all = "";
//				// for (int i = 0; i < buffer.limit(); i++) {
//				byte[] data = new byte[buffer.limit()];
//				buffer.get(data);
//				// }
//				// System.out.println(all);
//				split = new String(data);
//				split = remaining += split;
//
//				remaining = "";
//				// System.out.println(all);
//				logger.info("Split has size: " + split.getBytes(Charset.forName(fileEncoding)).length);
//				// Assure that words are not split in parts by the buffer: only
//				// take the split until the last occurrance of " " and then
//				// append that to the first again
//
//				if (split.getBytes(Charset.forName(fileEncoding)).length >= maxFileSize) {
//					actualData = split.substring(0, split.lastIndexOf(" "));
//					remaining = split.substring(split.lastIndexOf(" ") + 1, split.length());
//				} else {
//					actualData = split;
//					remaining = "";
//				}
//				submitInternally(startProcedure, outputJPD, dataInputDomain, keyfilePath, filePartCounter++,
//						actualData);
//				buffer.clear(); // do something with the data and clear/compact
//								// it.
//				split = "";
//				actualData = "";
//			}
//			inChannel.close();
//			aFile.close();
//		} catch (Exception e) {
//			logger.warn("Exception on reading file at location: " + keyfilePath, e);
//		}
//	}

}
