package net.tomp2p.mapreduce.utils;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.executors.JobSubmissionExecutor;
import mapreduce.storage.DHTWrapper;
import net.tomp2p.mapreduce.FutureTask;
import net.tomp2p.peers.Number160;

public class FileSplitter {
	private static Logger logger = LoggerFactory.getLogger(JobSubmissionExecutor.class);

	/**
	 * Splits a text file located at keyFilePath into pieces of at max maxFileSize. Words are not split! Meaning, this method is appropriate for word count
	 * 
	 * @param keyfilePath
	 * @param dht
	 *            connection to put it into the dht
	 * @param maxFileSize
	 * @param fileEncoding
	 *            (e.g. UTF-8)
	 * @return a map containing all generated dht keys of the file splits to retrieve them together with the FuturePut to be called in Futures.whenAllSucess(...)
	 */
	public static Map<Number160, FutureTask> splitWithWordsAndWrite(String keyfilePath, DHTWrapper dht, int nrOfExecutions, Number160 domainKey, int maxFileSize, String fileEncoding) {
		Map<Number160, FutureTask> dataKeysAndFuturePuts = Collections.synchronizedMap(new HashMap<>());
		System.out.println("Filepath: " + keyfilePath);
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
				// System.out.println("Put data: " + actualData + ", remaining data: " + remaining);
				Number160 dataKey = Number160.createHash(actualData);
  
				dataKeysAndFuturePuts.put(dataKey, dht.put(dataKey, domainKey, actualData, nrOfExecutions));

				buffer.clear();
				split = "";
				actualData = "";
			}
			inChannel.close();
			aFile.close();
		} catch (Exception e) {
			logger.warn("Exception on reading file at location: " + keyfilePath, e);
		}

		return dataKeysAndFuturePuts;
	}

}
