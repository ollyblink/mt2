package mapreduce.execution.tasks.taskdatacomposing;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import mapreduce.utils.FileSize;

public class MaxFileSizeTaskDataComposer implements ITaskDataComposer {
	private FileSize maxFileSize = FileSize.THIRTY_TWO_KILO_BYTES;
	private String fileEncoding = "UTF-8";
	private String splitValue = "\n";

	// private String data = "";
	private String remainingData = "";

	public static MaxFileSizeTaskDataComposer create() {
		return new MaxFileSizeTaskDataComposer();
	}

	// @Override
	// public String append(String line) {
	// String newData = this.data + line + this.splitValue;
	// long newFileSizeCounter = newData.getBytes(Charset.forName(this.fileEncoding)).length;
	// if (newFileSizeCounter >= maxFileSize.value()) {
	// String currentData = this.data;
	// this.data = "";
	// this.data = line + this.splitValue;
	// if (currentData.trim().length() == 0) {
	// return null;
	// } else {
	// return currentData;
	// }
	// } else {
	// this.data = newData;
	// return null;
	// }
	// }
	//
	// @Override
	// public void reset() {
	// this.data = "";
	// }

	@Override
	public String fileEncoding() {
		return this.fileEncoding;
	}

	public MaxFileSizeTaskDataComposer fileEncoding(String fileEncoding) {
		this.fileEncoding = fileEncoding;
		return this;
	}

	public MaxFileSizeTaskDataComposer maxFileSize(FileSize maxFileSize) {
		this.maxFileSize = maxFileSize;
		return this;
	}

	public MaxFileSizeTaskDataComposer splitValue(String splitValue) {
		this.splitValue = splitValue;
		return this;
	}

	// @Override
	// public String currentValues() {
	// return this.data;
	// }

	@Override
	public List<String> splitToSize(String toSplit) {
		List<String> splits = new ArrayList<>();
		String split = "";

		StringTokenizer tokens = new StringTokenizer(toSplit);
		while (tokens.hasMoreTokens()) {
			String nextToken = tokens.nextToken();
			if ((split + nextToken + " ").getBytes(Charset.forName(this.fileEncoding)).length >= maxFileSize
					.value()) {
				splits.add(split.trim());
				split = "";
			}
			split += nextToken + " ";
		}

		this.remainingData = "";
		if (split.length() > 0) {
			this.remainingData = split.trim();
		}
		return splits;
	}

	@Override
	public String remainingData() {
		return this.remainingData;
	}

	@Override
	public FileSize maxFileSize() {
		return maxFileSize;
	}

	@Override
	public String splitValue() {
		return splitValue;
	}

	@Override
	public void reset() {
		this.remainingData = "";
		this.splitValue = "";
	}

}
