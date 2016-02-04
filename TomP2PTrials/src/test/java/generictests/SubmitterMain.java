package generictests;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import mapreduce.engine.broadcasting.broadcasthandlers.JobSubmissionBroadcastHandler;
import mapreduce.engine.executors.JobSubmissionExecutor;
import mapreduce.engine.messageconsumers.JobSubmissionMessageConsumer;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.jobs.PriorityLevel;
import mapreduce.execution.procedures.WordCountMapper;
import mapreduce.execution.procedures.WordCountReducer;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.FileSize;
import mapreduce.utils.FileUtils;

public class SubmitterMain {
	private static Random random = new Random();

	public static void main(String[] args) throws Exception {
		String jsMapper = FileUtils.INSTANCE.readLines(System.getProperty("user.dir") + "/src/main/java/mapreduce/execution/procedures/wordcountmapper.js");
		// System.out.println(jsMapper);
		String jsReducer = FileUtils.INSTANCE.readLines(System.getProperty("user.dir") + "/src/main/java/mapreduce/execution/procedures/wordcountreducer.js");
		// System.out.println(jsReducer);

		int other = random.nextInt(40000) + 4000;

		JobSubmissionBroadcastHandler submitterBCHandler = JobSubmissionBroadcastHandler.create();

		IDHTConnectionProvider dhtCon = DHTConnectionProvider.create("192.168.43.65", Integer.parseInt(args[0]), other).broadcastHandler(submitterBCHandler)
		// .storageFilePath(System.getProperty("user.dir")
		// + "/src/test/java/mapreduce/engine/componenttests/storage/submitter/")
		;

		JobSubmissionExecutor submissionExecutor = JobSubmissionExecutor.create().dhtConnectionProvider(dhtCon);

		JobSubmissionMessageConsumer submissionMessageConsumer = JobSubmissionMessageConsumer.create().dhtConnectionProvider(dhtCon).executor(submissionExecutor);

		submitterBCHandler.messageConsumer(submissionMessageConsumer);

		dhtCon.connect();

		String fileInputFolderPath = System.getProperty("user.dir") + "/src/test/java/generictests/files/";
		String resultOutputFolderPath = System.getProperty("user.dir") + "/src/test/java/generictests/files/";
		Job job = Job.create(submissionExecutor.id(), PriorityLevel.MODERATE).submitterTimeToLive(60000).calculatorTimeToLive(5000).maxFileSize(FileSize.MEGA_BYTE)
				.fileInputFolderPath(fileInputFolderPath, Job.DEFAULT_FILE_ENCODING).resultOutputFolder(resultOutputFolderPath, FileSize.MEGA_BYTE)
				.addSucceedingProcedure(WordCountMapper.create(), WordCountReducer.create(), 1, 1, false, false).addSucceedingProcedure(WordCountReducer.create(), null, 1, 1, false, false);

		long before = System.currentTimeMillis();
		submissionExecutor.submit(job);
		while (!submissionExecutor.jobIsRetrieved(job)) {
			Thread.sleep(100);
		}
		long after = System.currentTimeMillis();
		long diff = after - before;
		System.err.println("Finished after " + diff + " ms");
		System.out.println("shutting down submitter");
		dhtCon.shutdown();
		List<String> pathVisitor = new ArrayList<>();
		FileUtils.INSTANCE.getFiles(new File(fileInputFolderPath), pathVisitor);
		String txt = FileUtils.INSTANCE.readLines(pathVisitor.get(0));
		HashMap<String, Integer> counter = getCounts(txt);

		String outFolder = resultOutputFolderPath + "/tmp";
		pathVisitor.clear();
		FileUtils.INSTANCE.getFiles(new File(outFolder), pathVisitor);
		txt = FileUtils.INSTANCE.readLines(pathVisitor.get(0));
		for (String key : counter.keySet()) {
			Integer count = counter.get(key);
			System.err.println(txt.contains(key + "\t" + count));
		}

		// FileUtils.INSTANCE.deleteFilesAndFolder(outFolder, pathVisitor);
		// Thread.sleep(Long.MAX_VALUE);
	}

	private static HashMap<String, Integer> getCounts(String txt) {
		HashMap<String, Integer> res = new HashMap<>();

		StringTokenizer tokens = new StringTokenizer(txt);
		while (tokens.hasMoreTokens()) {
			String word = tokens.nextToken();
			Integer count = res.get(word);
			if (count == null) {
				count = 0;
			}
			res.put(word, ++count);
		}

		return res;
	}

}
