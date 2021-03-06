package mapreduce.engine.componenttests;

import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import org.junit.Before;
import org.junit.Test;

import mapreduce.engine.broadcasting.broadcasthandlers.JobSubmissionBroadcastHandler;
import mapreduce.engine.executors.JobSubmissionExecutor;
import mapreduce.engine.messageconsumers.JobSubmissionMessageConsumer;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.jobs.PriorityLevel;
import mapreduce.execution.procedures.WordCountMapper;
import mapreduce.execution.procedures.WordCountReducer;
import mapreduce.storage.DHTWrapper;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.FileSize;
import mapreduce.utils.FileUtils;

/**
 * This is only the submitter, use the ExecutorMain before to set up the Calculation nodes
 * 
 * @author Oliver
 *
 */
public class SystemInteractionTest {
	private static Random random = new Random();

	private static void write(String loc, int nrOfTokens, int nrOfTokenRepetitions) throws IOException {
		String messageToWrite = "";
		Path logFile = Paths.get(loc);
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8)) {
			for (int i = 0; i < nrOfTokens; ++i) {
				for (int j = 0; j < nrOfTokenRepetitions - 1; ++j) {
					messageToWrite += i + " ";
				}
				messageToWrite += i + "\n";

				writer.write(messageToWrite);
				messageToWrite = "";
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void test() throws Exception {
		int nrOfFiles = 1;
		int nrOfWords = 100;
		int nrOfReps = 1;
		// String jsMapper =
		// FileUtils.INSTANCE.readLines(System.getProperty("user.dir") +
		// "/src/main/java/mapreduce/execution/procedures/wordcountmapper.js");
		// System.out.println(jsMapper);
		// String jsReducer =
		// FileUtils.INSTANCE.readLines(System.getProperty("user.dir") +
		// "/src/main/java/mapreduce/execution/procedures/wordcountreducer.js");
		// System.out.println(jsReducer);
		String fileInputFolderPath = System.getProperty("user.dir") + "/src/test/java/generictests/files/";

		for (int i = 0; i < nrOfFiles; ++i) {
			write(fileInputFolderPath + "test_" + i + ".txt", nrOfWords, nrOfReps);

		}
		int other = random.nextInt((32000-1025)) + 1025;
		JobSubmissionBroadcastHandler submitterBCHandler = JobSubmissionBroadcastHandler.create();

		IDHTConnectionProvider dhtCon = DHTWrapper.create("192.168.43.65", 4442, other).broadcastHandler(submitterBCHandler)
		// .storageFilePath(System.getProperty("user.dir")
		// +
		// "/src/test/java/mapreduce/engine/componenttests/storage/submitter/")
		;

		// JobSubmissionExecutor submissionExecutor = JobSubmissionExecutor.create().dhtConnectionProvider(dhtCon);

		JobSubmissionMessageConsumer submissionMessageConsumer = JobSubmissionMessageConsumer.create().dhtConnectionProvider(dhtCon)
		// .executor(submissionExecutor)
		;

		submitterBCHandler.messageConsumer(submissionMessageConsumer);

		dhtCon.connect();
		String resultOutputFolderPath = System.getProperty("user.dir") + "/src/test/java/generictests/outfiles/";
		// job = Job.create("S1", PriorityLevel.MODERATE).maxFileSize(FileSize.THIRTY_TWO_BYTES).addSucceedingProcedure(WordCountMapper.create(), null, 1, 1, false, false)
		// .addSucceedingProcedure(WordCountReducer.create(), null, 1, 1, false, false).calculatorTimeoutSpecification(2000, true, 2.0);
		Job job = Job.create(JobSubmissionExecutor.classId, PriorityLevel.MODERATE).submitterTimeoutSpecification(15000, false, 0.0).calculatorTimeoutSpecification(2000, true, 2.0)
				.maxFileSize(FileSize.MEGA_BYTE).fileInputFolderPath(fileInputFolderPath, Job.DEFAULT_FILE_ENCODING).resultOutputFolder(resultOutputFolderPath, FileSize.MEGA_BYTE)
				.addSucceedingProcedure(WordCountMapper.create()).addSucceedingProcedure(WordCountReducer.create());

		// .addSucceedingProcedure(TestWaitingProcedure.create())
		// .addSucceedingProcedure(SumSummer.create())
		;
		long before = System.currentTimeMillis();
		submissionMessageConsumer.executor().submit(job);
		while (!submissionMessageConsumer.executor().jobIsRetrieved(job)) {
			Thread.sleep(100);
		}
		long after = System.currentTimeMillis();
		long diff = after - before;
		System.err.println("Finished after " + diff + " ms");
		System.err.println("Waiting for retrieval to finish");
		Thread.sleep(5000);
		List<String> pathVisitor = new ArrayList<>();
		FileUtils.INSTANCE.getFiles(new File(fileInputFolderPath), pathVisitor);
		List<String> txts = new ArrayList<>();
		for (String path : pathVisitor) {
			txts.add(FileUtils.INSTANCE.readLines(path));
		}
		HashMap<String, Integer> counter = getCounts(txts);

		pathVisitor.clear();
		String outFolder = resultOutputFolderPath + "tmp";
		FileUtils.INSTANCE.getFiles(new File(outFolder), pathVisitor);
		String resultFileToCheck = FileUtils.INSTANCE.readLines(pathVisitor.get(0));
		System.err.println("===========RESULTTEXT=============");
		System.err.println(resultFileToCheck);
		System.err.println("==================================");
		for (String key : counter.keySet()) {
			Integer count = counter.get(key);
			System.err.println("resultFileToCheck.contains(" + key + "\t" + count + ")? " + resultFileToCheck.contains(key + "\t" + count));
			assertEquals(true, resultFileToCheck.contains(key + "\t" + count));
		}

		// FileUtils.INSTANCE.deleteFilesAndFolder(outFolder, pathVisitor);
		// Thread.sleep(Long.MAX_VALUE);
		System.err.println("Shutting down executor in 5 seconds");
		Thread.sleep(15000);
		System.out.println("shutting down submitter");
		dhtCon.shutdown();
		Thread.sleep(5000);
	}

	private static HashMap<String, Integer> getCounts(List<String> txts) {
		HashMap<String, Integer> res = new HashMap<>();
		for (String txt : txts) {
			StringTokenizer tokens = new StringTokenizer(txt);
			while (tokens.hasMoreTokens()) {
				String word = tokens.nextToken();
				Integer count = res.get(word);
				if (count == null) {
					count = 0;
				}
				res.put(word, ++count);
			}
		}
		return res;
	}

}
