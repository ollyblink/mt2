package mapreduce.engine.componenttests;

import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.broadcasting.broadcasthandlers.JobCalculationBroadcastHandler;
import mapreduce.engine.broadcasting.messages.CompletedBCMessage;
import mapreduce.engine.broadcasting.messages.IBCMessage;
import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.execution.context.DHTStorageContext;
import mapreduce.execution.context.IContext;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.jobs.PriorityLevel;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.procedures.WordCountMapper;
import mapreduce.execution.procedures.WordCountReducer;
import mapreduce.execution.tasks.Task;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.FileSize;
import mapreduce.utils.FileUtils;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number640;

public class JobCalculationComponentTest {
	private static Logger logger = LoggerFactory.getLogger(JobCalculationComponentTest.class);
	private JobCalculationExecutor calculationExecutor;
	private JobCalculationMessageConsumer calculationMessageConsumer;
	private JobCalculationBroadcastHandler executorBCHandler;
	private IDHTConnectionProvider dhtCon;

	@Before
	public void setUp() throws Exception {

	}

	@After
	public void tearDown() throws Exception {
		dhtCon.shutdown();
		Thread.sleep(1000);
	}

	private static class Tuple {

		public Tuple(Task task, Object value) {
			this.task = task;
			this.value = value;
		}

		Task task;
		Object value;
	}

	@Test
	public void testAllOnceOneInitialTaskOneWord() throws Exception {
		// ===========================================================================================================================================================
		// This is the simplest possible trial of the word count example.
		// Every task needs to be executed only once
		// Every procedure needs to be executed only once
		// There is only 1 initial task to execute
		// The task has only 1 word to count
		// Time to live before running out of time is set to Long.MAX_VALUE (should thus never run out of
		// time)
		// !!!!!!!!!!!!!!!!!!ADDitionally it filters out words with lower count than 2 (this is important as
		// it requires something to happen as no output data is produced, nor transferred
		// ===========================================================================================================================================================

		Job job = Job.create("S1", PriorityLevel.MODERATE).maxFileSize(FileSize.THIRTY_TWO_BYTES).addSucceedingProcedure(WordCountMapper.create(), WordCountReducer.create(), 1, 1, false, false)
				.calculatorTimeToLive(2000l).addSucceedingProcedure(WordCountReducer.create(2), null, 1, 1, false, false);

		List<Tuple> tasks = new ArrayList<>();
		tasks.add(new Tuple(Task.create("testfile1", "S1"), "hello hello world"));
		HashMap<String, Integer> res2 = filter(getCounts(tasks), 2);

		executeTest(job, tasks, res2, 1, 1);
	}

	@Test
	public void testAllOnceOneInitialTaskMultipleWords() throws Exception {
		// ===========================================================================================================================================================
		// This is the simplest possible trial of the word count example.
		// Every task needs to be executed only once
		// Every procedure needs to be executed only once
		// There is only 1 initial task to execute
		// The task has 9 words to count
		// Time to live before running out of time is set to Long.MAX_VALUE (should thus never run out of
		// time)
		// ===========================================================================================================================================================

		Job job = Job.create("S1", PriorityLevel.MODERATE).maxFileSize(FileSize.THIRTY_TWO_BYTES).addSucceedingProcedure(WordCountMapper.create(), WordCountReducer.create(), 1, 1, false, false)
				.calculatorTimeToLive(2000l).addSucceedingProcedure(WordCountReducer.create(), null, 1, 1, false, false);

		List<Tuple> tasks = new ArrayList<>();
		tasks.add(new Tuple(Task.create("testfile1", "S1"), "the quick fox jumps over the lazy brown dog"));
		HashMap<String, Integer> res = getCounts(tasks);
		executeTest(job, tasks, res, 1, 1);
	}

	@Test
	public void testAllOnceOneInitialTaskMultipleSameInitialTasks() throws Exception {
		// ===========================================================================================================================================================
		// This is the simplest possible trial of the word count example.
		// Every task needs to be executed only once
		// Every procedure needs to be executed only once
		// There is only 2 initial tasks to execute
		// The tasks have 9 words each (twice the same) to count
		// Time to live before running out of time is set to Long.MAX_VALUE (should thus never run out of
		// time)
		// ===========================================================================================================================================================

		Job job = Job.create("S1", PriorityLevel.MODERATE).maxFileSize(FileSize.THIRTY_TWO_BYTES).addSucceedingProcedure(WordCountMapper.create(), WordCountReducer.create(), 1, 1, false, false)
				.calculatorTimeToLive(2000l).addSucceedingProcedure(WordCountReducer.create(), null, 1, 1, false, false);

		List<Tuple> tasks = new ArrayList<>();
		tasks.add(new Tuple(Task.create("testfile1", "S1"), "the quick fox jumps over the lazy brown dog"));
		tasks.add(new Tuple(Task.create("testfile2", "S1"), "the quick fox jumps over the lazy brown dog"));
		HashMap<String, Integer> res = getCounts(tasks);
		executeTest(job, tasks, res, 1, 1);
	}

	@Test
	public void testAllOnceOneInitialTaskMultipleDifferentInitialTasks() throws Exception {
		// ===========================================================================================================================================================
		// This is the simplest possible trial of the word count example.
		// Every task needs to be executed only once
		// Every procedure needs to be executed only once
		// There is only 3 initial tasks to execute
		// The tasks have 8 words each (twice the same) to count
		// Time to live before running out of time is set to Long.MAX_VALUE (should thus never run out of
		// time)
		// ===========================================================================================================================================================

		Job job = Job.create("S1", PriorityLevel.MODERATE).maxFileSize(FileSize.THIRTY_TWO_BYTES).addSucceedingProcedure(WordCountMapper.create(), WordCountReducer.create(), 1, 1, false, false)
				.calculatorTimeToLive(2000l).addSucceedingProcedure(WordCountReducer.create(), null, 1, 1, false, false);

		List<Tuple> tasks = new ArrayList<>();
		int counter = 0;
		tasks.add(new Tuple(Task.create("testfile_" + counter++, "S1"), "the quick fox jumps over the lazy brown dog"));
		tasks.add(new Tuple(Task.create("testfile_" + counter++, "S1"), "sphinx of black quartz judge my vow"));
		tasks.add(new Tuple(Task.create("testfile_" + counter++, "S1"), "the five boxing wizards jump quickly"));
		HashMap<String, Integer> res = getCounts(tasks);
		executeTest(job, tasks, res, 1, 1);
	}

	@Test
	public void testAllOnceExternalInputFile() throws Exception {
		// ===========================================================================================================================================================
		// This is the simplest possible trial of the word count example.
		// Every task needs to be executed only once
		// Every procedure needs to be executed only once
		// There is an external file to be processed
		// Time to live before running out of time is set to Long.MAX_VALUE (should thus never run out of
		// time)
		// !!!!!!!!!!!!!!!!!!ADDitionally it filters out words with lower count than 10 (this is important as
		// it requires something to happen as no output data is produced, nor transferred
		// ===========================================================================================================================================================
		try {
			// int nrOfTokens = 100;
			// System.err.println("Before writing file with " + nrOfTokens + " tokens");
			String text = FileUtils.INSTANCE.readLines(System.getProperty("user.dir") + "/src/test/java/mapreduce/engine/componenttests/largerinputfiles/testfile.txt");
			// write(text, nrOfTokens);
			System.err.println("Before Reading file");

			int MAX_COUNT = 0;
			Job job = Job.create("S1", PriorityLevel.MODERATE).maxFileSize(FileSize.THIRTY_TWO_BYTES).addSucceedingProcedure(WordCountMapper.create(), WordCountReducer.create(), 1, 1, false, false)
					.calculatorTimeToLive(2000).addSucceedingProcedure(WordCountReducer.create(MAX_COUNT), null, 1, 1, false, false)
					// .addSucceedingProcedure(WordsWithSameCounts.create(), null, 1, 1, false, false)
					;

			List<Tuple> tasks = new ArrayList<>();
			int counter = 0;
			tasks.add(new Tuple(Task.create("testfile_" + counter++, "S1"), text));
			HashMap<String, Integer> res = getCounts(tasks);
			HashMap<String, Integer> res2 = filter(res, MAX_COUNT);
			System.err.println("Before Execution");
			executeTest(job, tasks, res2, 1, 1);
		} catch (NoSuchFileException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testMultipleExecutors() throws Exception {
		// ===========================================================================================================================================================
		// This is the simplest possible trial of the word count example.
		// Every task needs to be executed only once
		// Every procedure needs to be executed only once
		// There is an external file to be processed
		// Time to live before running out of time is set to Long.MAX_VALUE (should thus never run out of
		// time)
		// !!!!!!!!!!!!!!!!!!ADDitionally it filters out words with lower count than 10 (this is important as
		// it requires something to happen as no output data is produced, nor transferred
		// ===========================================================================================================================================================
		try {
			String text = FileUtils.INSTANCE.readLines(System.getProperty("user.dir") + "/src/test/java/mapreduce/engine/componenttests/largerinputfiles/testfile2.txt");
			// write(text, nrOfTokens);
			System.err.println("Before Reading file");

			int MAX_COUNT = 0;
			Job job = Job.create("S1", PriorityLevel.MODERATE).maxFileSize(FileSize.THIRTY_TWO_BYTES).addSucceedingProcedure(WordCountMapper.create(), WordCountReducer.create(), 1, 1, false, false)
					.calculatorTimeToLive(2000).addSucceedingProcedure(WordCountReducer.create(MAX_COUNT), null, 1, 1, false, false)
					// .addSucceedingProcedure(WordsWithSameCounts.create(), null, 1, 1, false, false)
					;

			List<Tuple> tasks = new ArrayList<>();
			int counter = 0;
			tasks.add(new Tuple(Task.create("testfile_" + counter++, "S1"), text));
			HashMap<String, Integer> res = getCounts(tasks);
			// HashMap<String, Integer> res2 = filter(res, MAX_COUNT);
			executeTest(job, tasks, res, 4, 4);
		} catch (NoSuchFileException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		try {
			write(System.getProperty("user.dir") + "\\src\\test\\java\\mapreduce\\engine\\componenttests\\largerinputfiles\\testfile2.txt", 200);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void write(String loc, int nrOfTokens) throws IOException {
		Random r = new Random();
		String messageToWrite = "";
		Path logFile = Paths.get(loc);
		try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8)) {
			for (int i = 1; i < nrOfTokens; i++) {
				messageToWrite = i + "_" + r.nextLong() + "\n";
				writer.write(messageToWrite);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private HashMap<String, Integer> filter(HashMap<String, Integer> res, int maxCount) {
		HashMap<String, Integer> res2 = new HashMap<>();
		for (String key : res.keySet()) {
			if (res.get(key) >= maxCount) {
				res2.put(key, res.get(key));
			}
		}
		return res2;
	}

	private HashMap<String, Integer> getCounts(List<Tuple> tasks) {
		HashMap<String, Integer> res = new HashMap<>();
		for (Tuple tuple : tasks) {
			String valueString = (String) tuple.value;
			StringTokenizer tokens = new StringTokenizer(valueString);
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

	private void executeTest(Job job, List<Tuple> tasks, Map<String, Integer> res, int executorCount, int bccount) throws ClassNotFoundException, IOException, InterruptedException {
		calculationExecutor = JobCalculationExecutor.create();

		calculationMessageConsumer = JobCalculationMessageConsumer.create(executorCount).executor(calculationExecutor);
		executorBCHandler = JobCalculationBroadcastHandler.create(bccount).messageConsumer(calculationMessageConsumer);
		// int bootstrapPort = 4001;
		dhtCon = TestUtils.getTestConnectionProvider(executorBCHandler);
		Thread.sleep(1000);
		// DHTConnectionProvider
		// .create("192.168.43.65", bootstrapPort, bootstrapPort).broadcastHandler(executorBCHandler)
		// .storageFilePath("C:\\Users\\Oliver\\Desktop\\storage")
		;
		dhtCon.broadcastHandler(executorBCHandler);
		calculationExecutor.dhtConnectionProvider(dhtCon);
		calculationMessageConsumer.dhtConnectionProvider(dhtCon);
		long start = System.currentTimeMillis();
		System.err.println("Executing for " + res.keySet().size() + " words");
		execute(job, tasks);

		while (executorBCHandler.jobFutures().isEmpty()) {
			System.err.println("sleeping while jobFutures is empty");
			Thread.sleep(1000);
		}
		job = executorBCHandler.jobFutures().keySet().iterator().next();

		long secs = 0;
		long interv = 5000;
		while (!job.isFinished()) {
			System.err.println("slept for " + (secs / 1000) + " secs.");
			Thread.sleep(interv);
			secs += interv;
		}
		long end = System.currentTimeMillis();
		FutureGet getKeys = dhtCon.getAll(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, executorBCHandler.getJob(job.id()).currentProcedure().dataInputDomain().toString()).awaitUninterruptibly();
		if (getKeys.isSuccess()) {
			Set<Number640> keySet = getKeys.dataMap().keySet();
			List<String> resultKeys = new ArrayList<>();
			for (Number640 keyN : keySet) {
				String outKey = (String) getKeys.dataMap().get(keyN).object();

				resultKeys.add(outKey);
			}
			assertEquals(res.keySet().size(), resultKeys.size());
			for (String key : res.keySet()) {
				assertEquals(true, resultKeys.contains(key));
				checkGets(job, key, 1, res.get(key));
			}
			System.err.println("Execution time for " + keySet.size() + " words:: " + ((end - start) / 1000) + "s.");
		}
	}

	private void checkGets(Job job, String key, int nrOfValues, int sum) throws ClassNotFoundException, IOException {
		FutureGet getValues = dhtCon.getAll(key, executorBCHandler.getJob(job.id()).currentProcedure().dataInputDomain().toString()).awaitUninterruptibly();
		if (getValues.isSuccess()) {
			Set<Number640> valueSet = getValues.dataMap().keySet();
			assertEquals(1, valueSet.size());
			List<Integer> resultValues = new ArrayList<>();
			for (Number640 valueN : valueSet) {
				Integer outValue = (Integer) ((Value) getValues.dataMap().get(valueN).object()).value();
				resultValues.add(outValue);
			}
			assertEquals(nrOfValues, resultValues.size());
			assertEquals(true, resultValues.contains(sum));
			// logger.info("Results: " + key + " with values " + resultValues);
		}
	}

	private void execute(Job job, List<Tuple> tasks) throws InterruptedException {

		// executorBCHandler.dhtConnectionProvider(dhtCon);

		logger.info("Procedures before put: " + job.procedures());
		dhtCon.put(DomainProvider.JOB, job, job.id()).awaitUninterruptibly();
		Procedure procedure = job.currentProcedure();
		JobProcedureDomain outputJPD = JobProcedureDomain.create(job.id(), job.submissionCount(), "S1", procedure.executable().getClass().getSimpleName(), procedure.procedureIndex());
		procedure.dataInputDomain(JobProcedureDomain.create(job.id(), job.submissionCount(), "S1", DomainProvider.INITIAL_PROCEDURE, -1).expectedNrOfFiles(tasks.size())).addOutputDomain(outputJPD);

		List<IBCMessage> msgs = new ArrayList<>();
		for (Tuple tuple : tasks) {
			ExecutorTaskDomain outputETD = ExecutorTaskDomain.create(tuple.task.key(), "S1", tuple.task.newStatusIndex(), outputJPD);
			IContext context = DHTStorageContext.create().outputExecutorTaskDomain(outputETD).dhtConnectionProvider(dhtCon);

			context.write(tuple.task.key(), tuple.value);
			Futures.whenAllSuccess(context.futurePutData()).awaitUninterruptibly();
			outputETD.resultHash(context.resultHash());
			IBCMessage msg = CompletedBCMessage.createCompletedTaskBCMessage(outputETD, procedure.dataInputDomain());
			msgs.add(msg);
		}
		logger.info("Procedures before broadcast: " + job.procedures());

		for (IBCMessage msg : msgs) {
			logger.info("XXXinput: " + msg.inputDomain() + ", output: " + msg.outputDomain());
			dhtCon.broadcastCompletion(msg);
		}

		Thread.sleep(1000);
	}

}
