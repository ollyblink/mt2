package mapreduce.engine.componenttests;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.broadcasting.broadcasthandlers.JobSubmissionBroadcastHandler;
import mapreduce.engine.broadcasting.messages.CompletedBCMessage;
import mapreduce.engine.broadcasting.messages.IBCMessage;
import mapreduce.engine.executors.JobSubmissionExecutor;
import mapreduce.engine.messageconsumers.JobSubmissionMessageConsumer;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.StartProcedure;
import mapreduce.execution.procedures.WordCountMapper;
import mapreduce.execution.procedures.WordCountReducer;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.FileSize;
import mapreduce.utils.FileUtils;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.peers.Number640;

public class JobSubmissionComponentTest {
	private static Logger logger = LoggerFactory.getLogger(JobSubmissionComponentTest.class);
	private JobSubmissionExecutor submissionExecutor;
	private JobSubmissionMessageConsumer submissionMessageConsumer;
	private JobSubmissionBroadcastHandler submissionBCHandler;
	private IDHTConnectionProvider dhtCon;

	@Before
	public void setUp() throws Exception {
		submissionExecutor = JobSubmissionExecutor.create();

		submissionMessageConsumer = JobSubmissionMessageConsumer.create().executor(submissionExecutor);
		submissionBCHandler = JobSubmissionBroadcastHandler.create()
				.messageConsumer(submissionMessageConsumer);
		// int bootstrapPort = 4001;
		dhtCon = TestUtils.getTestConnectionProvider(submissionBCHandler);
		// DHTConnectionProvider
		// .create("192.168.43.65", bootstrapPort, bootstrapPort).broadcastHandler(executorBCHandler)
		// .storageFilePath("C:\\Users\\Oliver\\Desktop\\storage")
		;
		dhtCon.broadcastHandler(submissionBCHandler);
		submissionExecutor.dhtConnectionProvider(dhtCon);
		submissionMessageConsumer.dhtConnectionProvider(dhtCon);
	}

	@After
	public void tearDown() throws Exception {
		dhtCon.shutdown();
	}

	@Test
	public void testSubmission() throws Exception {

		String fileInputFolderPath = System.getProperty("user.dir")
				+ "/src/test/java/mapreduce/engine/componenttests/testfiles";
		Job job = Job.create(submissionExecutor.id())
				.fileInputFolderPath(fileInputFolderPath).addSucceedingProcedure(WordCountMapper.create(),
						WordCountReducer.create(), 1, 1, false, false)
				.addSucceedingProcedure(WordCountReducer.create(), null, 1, 1, false, false);
		submissionExecutor.submit(job);

		FutureGet res = dhtCon.getAll(DomainProvider.JOB, job.id()).awaitUninterruptibly();

		if (res.isSuccess()) {
			assertEquals(1, res.dataMap().keySet().size());
			for (Number640 n : res.dataMap().keySet()) {
				Job job2 = (Job) res.dataMap().get(n).object();
				assertEquals(job, job2);
			}
		}
		/*
		 * C{JPD[J(JOB[TS(1453620393522)_RND(8030206527332109982)_LC(2)]_S(JOBSUBMISSIONEXECUTOR[TS(
		 * 1453620391979)_RND(-4202516038778062455)_LC(0)]))_JS(0)_PE(JOBSUBMISSIONEXECUTOR[TS(1453620391979)
		 * _RND(-4202516038778062455)_LC(0)])_P(STARTPROCEDURE)_PI(0)]}:::{ETD[T(testfile.txt_0)_P(
		 * JOBSUBMISSIONEXECUTOR[TS(1453620391979)_RND(-4202516038778062455)_LC(0)])_JSI(0)]}
		 */
		List<String> tasks = new ArrayList<>();
		tasks.add("hello");
		tasks.add("world");
		JobProcedureDomain outputJPD = JobProcedureDomain.create(job.id(), job.submissionCount(),
				submissionExecutor.id(), StartProcedure.class.getSimpleName(), 0);
		ExecutorTaskDomain outputETD = ExecutorTaskDomain.create("testfile.txt_0", submissionExecutor.id(), 0,
				outputJPD);
		res = dhtCon.getAll(DomainProvider.TASK_OUTPUT_RESULT_KEYS, outputETD.toString())
				.awaitUninterruptibly();

		if (res.isSuccess()) {
			assertEquals(1, res.dataMap().keySet().size());
			for (Number640 n : res.dataMap().keySet()) {
				String key = (String) res.dataMap().get(n).object();
				assertEquals(key, "testfile.txt_0");
			}
		}
		// for (String key : tasks) {
		res = dhtCon.getAll("testfile.txt_0", outputETD.toString()).awaitUninterruptibly();
		if (res.isSuccess()) {
			assertEquals(1, res.dataMap().keySet().size());
			for (Number640 n : res.dataMap().keySet()) {
				String value = (String) ((Value) res.dataMap().get(n).object()).value();
				String[] split = value.split(" ");
				assertEquals(2, split.length);
				assertEquals(tasks.get(0), split[0].trim());
				assertEquals(tasks.get(1), split[1].trim());
			}
		}
		// }
	}

	@Test
	public void testStoreResults() throws Exception {

		String fileInputFolderPath = System.getProperty("user.dir")
				+ "/src/test/java/mapreduce/engine/componenttests/testfiles";
		String fileOutputFolder = System.getProperty("user.dir")
				+ "/src/test/java/mapreduce/engine/componenttests/testfiles";

		Job job = Job.create(submissionExecutor.id()).resultOutputFolder(fileOutputFolder, FileSize.MEGA_BYTE)
				.fileInputFolderPath(fileInputFolderPath).addSucceedingProcedure(WordCountMapper.create(),
						WordCountReducer.create(), 1, 1, false, false)
				.addSucceedingProcedure(WordCountReducer.create(), null, 1, 1, false, false);

		JobProcedureDomain resultDomain = JobProcedureDomain
				.create(job.id(), 0, "E1", WordCountReducer.class.getSimpleName(), 2).isJobFinished(true);
		dhtCon.add("hello", 3, resultDomain.toString(), true).awaitUninterruptibly();
		dhtCon.add(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, "hello", resultDomain.toString(), false)
				.awaitUninterruptibly();
		dhtCon.add("world", 1, resultDomain.toString(), true).awaitUninterruptibly();
		dhtCon.add(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, "world", resultDomain.toString(), false)
				.awaitUninterruptibly();

		IBCMessage bcMessage = CompletedBCMessage.createCompletedProcedureBCMessage(resultDomain,
				resultDomain);
		Field jobsField = JobSubmissionExecutor.class.getDeclaredField("submittedJobs");
		jobsField.setAccessible(true);
		Set<Job> submittedJobs = (Set<Job>) jobsField.get(submissionExecutor);
		submittedJobs.add(job);
		submissionBCHandler.evaluateReceivedMessage(bcMessage);
		Thread.sleep(1000);
		List<String> pathVisitor = new ArrayList<>();
		String outFolder = fileOutputFolder + "/tmp";
		FileUtils.INSTANCE.getFiles(new File(outFolder), pathVisitor);
		assertEquals(1, pathVisitor.size());
		String txt = FileUtils.INSTANCE.readLines(pathVisitor.get(0));
		logger.info("txt: " + txt);
		assertEquals(true, txt.contains("hello\t3"));
		assertEquals(true, txt.contains("world\t1"));

		FileUtils.INSTANCE.deleteFilesAndFolder(outFolder, pathVisitor);
	}
}
