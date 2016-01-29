package mapreduce.engine.executors;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import mapreduce.engine.broadcasting.broadcasthandlers.JobSubmissionBroadcastHandler;
import mapreduce.engine.messageconsumers.JobSubmissionMessageConsumer;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.IDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.finishables.AbstractFinishable;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.StartProcedure;
import mapreduce.execution.procedures.WordCountReducer;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.FileSize;
import mapreduce.utils.FileUtils;
import mapreduce.utils.SyncedCollectionProvider;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class JobSubmissionExecutorTest {
	private static Logger logger = LoggerFactory.getLogger(JobSubmissionExecutorTest.class);

	private static JobSubmissionExecutor sExecutor = JobSubmissionExecutor.create();

	public static void main(String[] args) {

	}

	@Test
	public void testJobIsRetrievedAndMarkAsRetrievedAndJobAndSubmittedJob() throws Exception {
		// ================================================================================================
		// Testing 4 simple methods: jobIsRetrieved, job, submittedJob, markAsRetrieved
		// ================================================================================================
		Field submittedJobsField = JobSubmissionExecutor.class.getDeclaredField("submittedJobs");
		submittedJobsField.setAccessible(true);
		Set<Job> submittedJobs = (Set<Job>) submittedJobsField.get(sExecutor);
		Job job = Job.create(sExecutor.id());
		Job job2 = Job.create(sExecutor.id());
		assertEquals(false, sExecutor.submittedJob(job));
		assertEquals(false, sExecutor.submittedJob(job2));
		assertEquals(false, sExecutor.jobIsRetrieved(job));
		assertEquals(false, sExecutor.jobIsRetrieved(job2));
		assertEquals(null, sExecutor.job(job.id()));
		assertEquals(null, sExecutor.job(job2.id()));
		submittedJobs.add(job);
		submittedJobs.add(job2);
		assertEquals(true, sExecutor.submittedJob(job));
		assertEquals(true, sExecutor.submittedJob(job2));
		assertEquals(false, sExecutor.jobIsRetrieved(sExecutor.job(job.id())));
		assertEquals(false, sExecutor.jobIsRetrieved(sExecutor.job(job2.id())));
		sExecutor.markAsRetrieved(job.id());
		assertEquals(true, sExecutor.jobIsRetrieved(sExecutor.job(job.id())));
		assertEquals(false, sExecutor.jobIsRetrieved(sExecutor.job(job2.id())));
		sExecutor.markAsRetrieved(job2.id());
		assertEquals(true, sExecutor.jobIsRetrieved(sExecutor.job(job.id())));
		assertEquals(true, sExecutor.jobIsRetrieved(sExecutor.job(job2.id())));
	}

	@Test
	public void testSubmitInternally() throws Exception {
		Method submitInternally = JobSubmissionExecutor.class.getDeclaredMethod("submitInternally", StartProcedure.class, JobProcedureDomain.class, JobProcedureDomain.class, String.class,
				Integer.class, String.class);
		submitInternally.setAccessible(true);

		Random random = new Random();
		JobSubmissionBroadcastHandler bcHandler = Mockito.mock(JobSubmissionBroadcastHandler.class);

		IDHTConnectionProvider dht = TestUtils.getTestConnectionProvider(bcHandler);

		sExecutor.dhtConnectionProvider(dht);
		JobProcedureDomain outputJPD = JobProcedureDomain.create("J1", 0, sExecutor.id(), DomainProvider.INITIAL_PROCEDURE, -1);
		JobProcedureDomain dataInputDomain = JobProcedureDomain.create("J1", 0, sExecutor.id(), StartProcedure.class.getSimpleName(), 0);
		String keyFilePath = System.getProperty("user.dir") + "/src/test/java/mapreduce/engine/executors/testfiles/testfile2.txt";
		Integer filePartCounter = 0;
		String vals = "hello world hello world\nhello world hello world";

		submitInternally.invoke(sExecutor, StartProcedure.create(), outputJPD, dataInputDomain, keyFilePath, filePartCounter, vals);
		String key = "testfile2.txt_0";
		ExecutorTaskDomain outputETD = ExecutorTaskDomain.create(key, sExecutor.id(), 0, outputJPD);
		checkData(dht, outputETD, key, vals);
	}

	@Test
	public void testReadFile() throws Exception {
		Method readFile = JobSubmissionExecutor.class.getDeclaredMethod("readFile", StartProcedure.class, String.class, JobProcedureDomain.class, JobProcedureDomain.class);
		readFile.setAccessible(true);

		Random random = new Random();
		JobSubmissionBroadcastHandler bcHandler = Mockito.mock(JobSubmissionBroadcastHandler.class);

		IDHTConnectionProvider dht = TestUtils.getTestConnectionProvider(bcHandler);

		sExecutor.dhtConnectionProvider(dht);
		JobProcedureDomain outputJPD = JobProcedureDomain.create("J1", 0, sExecutor.id(), DomainProvider.INITIAL_PROCEDURE, -1);
		JobProcedureDomain dataInputDomain = JobProcedureDomain.create("J1", 0, sExecutor.id(), StartProcedure.class.getSimpleName(), 0);
		String keyFilePath = System.getProperty("user.dir") + "/src/test/java/mapreduce/engine/executors/testfiles/testfile2.txt";

		readFile.invoke(sExecutor, StartProcedure.create(), keyFilePath, outputJPD, dataInputDomain);
		String key = "testfile2.txt_0";
		ExecutorTaskDomain outputETD = ExecutorTaskDomain.create(key, sExecutor.id(), 0, outputJPD);
		String vals = "hello world hello world hello world hello world";
		checkData(dht, outputETD, key, vals);

	}

	@Test
	public void testSubmit() throws Exception {
		Random random = new Random();
		JobSubmissionBroadcastHandler bcHandler = Mockito.mock(JobSubmissionBroadcastHandler.class);

		IDHTConnectionProvider dht = TestUtils.getTestConnectionProvider(bcHandler);

		sExecutor.dhtConnectionProvider(dht);
		String fileInputFolderPath = System.getProperty("user.dir") + "/src/test/java/mapreduce/engine/executors/testfiles/";

		Job job = Job.create(sExecutor.id()).fileInputFolderPath(fileInputFolderPath).maxFileSize(FileSize.THIRTY_TWO_BYTES);
		sExecutor.submit(job);
		Field outputJPDsField = AbstractFinishable.class.getDeclaredField("outputDomains");
		outputJPDsField.setAccessible(true);

		JobProcedureDomain outputJPD = (JobProcedureDomain) ((List<IDomain>) outputJPDsField.get(job.currentProcedure())).get(0);

		String key11 = "testfile.txt_0";
		String vals11 = "the quick fox jumps over the";
		ExecutorTaskDomain outputETD11 = ExecutorTaskDomain.create(key11, sExecutor.id(), 0, outputJPD);
		checkData(dht, outputETD11, key11, vals11);

		String key12 = "testfile.txt_1";
		String vals12 = "lazy dog";
		ExecutorTaskDomain outputETD12 = ExecutorTaskDomain.create(key12, sExecutor.id(), 0, outputJPD);
		checkData(dht, outputETD12, key12, vals12);

		String key21 = "testfile2.txt_0";
		String vals21 = "hello world hello world hello";
		ExecutorTaskDomain outputETD2 = ExecutorTaskDomain.create(key21, sExecutor.id(), 0, outputJPD);
		checkData(dht, outputETD2, key21, vals21);

		String key22 = "testfile2.txt_1";
		String vals22 = "world hello world";
		ExecutorTaskDomain outputETD22 = ExecutorTaskDomain.create(key21, sExecutor.id(), 0, outputJPD);
		checkData(dht, outputETD22, key22, vals22);
	}

	private void checkData(IDHTConnectionProvider dht, ExecutorTaskDomain outputETD1, String key, String vals) throws ClassNotFoundException, IOException {
		FutureGet getData = dht.getAll(key, outputETD1.toString()).awaitUninterruptibly();
		logger.info("Retrieved data for " + key);
		if (getData.isSuccess()) {
			Map<Number640, Data> dataMap = getData.dataMap();
			for (Number640 n : dataMap.keySet()) {
				String retrieved = (String) ((Value) dataMap.get(n).object()).value();
				logger.info("Data: <" + key + ", " + retrieved + ">");
				assertEquals(vals, retrieved);
			}
		}
	}

	@Test
	public void testRetrieveAndStoreDataOfFinishedJob() throws Exception {
		JobSubmissionBroadcastHandler bcHandler = Mockito.mock(JobSubmissionBroadcastHandler.class);

		IDHTConnectionProvider dht = TestUtils.getTestConnectionProvider(bcHandler);
		sExecutor.dhtConnectionProvider(dht);
		JobProcedureDomain domain = JobProcedureDomain.create("J1", 0, "E1", WordCountReducer.class.getSimpleName(), 2);
		String[] keys = { "hello", "world", "this", "is", "a", "test" };
		Integer[] counts = { 1, 2, 3, 4, 5, 6 };

		for (int i = 0; i < keys.length; ++i) {
			dht.add(keys[i], counts[i], domain.toString(), true).awaitListenersUninterruptibly();
			dht.add(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, keys[i], domain.toString(), false).awaitListenersUninterruptibly();
		}

		Field jobsField = JobSubmissionExecutor.class.getDeclaredField("submittedJobs");
		jobsField.setAccessible(true);
		Set<Job> submittedJobs = (Set<Job>) jobsField.get(sExecutor);

		Job job = Mockito.mock(Job.class);
		String resultOutputFolder = System.getProperty("user.dir") + "/src/test/java/mapreduce/engine/executors/testoutputfiles/tmp1";
		File file = new File(resultOutputFolder);
		file.mkdirs();

		Mockito.when(job.id()).thenReturn("J1");
		Mockito.when(job.resultOutputFolder()).thenReturn(resultOutputFolder);
		Mockito.when(job.outputFileSize()).thenReturn(FileSize.MEGA_BYTE);
		submittedJobs.add(job);

		sExecutor.retrieveAndStoreDataOfFinishedJob(domain);
		Thread.sleep(1000);

		List<String> pathVisitor = new ArrayList<>();
		FileUtils.INSTANCE.getFiles(new File(resultOutputFolder), pathVisitor);

		ArrayList<String> res = FileUtils.INSTANCE.readLinesFromFile(pathVisitor.get(0));
		String resFile = "";
		for (String line : res) {
			resFile += line + "\n";
		}
		logger.info("File data: " + resFile);
		for (int i = 0; i < keys.length; ++i) {
			assertEquals(true, resFile.contains(keys[i] + "\t" + counts[i]));
		}

		for (String fP : pathVisitor) {
			new File(fP).delete();
			assertEquals(false, new File(fP).exists());
		}
		new File(resultOutputFolder).delete();
		assertEquals(false, new File(resultOutputFolder).exists());
		new File(resultOutputFolder.replace("tmp1", "")).delete();
		assertEquals(false, new File(resultOutputFolder.replace("tmp1", "")).exists());
	}

	@Test
	public void testWriteFlush() throws Exception {
		Method write = JobSubmissionExecutor.class.getDeclaredMethod("write", String.class, String.class, Long.class);
		write.setAccessible(true);
		Method flush = JobSubmissionExecutor.class.getDeclaredMethod("flush", String.class);
		flush.setAccessible(true);
		String resultOutputFolder = System.getProperty("user.dir") + "/src/test/java/mapreduce/engine/executors/testoutputfiles";
		Job job = Mockito.mock(Job.class);
		new File(resultOutputFolder).mkdirs();
		new File(resultOutputFolder + "/tmp1").mkdirs();
		Mockito.when(job.resultOutputFolder()).thenReturn(resultOutputFolder + "/tmp1");

		write.invoke(sExecutor, "hello\t20", job.resultOutputFolder(), FileSize.SIXTEEN_BYTES.value());
		write.invoke(sExecutor, "world\t15", job.resultOutputFolder(), FileSize.SIXTEEN_BYTES.value());
		write.invoke(sExecutor, "this\t10", job.resultOutputFolder(), FileSize.SIXTEEN_BYTES.value());
		write.invoke(sExecutor, "is\t5", job.resultOutputFolder(), FileSize.SIXTEEN_BYTES.value());
		write.invoke(sExecutor, "a\t10", job.resultOutputFolder(), FileSize.SIXTEEN_BYTES.value());
		write.invoke(sExecutor, "test\t11", job.resultOutputFolder(), FileSize.SIXTEEN_BYTES.value());
		flush.invoke(sExecutor, job.resultOutputFolder());
		List<String> pathVisitor = new ArrayList<>();
		FileUtils.INSTANCE.getFiles(new File(resultOutputFolder + "/tmp1"), pathVisitor);
		assertEquals(3, pathVisitor.size());
		FileUtils.INSTANCE.deleteFilesAndFolder(resultOutputFolder + "/tmp1", pathVisitor);
		pathVisitor.clear();
		FileUtils.INSTANCE.deleteFilesAndFolder(resultOutputFolder, pathVisitor);
		assertEquals(false, new File(resultOutputFolder).exists());
	}

	@Test
	public void testEstimatedNrOfFiles() throws Exception {
		Method estimatedNrOfFilesMethod = JobSubmissionExecutor.class.getDeclaredMethod("estimatedNrOfFiles", List.class, Long.class);
		estimatedNrOfFilesMethod.setAccessible(true);
		List<String> keyFilePaths = new ArrayList<>();
		keyFilePaths.add(System.getProperty("user.dir") + "/src/test/java/mapreduce/engine/executors/testfiles/testfile2.txt");
		int nrFiles = (int) estimatedNrOfFilesMethod.invoke(sExecutor, keyFilePaths, 20l);
		assertEquals(3, nrFiles);
	}

	@Test
	public void testFilePaths() throws Exception {
		String fileInputFolderPath = System.getProperty("user.dir") + "/src/test/java/mapreduce/engine/executors/testfiles/";
		Method filePaths = JobSubmissionExecutor.class.getDeclaredMethod("filePaths", String.class);
		filePaths.setAccessible(true);
		List<String> keyFilePaths = (List<String>) filePaths.invoke(sExecutor, fileInputFolderPath);
		assertEquals(2, keyFilePaths.size());
		assertEquals("testfile.txt", new File(keyFilePaths.get(0)).getName());
		assertEquals("testfile2.txt", new File(keyFilePaths.get(1)).getName());
	}

	@Test
	public void testCreateFolder() throws Exception {
		Method createFolder = JobSubmissionExecutor.class.getDeclaredMethod("createFolder", String.class);
		createFolder.setAccessible(true);
		String testFolder = System.getProperty("user.dir") + "/src/test/java/mapreduce/engine/executors/testfiles/trials";

		assertEquals(false, new File(testFolder).exists());
		createFolder.invoke(sExecutor, testFolder);
		assertEquals(true, new File(testFolder).exists());
		FileUtils.INSTANCE.deleteFilesAndFolder(testFolder, new ArrayList<>());
		assertEquals(false, new File(testFolder).exists());
	}
}
