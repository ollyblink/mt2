package mapreduce.engine.executors;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.broadcasting.messages.CompletedProcedureBCMessage;
import mapreduce.engine.broadcasting.messages.CompletedTaskBCMessage;
import mapreduce.engine.broadcasting.messages.IBCMessage;
import mapreduce.engine.executors.performance.PerformanceInfo;
import mapreduce.execution.context.DHTStorageContext;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.procedures.StartProcedure;
import mapreduce.execution.tasks.Task;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.FileUtils;
import mapreduce.utils.IDCreator;
import mapreduce.utils.SyncedCollectionProvider;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number640;

public class JobSubmissionExecutor extends AbstractExecutor {
	private static Logger logger = LoggerFactory.getLogger(JobSubmissionExecutor.class);
	private static final int DEFAULT_NR_OF_SUBMISSIONS = 1;
	/**
	 * How many times should a job be tried to be submitted before it is
	 * aborted?
	 */
	private int maxNrOfSubmissions = DEFAULT_NR_OF_SUBMISSIONS;
	private Set<Job> submittedJobs = SyncedCollectionProvider.syncedHashSet();
	private List<String> outputLines = SyncedCollectionProvider.syncedArrayList();
	private int fileCounter;

	private JobSubmissionExecutor() {
		super(IDCreator.INSTANCE.createTimeRandomID(JobSubmissionExecutor.class.getSimpleName()));
	}

	public static JobSubmissionExecutor create() {
		return new JobSubmissionExecutor();
	}

	public JobSubmissionExecutor maxNrOfSubmissions(int maxNrOfSubmissions) {
		this.maxNrOfSubmissions = maxNrOfSubmissions;
		return this;
	}

	/**
	 * The idea is that for each key/value pair, a broadcast is done that a new
	 * task is available for a certain job. The tasks then can be pulled from
	 * the DHT. If a submission fails here, the whole job is aborted because not
	 * the whole data could be sent (this is different from when a job executor
	 * fails, as then the job simply is marked as failed, cleaned up, and may be
	 * pulled again).
	 * 
	 * @param job
	 * @return
	 */
	public void submit(final Job job) {
		FuturePut putJob = dhtConnectionProvider.put(DomainProvider.JOB, job, job.id()).awaitUninterruptibly();
		// taskDataComposer.splitValue("\n").maxFileSize(job.maxFileSize());

		List<String> keysFilePaths = filePaths(job.fileInputFolderPath());

		Procedure procedure = job.currentProcedure();
		JobProcedureDomain outputJPD = JobProcedureDomain.create(job.id(), job.submissionCount(), id,
				procedure.executable().getClass().getSimpleName(), procedure.procedureIndex(),
				procedure.currentExecutionNumber());
		procedure
				.dataInputDomain(JobProcedureDomain
						.create(job.id(), job.submissionCount(), id, DomainProvider.INITIAL_PROCEDURE, -1, 0)
						.expectedNrOfFiles(estimatedNrOfFiles(keysFilePaths, job.maxFileSize().value())))
				.addOutputDomain(outputJPD);

		if (putJob.isSuccess()) {
			logger.info("Successfully submitted job " + job);
			submittedJobs.add(job);
			for (String keyfilePath : keysFilePaths) {
				readFile(job.maxFileSize().value(), (StartProcedure) procedure.executable(), keyfilePath, outputJPD,
						procedure.dataInputDomain(), job.fileEncoding());
			}
		}

	}

	private void readFile(Integer maxFileSize, StartProcedure startProcedure, String keyfilePath,
			JobProcedureDomain outputJPD, JobProcedureDomain dataInputDomain, String fileEncoding) {
		// Path path = Paths.get(keyfilePath);
		// Charset charset = Charset.forName(taskDataComposer.fileEncoding());
		try {
			RandomAccessFile aFile = new RandomAccessFile(keyfilePath, "r");
			FileChannel inChannel = aFile.getChannel();
			ByteBuffer buffer = ByteBuffer.allocate(maxFileSize);
			int filePartCounter = 0;
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
				System.err.println("Split has size: " + split.getBytes(Charset.forName(fileEncoding)).length);
				// Assure that words are not split in parts by the buffer: only
				// take the split until the last occurrance of " " and then
				// append that to the first again

				if (split.getBytes(Charset.forName(fileEncoding)).length >= maxFileSize) {
					actualData = split.substring(0, split.lastIndexOf(" "));
					remaining = split.substring(split.lastIndexOf(" ") + 1, split.length());
				} else {
					actualData = split;
					remaining = "";
				}
				submitInternally(startProcedure, outputJPD, dataInputDomain, keyfilePath, filePartCounter++,
						actualData);
				buffer.clear(); // do something with the data and clear/compact
								// it.
				split = "";
				actualData = "";
			}
			inChannel.close();
			aFile.close();
		} catch (Exception e) {
			logger.warn("Exception on reading file at location: " + keyfilePath, e);
		}
	}

	private void submitInternally(StartProcedure startProcedure, JobProcedureDomain outputJPD,
			JobProcedureDomain dataInputDomain, String keyfilePath, Integer filePartCounter, String vals) {
		Collection<Object> values = new ArrayList<>();
		values.add(vals);
		Task task = Task.create(new File(keyfilePath).getName() + "_" + filePartCounter, id);
		ExecutorTaskDomain outputETD = ExecutorTaskDomain.create(task.key(), id, task.currentExecutionNumber(),
				outputJPD);
		logger.info("outputETD: " + outputETD.toString());
		DHTStorageContext context = DHTStorageContext.create().outputExecutorTaskDomain(outputETD)
				.dhtConnectionProvider(dhtConnectionProvider);

		logger.info("internal submit(): Put split: <" + task.key() + ", \"" + vals + "\">");
		startProcedure.process(task.key(), values, context);
		FutureDone<List<FuturePut>> awaitPut = Futures.whenAllSuccess(context.futurePutData()).awaitUninterruptibly();
		if (awaitPut.isSuccess()) {
			outputETD.resultHash(context.resultHash());
			IBCMessage msg = CompletedTaskBCMessage.create(outputJPD, dataInputDomain).addOutputDomainTriple(outputETD);
			dhtConnectionProvider.broadcastCompletion(msg);
			logger.info("submitInternally::Successfully broadcasted TaskCompletedBCMessage for task " + task.key());
		} else {
			logger.warn("No success on task execution. Reason: " + awaitPut.failedReason());
		}
	}

	private List<String> filePaths(String fileInputFolderPath) {
		List<String> keysFilePaths = new ArrayList<String>();
		FileUtils.INSTANCE.getFiles(new File(fileInputFolderPath), keysFilePaths);
		return keysFilePaths;
	}

	/**
	 * Tries to give an initial guess of how many files there are going to be
	 * (this calculation is solely based on the overall file size. It may be
	 * that splitting the file reduces the file sizes and thus, fewer files are
	 * actually transferred in the process than were expected
	 * 
	 * @param job
	 * @param keysFilePaths
	 * @param maxFileSize
	 * @return
	 */
	private int estimatedNrOfFiles(List<String> keysFilePaths, Integer maxFileSize) {
		int nrOfFiles = 0;
		for (String fileName : keysFilePaths) {
			long fileSize = new File(fileName).length();
			nrOfFiles += (int) (fileSize / maxFileSize);
			if (fileSize % maxFileSize > 0) {
				++nrOfFiles;
			}
		}
		return nrOfFiles;
	}

	public synchronized void retrieveAndStoreDataOfFinishedJob(JobProcedureDomain resultDomain) {
		try {
			Job job = job(resultDomain.jobId());

			String resultOutputFolder = job.resultOutputFolder();
			String fileEncoding = job.fileEncoding();
			dhtConnectionProvider.getAll(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, resultDomain.toString())
					.addListener(new BaseFutureAdapter<FutureGet>() {

						@Override
						public void operationComplete(FutureGet future) throws Exception {
							List<FutureGet> getFutures = SyncedCollectionProvider.syncedArrayList();
							if (future.isSuccess()) {
								for (Number640 keyNumber : future.dataMap().keySet()) {
									String key = (String) future.dataMap().get(keyNumber).object();
									logger.info("get(" + key + ").domain(" + resultDomain.toString() + ")");
									List<Object> values = SyncedCollectionProvider.syncedArrayList();
									getFutures.add(dhtConnectionProvider.getAll(key, resultDomain.toString())
											.addListener(new BaseFutureAdapter<FutureGet>() {

										@Override
										public void operationComplete(FutureGet future) throws Exception {
											if (future.isSuccess()) {
												for (Number640 valueNr : future.dataMap().keySet()) {
													values.add(
															((Value) future.dataMap().get(valueNr).object()).value());
												}

												String line = key.toString() + "\t";
												for (int i = 0; i < values.size() - 1; ++i) {
													line += values.get(i).toString() + ", ";
												}
												line += values.get(values.size() - 1).toString() + ", ";
												write(line.substring(0, line.lastIndexOf(",")), resultOutputFolder,
														job(resultDomain.jobId()).outputFileSize().value(),
														fileEncoding);
											} else {
												logger.info("failed to retrieve values for key: " + key);
											}
										}

									}));

								}
							} else {
								logger.info("Failed to retrieve keys for job " + resultDomain.jobId());
							}

							Futures.whenAllSuccess(getFutures)
									.addListener(new BaseFutureAdapter<FutureDone<FutureGet>>() {
								@Override
								public void operationComplete(FutureDone<FutureGet> future) throws Exception {
									if (future.isSuccess()) {
										logger.info("Successfully wrote data to file system.Marking job "
												+ resultDomain.jobId() + " as finished.");
										markAsRetrieved(resultDomain.jobId());
										flush(resultOutputFolder, fileEncoding);
									}
								}

							});
						}
					});

		} catch (Exception e) {
			logger.info("Exception caught", e);
		}
	}

	private void write(String dataLine, String resultOutputFolder, Integer outputFileSize, String fileEncoding) {
		if (lineSizes(dataLine, fileEncoding) >= outputFileSize) {
			flush(resultOutputFolder, fileEncoding);
		}
		outputLines.add(dataLine);
	}

	private void flush(String resultOutputFolder, String fileEncoding) {
		synchronized (outputLines) {
			if (!outputLines.isEmpty()) {
				createFolder(resultOutputFolder);
				Path file = Paths.get(resultOutputFolder + "/file_" + (fileCounter++) + ".txt");
				Charset charset = Charset.forName(fileEncoding);
				try (BufferedWriter writer = Files.newBufferedWriter(file, charset)) {

					for (String line : outputLines) {
						writer.write(line + "\n");
					}
					outputLines.clear();

					writer.flush();
					writer.close();
				} catch (IOException x) {
					System.err.format("IOException: %s%n", x);
				}
			}
		}
	}

	private void createFolder(String outputFolder) {
		if (!new File(outputFolder).exists()) {
			new File(outputFolder).mkdirs();
			logger.info("createFolder::outputFolder: " + outputFolder);
		}
		// else { //TODO do I actually need this?
		// int counter = 0;
		// while (new File(outputFolder + counter).exists()) {
		// counter++;
		// }
		// new File(outputFolder + counter).mkdirs();
		// }
	}

	private long lineSizes(String dataLine, String fileEncoding) {
		long lineSizes = 0;
		synchronized (outputLines) {
			for (String line : outputLines) {
				lineSizes += line.getBytes(Charset.forName(fileEncoding)).length;
			}
		}
		return lineSizes + dataLine.getBytes(Charset.forName(fileEncoding)).length;

	}

	public boolean submittedJob(Job job) {
		return submittedJobs.contains(job);
	}

	public boolean jobIsRetrieved(Job job) {
		if (submittedJob(job)) {
			synchronized (submittedJobs) {
				for (Job j : submittedJobs) {
					if (j.equals(job)) {
						return j.isRetrieved();
					}
				}
			}
		}
		return false;
	}

	public void markAsRetrieved(String jobId) {
		synchronized (submittedJobs) {
			for (Job job : submittedJobs) {
				if (job.id().equals(jobId)) {
					job.isRetrieved(true);
				}
			}
		}
	}

	public Job job(String jobId) {
		synchronized (submittedJobs) {
			for (Job j : submittedJobs) {
				if (j.id().equals(jobId)) {
					return j;
				}
			}
		}
		return null;
	}

	@Override
	public JobSubmissionExecutor dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		return this;
	}

	@Override
	public JobSubmissionExecutor performanceInformation(PerformanceInfo performanceInformation) {
		this.performanceInformation = performanceInformation;
		return this;
	}
}
