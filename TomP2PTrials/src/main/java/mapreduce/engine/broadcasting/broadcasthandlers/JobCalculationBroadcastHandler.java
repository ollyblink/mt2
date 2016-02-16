package mapreduce.engine.broadcasting.broadcasthandlers;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ListMultimap;

import mapreduce.engine.broadcasting.messages.BCMessageStatus;
import mapreduce.engine.broadcasting.messages.CompletedProcedureBCMessage;
import mapreduce.engine.broadcasting.messages.CompletedTaskBCMessage;
import mapreduce.engine.broadcasting.messages.IBCMessage;
import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.messageconsumers.IMessageConsumer;
import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.engine.multithreading.PriorityExecutor;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.procedures.Procedures;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureAdapter;

public class JobCalculationBroadcastHandler extends AbstractMapReduceBroadcastHandler {

	private static Logger logger = LoggerFactory.getLogger(JobCalculationBroadcastHandler.class);
	private volatile Boolean lock = false;

	protected ListMultimap<Job, Future<?>> jobFuturesFor = SyncedCollectionProvider.syncedArrayListMultimap();
	protected PriorityExecutor messageExecutor;

	// Setter, Getter, Creator, Constructor follow below..
	protected JobCalculationBroadcastHandler(int nrOfConcurrentlyExecutedBCMessages) {

		this.messageExecutor = PriorityExecutor.newFixedThreadPool(nrOfConcurrentlyExecutedBCMessages);
	}

	@Override
	public void evaluateReceivedMessage(IBCMessage bcMessage) {

		String jobId = bcMessage.inputDomain().jobId();

		Job job = getJob(jobId);
		if (job == null) {
			synchronized (lock) {
				if (!lock) {
					lock = true;

					logger.info("Job was null... retrieving job");
					dhtConnectionProvider.get(DomainProvider.JOB, jobId).addListener(new BaseFutureAdapter<FutureGet>() {

						@Override
						public void operationComplete(FutureGet future) throws Exception {
							if (future.isSuccess()) {
								if (future.data() != null) {
									Job job = (Job) future.data().object();
									logger.info("Retrieved job: " + job);
									for (Procedure procedure : job.procedures()) {
										if (procedure.executable() instanceof String) {// Means a java script function --> convert
											procedure.executable(Procedures.convertJavascriptToJava((String) procedure.executable()));
										}
										if (procedure.combiner() != null && procedure.combiner() instanceof String) {
											procedure.combiner(Procedures.convertJavascriptToJava((String) procedure.combiner()));// Means a java script function --> convert
										}
									}
									logger.info("Retrieved Job (" + jobId + ") from DHT. ");
									processMessage(bcMessage, job);
									lock = false;
								}
							} else {
								logger.info("No success retrieving Job (" + jobId + ") from DHT. ");
							}
						}
					});

				} else {
					while (lock) {
						try {
							logger.info("Sleep and wait for job retrieval");
							Thread.sleep(100);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					evaluateReceivedMessage(bcMessage);
				}
			}
		} else {// Already received once... check if it maybe is a new submission
			if (job.submissionCount() < bcMessage.inputDomain().jobSubmissionCount()) {
				cancelJob(job);
				while (job.submissionCount() < bcMessage.inputDomain().jobSubmissionCount()) {
					job.incrementSubmissionCounter();
				}
			}
			// if (bcMessage.outputDomain() != null) {
			if (!bcMessage.outputDomain().executor().equals(JobCalculationExecutor.classId)) { // Don't receive messages from self
				processMessage(bcMessage, job);
			}
			// }
		}

	}

	@Override
	public void processMessage(IBCMessage bcMessage, Job job) {
		logger.info("Received message: " + bcMessage.status() + " for procedure " + bcMessage.outputDomain().procedureSimpleName() + " for job " + job);
		if (!job.isFinished()) {
			logger.info("Job: " + job + " is not finished. Executing BCMessage: " + bcMessage);
			updateTimeout(job, bcMessage);

			JobProcedureDomain rJPD = null;
			Runnable runnable = null;
			if (bcMessage.status() == BCMessageStatus.COMPLETED_TASK) {
				CompletedTaskBCMessage taskMsg = (CompletedTaskBCMessage) bcMessage;
				rJPD = taskMsg.outputDomain();
				runnable = new Runnable() {

					@Override
					public void run() {
						messageConsumer.handleCompletedTask(job, taskMsg.allExecutorTaskDomains(), bcMessage.inputDomain());
					}
				};
			} else if (bcMessage.status() == BCMessageStatus.COMPLETED_PROCEDURE) { // status == BCMessageStatus.COMPLETED_PROCEDURE
				CompletedProcedureBCMessage procMsg = (CompletedProcedureBCMessage) bcMessage;
				rJPD = procMsg.outputDomain();
				runnable = new Runnable() {

					@Override
					public void run() {
						messageConsumer.handleCompletedProcedure(job, procMsg.outputDomain(), procMsg.inputDomain());
					}
				};
			}

			boolean receivedOutdatedMessage = job.currentProcedure().procedureIndex() > rJPD.procedureIndex();
			if (receivedOutdatedMessage) {
				logger.info("processMessage:: I (" + JobCalculationExecutor.classId + ") Received an old message: nothing to do. message contained rJPD:" + rJPD + " but I already use procedure "
						+ job.currentProcedure().procedureIndex());
				return;
			} else {
				jobFuturesFor.put(job, messageExecutor.submit(runnable, job.priorityLevel(), job.creationTime(), job.id(), bcMessage.procedureIndex(), bcMessage.status(), bcMessage.creationTime()));
			}
		} else {
			logger.info("aborting job");
			cancelJob(job);
		}

	}

	public Job getJob(String jobId) {
		synchronized (jobFuturesFor) {
			for (Job job : jobFuturesFor.keySet()) {
				if (job.id().equals(jobId)) {
					return job;
				}
			}
			return null;
		}
	}

	public ListMultimap<Job, Future<?>> jobFutures() {
		return this.jobFuturesFor;
	}

	public void cancelJob(Job job) {
		List<Future<?>> jobFutures = jobFuturesFor.get(job);
		synchronized (jobFutures) {
			for (Future<?> jobFuture : jobFutures) {
				if (!jobFuture.isCancelled()) {
					jobFuture.cancel(true);
				}
			}
		}
		messageConsumer.cancelJob(job);

	}

	public void shutdown() {
		logger.info("shutdown:: shutdown messageExecutor");
		messageExecutor.shutdown();
		try {
			while (!messageExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
				logger.info("Awaiting completion of threads.");
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 *
	 * 
	 * @param nrOfConcurrentlyExecutedBCMessages
	 *            number of threads for this thread pool: how many bc messages may be executed at the same time?
	 * @return
	 */
	public static JobCalculationBroadcastHandler create(int nrOfConcurrentlyExecutedBCMessages) {
		return new JobCalculationBroadcastHandler(nrOfConcurrentlyExecutedBCMessages);
	}

	public static JobCalculationBroadcastHandler create() {
		return new JobCalculationBroadcastHandler(1);
	}

	@Override
	public JobCalculationBroadcastHandler messageConsumer(IMessageConsumer messageConsumer) {
		return (JobCalculationBroadcastHandler) super.messageConsumer((JobCalculationMessageConsumer) messageConsumer);
	}

}
