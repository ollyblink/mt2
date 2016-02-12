package mapreduce.engine.broadcasting.broadcasthandlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.broadcasting.messages.BCMessageStatus;
import mapreduce.engine.broadcasting.messages.CompletedProcedureBCMessage;
import mapreduce.engine.broadcasting.messages.CompletedTaskBCMessage;
import mapreduce.engine.broadcasting.messages.IBCMessage;
import mapreduce.engine.messageconsumers.IMessageConsumer;
import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.procedures.Procedures;
import mapreduce.utils.DomainProvider;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureAdapter;

public class JobCalculationBroadcastHandler extends AbstractMapReduceBroadcastHandler {

	private static Logger logger = LoggerFactory.getLogger(JobCalculationBroadcastHandler.class);
	private volatile Boolean lock = false;

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
											// Means a java script function --> convert
											procedure.combiner(Procedures.convertJavascriptToJava((String) procedure.combiner()));
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
					
				}else{
					while(lock){
						try {
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
				abortJobExecution(job);
				while (job.submissionCount() < bcMessage.inputDomain().jobSubmissionCount()) {
					job.incrementSubmissionCounter();
				}
			}
			if (!bcMessage.outputDomain().executor().equals(messageConsumer.executor().id())) { // Don't receive messages from self
				processMessage(bcMessage, job);
			}
		}

	}

	@Override
	public void processMessage(IBCMessage bcMessage, Job job) {
		logger.info("Received message: " + bcMessage.status() + " for procedure " + bcMessage.outputDomain().procedureSimpleName() + " for job " + job);
		if (!job.isFinished()) {
			logger.info("Job: " + job + " is not finished. Executing BCMessage: " + bcMessage);
			logger.info("Job's current procedure: " + job.currentProcedure().executable().getClass().getSimpleName() + " has tasks: " + job.currentProcedure().tasks());
			updateTimeout(job, bcMessage);
			jobFuturesFor.put(job, taskExecutionServer.submit(new Runnable() {

				@Override
				public void run() {
					if (bcMessage.status() == BCMessageStatus.COMPLETED_TASK) {
						CompletedTaskBCMessage taskMsg = (CompletedTaskBCMessage) bcMessage;
						logger.info("processMessage::Next message to be sent to msgConsumer.handleCompletedTask(): " + taskMsg);
						messageConsumer.handleCompletedTask(job, taskMsg.allExecutorTaskDomains(), bcMessage.inputDomain());
					} else { // status == BCMessageStatus.COMPLETED_PROCEDURE
						CompletedProcedureBCMessage procMsg = (CompletedProcedureBCMessage) bcMessage;
						logger.info("processMessage::Next message to be sent to msgConsumer.handleCompletedProcedure(): " + procMsg);
						messageConsumer.handleCompletedProcedure(job, bcMessage.outputDomain(), bcMessage.inputDomain());
					}
				}
			}, job.priorityLevel(), job.creationTime(), job.id(), bcMessage.procedureIndex(), bcMessage.status(), bcMessage.creationTime()));
		} else {
			logger.info("aborting job");
			abortJobExecution(job);
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

	// Setter, Getter, Creator, Constructor follow below..
	protected JobCalculationBroadcastHandler(int nrOfConcurrentlyExecutedBCMessages) {
		super(nrOfConcurrentlyExecutedBCMessages);
	}

}
