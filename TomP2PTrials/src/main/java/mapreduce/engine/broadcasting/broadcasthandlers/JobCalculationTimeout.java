package mapreduce.engine.broadcasting.broadcasthandlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.broadcasting.messages.CompletedProcedureBCMessage;
import mapreduce.engine.broadcasting.messages.IBCMessage;
import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.Procedure;

public class JobCalculationTimeout extends AbstractTimeout {
	private static Logger logger = LoggerFactory.getLogger(JobCalculationTimeout.class);

	public JobCalculationTimeout(JobCalculationBroadcastHandler broadcastHandler, Job job, long retrievalTimestamp, IBCMessage bcMessage, long timeToLive, boolean guessTimeout, double fraction) {
		super(broadcastHandler, job, retrievalTimestamp, bcMessage, timeToLive, guessTimeout, fraction);
	}

	@Override
	public void run() {
		sleep();
		JobCalculationBroadcastHandler broadcastHandler2 = ((JobCalculationBroadcastHandler) this.broadcastHandler);
		// synchronized (this.broadcastHandler) {
		// logger.info("for " + broadcastHandler.executorId() + " Timeout for job " + job + ", last bc message: " + bcMessage);
		JobProcedureDomain inputDomain = bcMessage.inputDomain();
		if (inputDomain != null && inputDomain.procedureIndex() == -1) {
			// logger.info("run::StartProcedure!handle start differently first, because it may be due to expected file size that is not the same..");
			// handle start differently first, because it may be due to expected file size that is not the same...
			Procedure currentProcedure = job.currentProcedure();
			int actualTasksSize = currentProcedure.tasksSize();
			int expectedTasksSize = inputDomain.expectedNrOfFiles();
			// logger.info("run::currentProcedure: " + currentProcedure.executable().getClass().getSimpleName() + ", tasksize: " + actualTasksSize
			// + ", received from inputDomain.expectedNrOfFiles(): " + expectedTasksSize);
			if (actualTasksSize < expectedTasksSize) {
				// logger.info("run::actualTasksSize < expectedTasksSize? " + (actualTasksSize < expectedTasksSize));
				currentProcedure.dataInputDomain().expectedNrOfFiles(expectedTasksSize);
				// JobCalculationExecutor executor = (JobCalculationExecutor) broadcastHandler.messageConsumer().executor();
				CompletedProcedureBCMessage msg = JobCalculationExecutor.create().dhtConnectionProvider(broadcastHandler2.dhtConnectionProvider()).tryCompletingProcedure(currentProcedure);
				if (msg != null) {
					broadcastHandler2.processMessage(msg, broadcastHandler2.getJob(job.id()));
					broadcastHandler2.dhtConnectionProvider().broadcastCompletion(msg);
					// logger.info("run:: Broadcasted Completed Procedure MSG: " + msg);
				}
			}
		} else {
			broadcastHandler2.cancelJob(job);
		}
		// }
	}

}
