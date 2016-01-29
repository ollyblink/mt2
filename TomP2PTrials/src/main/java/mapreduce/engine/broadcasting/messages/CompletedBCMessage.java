package mapreduce.engine.broadcasting.messages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.messageconsumers.IMessageConsumer;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.IDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;

public class CompletedBCMessage implements IBCMessage {
	protected static Logger logger = LoggerFactory.getLogger(CompletedBCMessage.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = 8321149591470136899L;
	private final IDomain outputDomain;
	private final JobProcedureDomain inputDomain;
	private final BCMessageStatus status;
	private Long creationTime;

	private CompletedBCMessage(IDomain outputDomain, JobProcedureDomain inputDomain, BCMessageStatus status) {
		this.outputDomain = outputDomain;
		this.inputDomain = inputDomain;
		this.status = status;
		this.creationTime = System.currentTimeMillis();
	}

	public static CompletedBCMessage createCompletedTaskBCMessage(IDomain outputDomain, JobProcedureDomain inputDomain) {
		return new CompletedBCMessage(outputDomain, inputDomain, BCMessageStatus.COMPLETED_TASK);
	}

	public static CompletedBCMessage createCompletedProcedureBCMessage(IDomain outputDomain, JobProcedureDomain inputDomain) {
		return new CompletedBCMessage(outputDomain, inputDomain, BCMessageStatus.COMPLETED_PROCEDURE);
	}

	@Override
	public IDomain outputDomain() {
		return outputDomain;
	}

	@Override
	public JobProcedureDomain inputDomain() {
		return inputDomain;
	}

	@Override
	public BCMessageStatus status() {
		return status;
	}

	@Override
	public void execute(Job job, IMessageConsumer messageConsumer) {
		if (status == BCMessageStatus.COMPLETED_TASK) {
			logger.info("execute(job, msgConsumer) TASK [" + ((ExecutorTaskDomain) outputDomain).taskId() + "] completed");
			messageConsumer.handleCompletedTask(job, (ExecutorTaskDomain) outputDomain, inputDomain);
		} else { // status == BCMessageStatus.COMPLETED_PROCEDURE
			logger.info("execute(job, msgConsumer) PROCEDURE [" + ((JobProcedureDomain) outputDomain).procedureSimpleName() + "] completed");
			messageConsumer.handleCompletedProcedure(job, (JobProcedureDomain) outputDomain, inputDomain);
		}
	}

	@Override
	public String toString() {
		return "CompletedBCMessage(" + status + ") [outputDomain=" + outputDomain.toString() + " from inputDomain=" + inputDomain.toString() + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((inputDomain == null) ? 0 : inputDomain.hashCode());
		result = prime * result + ((outputDomain == null) ? 0 : outputDomain.hashCode());
		result = prime * result + ((status == null) ? 0 : status.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CompletedBCMessage other = (CompletedBCMessage) obj;
		if (inputDomain == null) {
			if (other.inputDomain != null)
				return false;
		} else if (!inputDomain.equals(other.inputDomain))
			return false;
		if (outputDomain == null) {
			if (other.outputDomain != null)
				return false;
		} else if (!outputDomain.equals(other.outputDomain))
			return false;
		if (status != other.status)
			return false;
		return true;
	}

	@Override
	public Long creationTime() {
		return this.creationTime;
	}

	@Override
	public Integer procedureIndex() {
		return (outputDomain instanceof JobProcedureDomain ? ((JobProcedureDomain) outputDomain).procedureIndex() : ((ExecutorTaskDomain) outputDomain).jobProcedureDomain().procedureIndex());
	}

}
