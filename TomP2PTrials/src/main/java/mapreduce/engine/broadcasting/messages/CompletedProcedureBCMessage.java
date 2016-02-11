package mapreduce.engine.broadcasting.messages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.domains.JobProcedureDomain;

public class CompletedProcedureBCMessage extends AbstractBCMessage {
	protected final static Logger logger = LoggerFactory.getLogger(CompletedProcedureBCMessage.class);

	private static final long serialVersionUID = 8321149591470136899L;

	private CompletedProcedureBCMessage() {
		super(null, null);
	}

	private CompletedProcedureBCMessage(JobProcedureDomain outputDomain, JobProcedureDomain inputDomain) {
		super(outputDomain, inputDomain);
	}

	public static CompletedProcedureBCMessage create(JobProcedureDomain outputDomain, JobProcedureDomain inputDomain) {
		return new CompletedProcedureBCMessage(outputDomain, inputDomain);
	}  

	@Override
	public BCMessageStatus status() {
		return BCMessageStatus.COMPLETED_PROCEDURE;
	}

	@Override
	public String toString() {
		return super.toString();
	}
}
