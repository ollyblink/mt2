package mapreduce.engine.broadcasting.messages;

import java.io.Serializable;

import mapreduce.execution.domains.JobProcedureDomain;

public interface IBCMessage extends Serializable {
	public BCMessageStatus status();

	public Long creationTime();

	public Integer procedureIndex();

	public JobProcedureDomain outputDomain();

	public JobProcedureDomain inputDomain();

}
