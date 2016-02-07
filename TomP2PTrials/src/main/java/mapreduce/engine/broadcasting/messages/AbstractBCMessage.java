package mapreduce.engine.broadcasting.messages;

import mapreduce.execution.domains.JobProcedureDomain;

public abstract class AbstractBCMessage implements IBCMessage {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8488926766656335892L;
	private final JobProcedureDomain outputDomain;
	private final JobProcedureDomain inputDomain;
	private final Long creationTime = System.currentTimeMillis();

	protected AbstractBCMessage(JobProcedureDomain outputDomain, JobProcedureDomain inputDomain) {
		this.outputDomain = outputDomain;
		this.inputDomain = inputDomain;
	}

	@Override
	public Long creationTime() {
		return creationTime;
	}

	@Override
	public Integer procedureIndex() {
		return outputDomain.procedureIndex();
	}

	@Override
	public JobProcedureDomain outputDomain() {
		return outputDomain;
	}

	@Override
	public JobProcedureDomain inputDomain() {
		return inputDomain;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((creationTime == null) ? 0 : creationTime.hashCode());
		result = prime * result + ((inputDomain == null) ? 0 : inputDomain.hashCode());
		result = prime * result + ((outputDomain == null) ? 0 : outputDomain.hashCode());
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
		AbstractBCMessage other = (AbstractBCMessage) obj;
		if (creationTime == null) {
			if (other.creationTime != null)
				return false;
		} else if (!creationTime.equals(other.creationTime))
			return false;
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
		return true;
	}
	
	
}
