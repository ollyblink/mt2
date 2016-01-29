package mapreduce.engine.messageconsumers.updates;

import mapreduce.execution.domains.IDomain;
import mapreduce.execution.procedures.Procedure;

/** Only used to distinguish if its a completed procedure or task to update */
public interface IUpdate {
	/**
	 * 
	 * @param outputDomain
	 * @param procedure
	 * @return updated procedure (either the same as before or the one to which it was updated) or null if procedure input parameter was null
	 */
	public void executeUpdate(IDomain outputDomain, Procedure procedure);

}
