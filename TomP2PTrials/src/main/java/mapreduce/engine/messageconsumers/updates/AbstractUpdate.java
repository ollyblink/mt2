package mapreduce.engine.messageconsumers.updates;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.domains.IDomain;
import mapreduce.execution.procedures.Procedure;

public abstract class AbstractUpdate implements IUpdate {
	private static Logger logger = LoggerFactory.getLogger(AbstractUpdate.class);

	@Override
	public void executeUpdate(Procedure procedure) {
		if (procedure != null) {
			try {
				internalUpdate(procedure);
			} catch (Exception e) {
				logger.warn("Exception caught", e);
			}
		} else {
			logger.warn("No update, either output domain or procedure or both were null.");
		}
	}

	/**
	 * Template method, as the executeUpdate takes care of nullpointers
	 * 
	 * @param outputDomain
	 * @param procedure
	 */
	protected abstract void internalUpdate(Procedure procedure) throws ClassCastException, NullPointerException;

	protected AbstractUpdate() {

	}
}
