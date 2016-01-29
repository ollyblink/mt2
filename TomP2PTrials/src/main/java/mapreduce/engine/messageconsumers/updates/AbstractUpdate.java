package mapreduce.engine.messageconsumers.updates;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.domains.IDomain;
import mapreduce.execution.procedures.Procedure;

public abstract class AbstractUpdate implements IUpdate {
	private static Logger logger = LoggerFactory.getLogger(AbstractUpdate.class);

	@Override
	public void executeUpdate(IDomain outputDomain, Procedure procedure) {
		if (outputDomain != null && procedure != null) {
			logger.info("executeUpdate(" + procedure.executable().getClass().getSimpleName() + ");");
			try { 
				internalUpdate(outputDomain, procedure);
			} catch (Exception e) {
				logger.warn("Exception caught", e);
			}
		} else {
			logger.warn("No update, either output domain or procedure or both were null.");
		}
		logger.info("executeUpdate::done");
	}

	/**
	 * Template method, as the executeUpdate takes care of nullpointers
	 * 
	 * @param outputDomain
	 * @param procedure
	 */
	protected abstract void internalUpdate(IDomain outputDomain, Procedure procedure)
			throws ClassCastException, NullPointerException;

	protected AbstractUpdate() {

	}
}
