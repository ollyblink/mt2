package mapreduce.engine.messageconsumers.updates;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.execution.domains.IDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.Procedure;

public class ProcedureUpdate extends AbstractUpdate {
	private static Logger logger = LoggerFactory.getLogger(ProcedureUpdate.class);

	private Job job;
	private JobCalculationMessageConsumer msgConsumer;

	public ProcedureUpdate(Job job, JobCalculationMessageConsumer msgConsumer) {
		this.job = job;
		this.msgConsumer = msgConsumer;
	}

	@Override
	protected void internalUpdate(IDomain outputDomain, Procedure procedure)
			throws ClassCastException, NullPointerException {
		JobProcedureDomain outputJPD = (JobProcedureDomain) outputDomain;
		logger.info("internalUpdate(" + outputJPD + ", " + procedure.executable().getClass().getSimpleName()
				+ ");");

		logger.info("internalUpdate::procedure.isCompleted() before adding outputJPD? "
				+ procedure.isCompleted());
		procedure.addOutputDomain(outputJPD);
		logger.info(
				"internalUpdate::procedure.isCompleted() after adding outputJPD? " + procedure.isCompleted());
		boolean procedureIsFinished = procedure.isFinished();
		logger.info("internalUpdate::procedure.isFinished() && procedure.dataInputDomain() != null? "
				+ procedureIsFinished + " && " + (procedure.dataInputDomain() != null) + "? "
				+ (procedureIsFinished && procedure.dataInputDomain() != null));
		if (procedureIsFinished && procedure.dataInputDomain() != null) {
			logger.info("internalUpdate::before cancelProcedureExecution("
					+ procedure.dataInputDomain().toString() + ")");
			msgConsumer.cancelProcedureExecution(procedure.dataInputDomain().toString());
			JobProcedureDomain resultOutputDomain = procedure.resultOutputDomain();
			logger.info("internalUpdate::resultOutputDomain is: " + resultOutputDomain);
			logger.info("internalUpdate::procedureindex before incrementProcIndex: "
					+ job.currentProcedure().procedureIndex());
			job.incrementProcedureIndex();
			logger.info("internalUpdate::procedureindex after incrementProcIndex: "
					+ job.currentProcedure().procedureIndex());
			job.currentProcedure().dataInputDomain(resultOutputDomain);
			logger.info("internalUpdate::next procedure to execute is ["
					+ job.currentProcedure().executable().getClass().getSimpleName() + "] on input data ["
					+ job.currentProcedure().dataInputDomain()+"]");
			logger.info("internalUpdate:: job.isJobFinished?" + job.isFinished());
			 
			if (job.isFinished()) {
				logger.info("internalUpdate::adding isJobFinished(true) to resultOutputDomain ["
						+ resultOutputDomain+"]");
				resultOutputDomain.isJobFinished(true);
			}
		}
		logger.info("internalUpdate:: done");
	}
}
// Below code should not be needed as EndProcedure is always finished...
// if
// (job.currentProcedure().executable().getClass().getSimpleName().equals(EndProcedure.class.getSimpleName()))
// {
// job.isFinished(true);
// }