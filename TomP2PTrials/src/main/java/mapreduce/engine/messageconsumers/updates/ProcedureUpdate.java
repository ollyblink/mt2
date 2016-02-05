package mapreduce.engine.messageconsumers.updates;

import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.Procedure;

public class ProcedureUpdate extends AbstractUpdate {
	// private static Logger logger = LoggerFactory.getLogger(ProcedureUpdate.class);

	private Job job;
	private JobCalculationMessageConsumer msgConsumer;
	private JobProcedureDomain outputDomain;

	private ProcedureUpdate(Job job, JobCalculationMessageConsumer msgConsumer, JobProcedureDomain outputDomain) {
		this.job = job;
		this.msgConsumer = msgConsumer;
		this.outputDomain = outputDomain;
	}

	@Override
	protected void internalUpdate(Procedure procedure) throws ClassCastException, NullPointerException {
		procedure.addOutputDomain(outputDomain);
		boolean procedureIsFinished = procedure.isFinished();
		if (procedureIsFinished && procedure.dataInputDomain() != null) {
			msgConsumer.cancelProcedureExecution(procedure.dataInputDomain().toString());
			JobProcedureDomain newInputDomain = procedure.resultOutputDomain();
			job.incrementProcedureIndex();
			job.currentProcedure().dataInputDomain(newInputDomain);
			if (job.isFinished()) {
				job.currentProcedure().dataInputDomain().isJobFinished(true);
			}
		}
	}

	public static ProcedureUpdate create(Job job, JobCalculationMessageConsumer msgConsumer, JobProcedureDomain outputDomain) {
		return new ProcedureUpdate(job, msgConsumer, outputDomain);
	}

	private ProcedureUpdate() {

	}
}