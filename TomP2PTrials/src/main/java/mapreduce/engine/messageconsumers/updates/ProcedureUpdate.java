package mapreduce.engine.messageconsumers.updates;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.Procedure;

public class ProcedureUpdate extends AbstractUpdate {
	private static Logger logger = LoggerFactory.getLogger(ProcedureUpdate.class);

	private Job job;
	private JobCalculationMessageConsumer msgConsumer;
	private JobProcedureDomain outputDomain;

	private ProcedureUpdate(Job job, JobCalculationMessageConsumer msgConsumer, JobProcedureDomain outputDomain) {
		this.job = job;
		this.msgConsumer = msgConsumer;
		this.outputDomain = outputDomain;
	}

	@Override
	protected void internalUpdate(Procedure procedure) throws NullPointerException {
//		logger.info("internalUpdate:: "+(outputDomain.executor().equals(JobCalculationExecutor.classId)? " received output domain is from myself": "received output domain is from other ["+outputDomain.executor()+"]"));
//		logger.info("internalUpdate:: adding outputDomain: " + outputDomain);
		procedure.addOutputDomain(outputDomain);
		boolean procedureIsFinished = procedure.isFinished();
//		logger.info("internalUpdate:: procedureIsFinished: " + procedureIsFinished + ", procedure.dataInputDomain() != null: " + (procedure.dataInputDomain() != null));
		if (procedureIsFinished && procedure.dataInputDomain() != null) {
//			logger.info("internalUpdate:: procedureIsFinished: " + procedureIsFinished+", cancel procedure execution for inputdomain: " + procedure.dataInputDomain());
			msgConsumer.cancelProcedure(procedure.dataInputDomain().toString());
// 			logger.info("internalUpdate:: new procedure input domain: "+  procedure.resultOutputDomain());
			JobProcedureDomain newInputDomain = procedure.resultOutputDomain();
			job.incrementProcedureIndex();
			job.currentProcedure().dataInputDomain(newInputDomain);
			if (job.isFinished()) {
//				logger.info("job is finished");
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