package mapreduce.utils;

import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.JobProcedureDomain;

public enum DomainProvider {
	INSTANCE;
	public static final String PROCEDURE_OUTPUT_RESULT_KEYS = "PROCEDURE_OUTPUT_RESULT_KEYS";
	public static final String TASK_OUTPUT_RESULT_KEYS = "TASK_OUTPUT_RESULT_KEYS";
	public static final String JOB = "JOB";
	public static final String INITIAL_PROCEDURE = "INITIAL_PROCEDURE";

	public String executorTaskDomain(ExecutorTaskDomain executorTaskDomainParameter) {
		// ETD = EXECUTOR_TASK_DOMAIN
		// T = taskId
		// E = taskExecutor
		// TSI = taskStatusIndex
		// S = taskSubmissionCount
		// C = taskCreationTime
		return "ETD[T(" + executorTaskDomainParameter.taskId() + ")_P(" + executorTaskDomainParameter.executor() + ")_JSI("
				+ executorTaskDomainParameter.taskStatusIndex() + ")]";
	}

	// Job procedure domain key generation
	public String jobProcedureDomain(JobProcedureDomain jobProcedureDomainParameter) {
		// JPD = JOB_PROCEDURE_DOMAIN
		// J = jobId
		// JS = jobSubmissionCount
		// PE = procedureExecutor
		// P = procedureSimpleName
		// PI = procedureIndex
		return "JPD[J(" + jobProcedureDomainParameter.jobId() + ")_JS(" + jobProcedureDomainParameter.jobSubmissionCount() + ")_PE("
				+ jobProcedureDomainParameter.executor() + ")_P(" + jobProcedureDomainParameter.procedureSimpleName().toUpperCase() + ")_PI("
				+ jobProcedureDomainParameter.procedureIndex() + ")]";
	}

	// End Job procedure domain key generation

	public String concatenation(JobProcedureDomain jobProcedureDomainParameter, ExecutorTaskDomain executorTaskDomainParameter) {
		// C = CONCATENATION
		return "C{" + jobProcedureDomain(jobProcedureDomainParameter) + "}:::{" + executorTaskDomain(executorTaskDomainParameter) + "}";
	}

	public static void main(String[] args) {

	}

}
