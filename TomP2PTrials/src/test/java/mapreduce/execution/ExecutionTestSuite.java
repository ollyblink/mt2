package mapreduce.execution;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import mapreduce.execution.context.DHTStorageContextTest;
import mapreduce.execution.jobs.JobTest;
import mapreduce.execution.procedures.ProcedureTest;
import mapreduce.execution.procedures.ProceduresTest;
import mapreduce.execution.tasks.TaskTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({ TaskTest.class, ProcedureTest.class, ProceduresTest.class, JobTest.class, DHTStorageContextTest.class })

public class ExecutionTestSuite {

}
