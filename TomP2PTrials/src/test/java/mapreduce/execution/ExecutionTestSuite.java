package mapreduce.execution;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import mapreduce.execution.context.DHTStorageContextTest;
import mapreduce.execution.job.JobTest;
import mapreduce.execution.procedures.ProcedureTest;
import mapreduce.execution.procedures.ProceduresTest;
import mapreduce.execution.task.TaskTest;
import mapreduce.execution.task.taskdatacomposing.MaxFileSizeTaskDataComposerTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({ 
	TaskTest.class, 
	MaxFileSizeTaskDataComposerTest.class, 
	ProcedureTest.class, 
	ProceduresTest.class,
	JobTest.class, 
	DHTStorageContextTest.class 
})

public class ExecutionTestSuite {

}
