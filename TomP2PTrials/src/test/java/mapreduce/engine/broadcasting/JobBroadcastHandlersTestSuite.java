package mapreduce.engine.broadcasting;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import mapreduce.engine.broadcasting.broadcasthandlers.JobCalculationBroadcastHandlerTest;
import mapreduce.engine.broadcasting.broadcasthandlers.JobSubmissionBroadcastHandlerTest;
import mapreduce.engine.broadcasting.broadcasthandlers.timeouts.TimeoutTests;

@RunWith(Suite.class)
@Suite.SuiteClasses({ TimeoutTests.class, JobCalculationBroadcastHandlerTest.class,
		JobSubmissionBroadcastHandlerTest.class })

public class JobBroadcastHandlersTestSuite {

}
