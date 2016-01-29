package mapreduce.engine;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import mapreduce.engine.broadcasting.JobBroadcastHandlersTestSuite;
import mapreduce.engine.componenttests.ComponentTestSuite;
import mapreduce.engine.executors.JobExecutorsTestSuite;
import mapreduce.engine.messageconsumers.JobMessageConsumersTestSuite;
import mapreduce.engine.priorityexecutor.PriorityExecutorTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({ 
	JobBroadcastHandlersTestSuite.class, 
	JobExecutorsTestSuite.class,
	JobMessageConsumersTestSuite.class,
	ComponentTestSuite.class,
	PriorityExecutorTest.class
})
public class EngineTestSuite {

}
