package mapreduce.engine.priorityexecutor;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ 
	ComparableTaskExecutionTaskTest.class, 
	ComparableBCMessageTaskTest.class,
	PriorityExecutorTest.class
})

public class PriorityExecutorTestSuite {

}
