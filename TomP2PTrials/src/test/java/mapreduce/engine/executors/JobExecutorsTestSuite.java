package mapreduce.engine.executors;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import mapreduce.engine.executors.performance.PerformanceInfoTest;
@RunWith(Suite.class)
@Suite.SuiteClasses({
	PerformanceInfoTest.class,
	JobCalculationExecutorTest.class,
	JobSubmissionExecutorTest.class
})

public class JobExecutorsTestSuite {
 

}
