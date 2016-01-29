package mapreduce.engine.messageconsumers;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import mapreduce.engine.messageconsumers.updates.UpdateTestSuite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ 
	JobCalculationMessageConsumerTest.class,
	JobSubmissionMessageConsumerTest.class, 
	UpdateTestSuite.class
})

public class JobMessageConsumersTestSuite {

}
