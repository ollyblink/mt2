package mapreduce.engine.componenttests;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ JobCalculationComponentTest.class, JobSubmissionComponentTest.class })
public class ComponentTestSuite {

}
