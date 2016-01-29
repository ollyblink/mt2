package mapreduce;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import mapreduce.engine.EngineTestSuite;
import mapreduce.execution.ExecutionTestSuite;
import mapreduce.storage.DHTConnectionProviderTest;
@RunWith(Suite.class)
@Suite.SuiteClasses({
	ExecutionTestSuite.class,
	EngineTestSuite.class,
	DHTConnectionProviderTest.class
})
public class MapReduceTestSuite {
 
}
