package mapreduce.engine.executors;

import mapreduce.engine.executors.performance.PerformanceInfo;
import mapreduce.execution.procedures.Procedure;
import mapreduce.storage.IDHTConnectionProvider;

public interface IExecutor {

	public String id();

	public IExecutor dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider);

	public PerformanceInfo performanceInformation();

	public IExecutor performanceInformation(PerformanceInfo performanceInformation);
 

}
