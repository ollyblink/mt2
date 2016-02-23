package mapreduce.engine.messageconsumers;

import mapreduce.engine.executors.IExecutor;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.PerformanceInfo;

public abstract class AbstractMessageConsumer implements IMessageConsumer {

//	protected IExecutor executor;
	protected IDHTConnectionProvider dhtConnectionProvider;
//	protected PerformanceInfo performanceInformation;

//	@Override
//	public IExecutor executor() {
//		return this.executor;
//	}

	@Override
	public IMessageConsumer dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		return this;
	}

//	@Override
//	public IMessageConsumer executor(IExecutor executor) {
//		this.executor = executor;
//		this.performanceInformation = executor.performanceInformation();
//		return this;
//	}

}
