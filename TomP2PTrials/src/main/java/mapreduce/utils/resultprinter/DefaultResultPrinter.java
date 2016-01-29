package mapreduce.utils.resultprinter;

import java.util.Set;

import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.peers.Number640;

public class DefaultResultPrinter implements IResultPrinter {
	public void printResults(IDHTConnectionProvider dhtConnectionProvider, String outputDomainString) {
		dhtConnectionProvider.getAll(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, outputDomainString).awaitUninterruptibly()
				.addListener(new BaseFutureAdapter<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						if (future.isSuccess()) {
							Set<Number640> keySet = future.dataMap().keySet();
							System.out.println("Found: " + keySet.size() + " finished tasks.");
//							for (Number640 k : keySet) {
//								String key = (String) future.dataMap().get(k).object();
//								dhtConnectionProvider.getAll(key, outputDomainString.toString()).addListener(new BaseFutureAdapter<FutureGet>() {
//
//									@Override
//									public void operationComplete(FutureGet future) throws Exception {
//										if (future.isSuccess()) {
//											Set<Number640> keySet2 = future.dataMap().keySet();
//											String values = "";
//											for (Number640 k2 : keySet2) {
//												values += ((Value) future.dataMap().get(k2).object()).value() + ", ";
//											}
//											System.err.println(key + ":" + values);
//										}
//									}
//
//								});
//							}
						}
					}

				});
	}

	private DefaultResultPrinter() {

	}

	public static IResultPrinter create() {
		return new DefaultResultPrinter();
	}
}
