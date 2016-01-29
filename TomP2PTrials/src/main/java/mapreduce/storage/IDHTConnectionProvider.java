package mapreduce.storage;

import java.util.Collection;
import java.util.List;

import mapreduce.engine.broadcasting.broadcasthandlers.AbstractMapReduceBroadcastHandler;
import mapreduce.engine.broadcasting.messages.IBCMessage;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.storage.Data;

public interface IDHTConnectionProvider {

	// DHT access

	/**
	 * 
	 * @param job
	 *            needed for the job domain to put this data into
	 * @param taskKey
	 *            defines a task's initial key
	 * @param value
	 *            the value to be stored for that task key
	 */
	public FuturePut add(String key, Object value, String domain, boolean asList);

	public FuturePut addAll(String key, Collection<Data> values, String domain);

	public FuturePut put(String key, Object value, String domain);

	public FutureGet getAll(String keyString, String domainString);

	public FutureGet get(String keyString, String domainString);

	public void broadcastCompletion(IBCMessage completedMessage);
	// Maintenance

	/**
	 * Creates a BroadcastHandler and Peer and connects to the DHT. If a bootstrap port and ip were provided (meaning, there are already peers
	 * connected to a DHT), it will be bootstrap to that node.
	 * 
	 * @param performBlocking
	 * @return
	 * @throws Exception
	 */
	public PeerDHT connect() throws Exception;

	public void shutdown();

	public IDHTConnectionProvider storageFilePath(String storageFilePath);

	public AbstractMapReduceBroadcastHandler broadcastHandler();

	public IDHTConnectionProvider broadcastHandler(AbstractMapReduceBroadcastHandler broadcastHandler);
  

	// public IDHTConnectionProvider isBootstrapper(boolean isBootstrapper);

}
