package mapreduce.storage;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.broadcasting.broadcasthandlers.AbstractMapReduceBroadcastHandler;
import mapreduce.engine.broadcasting.messages.IBCMessage;
import mapreduce.utils.FileUtils;
import mapreduce.utils.IDCreator;
import mapreduce.utils.SyncedCollectionProvider;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.StorageDisk;

/**
 * Wrapper that abstracts the dht access to convenience methods
 * 
 * @author Oliver
 *
 */
public class DHTConnectionProvider implements IDHTConnectionProvider {
	private static final int DEFAULT_NUMBER_OF_PEERS = 1;
	private static Logger logger = LoggerFactory.getLogger(DHTConnectionProvider.class);
	private PeerDHT peerDHT;
	private AbstractMapReduceBroadcastHandler broadcastHandler;
	private String bootstrapIP;
	private int port;
	private String id;
	private String storageFilePath;
	private boolean isBootstrapper;
	private int bootstrapPort;

	private DHTConnectionProvider() {

	}

	private DHTConnectionProvider(String bootstrapIP, int bootstrapPort, int port) {
		this.id = IDCreator.INSTANCE.createTimeRandomID(this.getClass().getSimpleName());
		this.bootstrapIP = bootstrapIP;
		this.bootstrapPort = bootstrapPort;
		this.port = port;
		if (bootstrapPort == port) {
			this.isBootstrapper = true;
		}
	}

	public static DHTConnectionProvider create(String bootstrapIP, int bootstrapPort, int port) {
		return new DHTConnectionProvider(bootstrapIP, bootstrapPort, port).storageFilePath(null);
	}

	// GETTER/SETTER START
	// ======================

	@Override
	public DHTConnectionProvider storageFilePath(String storageFilePath) {
		this.storageFilePath = storageFilePath;
		return this;
	}

	/** Method for Testing purposes only... */
	public DHTConnectionProvider externalPeers(PeerDHT peerDHT, AbstractMapReduceBroadcastHandler bcHandler) {
		this.peerDHT = peerDHT;
		if (bcHandler != null) {
			this.broadcastHandler = bcHandler.dhtConnectionProvider(this);
		}
		return this;
	}

	@Override
	public AbstractMapReduceBroadcastHandler broadcastHandler() {
		return broadcastHandler;
	}

	@Override
	public DHTConnectionProvider broadcastHandler(AbstractMapReduceBroadcastHandler broadcastHandler) {
		this.broadcastHandler = broadcastHandler;
		return this;
	}
	// GETTER/SETTER FINISHED
	// ======================

	@Override
	public PeerDHT connect() throws Exception {
		if (broadcastHandler == null) {
			throw new Exception("Broadcasthandler not set!");
		} else {
			this.broadcastHandler.dhtConnectionProvider(this);
		}

		try {

			Peer peer = new PeerBuilder(Number160.createHash(this.id)).ports(port)
					.broadcastHandler(broadcastHandler).start();

			if (!this.isBootstrapper) {
				peer.bootstrap().inetAddress(InetAddress.getByName(bootstrapIP)).ports(bootstrapPort).start()
						.awaitUninterruptibly().addListener(new BaseFutureAdapter<FutureBootstrap>() {

							@Override
							public void operationComplete(FutureBootstrap future) throws Exception {
								if (future.isSuccess()) {
									logger.warn("successfully bootstrapped to " + bootstrapIP + "/"
											+ bootstrapPort);
								} else {
									logger.warn("No success on bootstrapping: fail reason: "
											+ future.failedReason());
								}
							}

						});
			}
			return connectDHT(peer);

		} catch (IOException e) {
			logger.debug("Exception on bootstrapping", e);
		}
		return null;
	}

	private PeerDHT connectDHT(Peer peer) {
		PeerBuilderDHT peerDHTBuilder = new PeerBuilderDHT(peer);

		if (storageFilePath != null) {
			File folder = FileUtils.INSTANCE.createTmpFolder(storageFilePath, peer.peerID().toString());
			peerDHTBuilder.storage(new StorageDisk(peer.peerID(), folder, null));
		}
		peerDHT = peerDHTBuilder.start();
		return peerDHT;
	}

	@Override
	public void broadcastCompletion(IBCMessage completedMessage) {
		Number160 bcHash = Number160.createHash(completedMessage.toString());
		NavigableMap<Number640, Data> dataMap = new TreeMap<Number640, Data>();
		try {
			dataMap.put(new Number640(bcHash, bcHash, bcHash, bcHash), new Data(completedMessage));
		} catch (IOException e) {
			e.printStackTrace();
		}
		peerDHT.peer().broadcast(bcHash).dataMap(dataMap).start();

	}

	@Override
	public void shutdown() {
		peerDHT.shutdown().awaitUninterruptibly().addListener(new BaseFutureAdapter<BaseFuture>() {

			@Override
			public void operationComplete(BaseFuture future) throws Exception {
				if (future.isSuccess()) {
					logger.info("Successfully shut down peer " + peerDHT.peerID() + ".");
				} else {
					logger.info("Could not shut down peer " + peerDHT.peerID() + ".");
				}
			}

		});
	}

	@Override
	public FutureGet getAll(String keyString, String domainString) {
		return peerDHT.get(Number160.createHash(keyString)).domainKey(Number160.createHash(domainString))
				.all().start();
	}

	@Override
	public FutureGet get(String keyString, String domainString) {
		return peerDHT.get(Number160.createHash(keyString)).domainKey(Number160.createHash(domainString))
				.start();
	}

	@Override
	public FuturePut add(String keyString, Object value, String domainString, boolean asList) {
		try {
			logger.info("add: Trying to perform: dHashtable.add(" + keyString + ", " + value + ").domain("
					+ domainString + ")");
			Data valueData = new Data(value);
			if (asList) {
				valueData = new Data(new Value(value));
			}

			return this.peerDHT.add(Number160.createHash(keyString)).data(valueData)
					.domainKey(Number160.createHash(domainString)).start();

		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public FuturePut addAll(String keyString, Collection<Data> values, String domainString) {
		logger.info("addAll: Trying to perform: dHashtable.add(" + keyString + ", " + values + ").domain("
				+ domainString + ")");
		return this.peerDHT.add(Number160.createHash(keyString)).dataSet(values)
				.domainKey(Number160.createHash(domainString)).start();
	}

	@Override
	public FuturePut put(String keyString, Object value, String domainString) {

		try {
			logger.info("put: Trying to perform: dHashtable.add(" + keyString + ", " + value + ").domain("
					+ domainString + ")");

			return this.peerDHT.put(Number160.createHash(keyString)).data(new Data(value))
					.domainKey(Number160.createHash(domainString)).start();

		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

}
