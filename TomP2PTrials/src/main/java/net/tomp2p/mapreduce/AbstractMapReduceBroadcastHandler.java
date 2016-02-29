package net.tomp2p.mapreduce;

import java.io.Serializable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.storage.DHTWrapper;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.StructuredBroadcastHandler;

public abstract class AbstractMapReduceBroadcastHandler extends StructuredBroadcastHandler implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8531956712914989163L;
	protected ThreadPoolExecutor executor;
	protected DHTWrapper dht;

	public AbstractMapReduceBroadcastHandler(DHTWrapper dht, ThreadPoolExecutor executor) {
		this.dht = dht;
		this.executor = executor;
	}

	public AbstractMapReduceBroadcastHandler(DHTWrapper dht) {
		this.dht = dht;
		this.executor = new ThreadPoolExecutor(1, 1, Long.MAX_VALUE, TimeUnit.DAYS, new LinkedBlockingQueue<>());
	}

	@Override
	public StructuredBroadcastHandler receive(Message message) {
		return super.receive(message);
	}

}
