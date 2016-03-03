package net.tomp2p.mapreduce;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.peers.PeerAddress;

public interface OperationMapper2 {

	FutureResponse create(ChannelCreator channelCreator, PeerAddress next);

	void interMediateResponse(FutureResponse futureResponse);

	void response(FutureTask futureTask, FutureDone<Void> futuresCompleted);

}
