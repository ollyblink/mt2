/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.tomp2p.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import mapreduce.execution.procedures.SumSummer;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.FutureSend;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.Futures;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.storage.Data;



/**
 *
 * @author draft
 */
public class ExampleMapReduce {
    private static class MyTaskMap {
        private PeerDHT d;
        public MyTaskMap(PeerDHT d) {
            this.d = d;
        }


        public void broadcastReceiver(NavigableMap<Number640, Data> input) throws Exception {
            
            String text = "auaoeu a oea,.py,.py gcrygc"; // from input
            
            int len = text.length() / 2;
            String block1 = text.substring(0, len);
            String block2 = text.substring(len + 1);
            
            FuturePut fp1 = d.put(Number160.ONE).data(new Data(block1)).start();
            FutureSend fs1 = d.send(Number160.ONE).object(block1).start();

            FuturePut fp2 = d.put(Number160.ZERO).data(new Data(block2)).start();
            //TODO: bytecode serialize
            FuturePut fp3 = d.put(Number160.MAX_VALUE).data(null).start(); //map task
            FuturePut fp4 = d.put(Number160.MAX_VALUE).data(null).start(); //reduce task
            FuturePut fp5 = d.put(Number160.MAX_VALUE).data(null).start(); //write disk
            Futures.whenAll(fp1, fp2, fp3, fp4).addListener(new BaseFutureAdapter<BaseFuture>() {
                @Override
                public void operationComplete(BaseFuture future) throws Exception {
                    NavigableMap<Number640, Data> map = new TreeMap<>();
                    //TODO: make proper n640
                    map.put(Number640.ZERO, new Data());
                    map.put(Number640.ZERO, new Data());
                    map.put(Number640.ZERO, new Data());
                    //periodic call if finished broadcast not received
                    d.peer().broadcast(Number160.ONE).dataMap(make(o));
                }
            });
        }
        
    }
    
    private static class MyTaskResultToDisk {
        Number640 previousId;
        Number640 currentId;
        private int counter = 0;
        public void broadcastReceiver(NavigableMap<Number640, Data> input) throws Exception {
            if(counter++ > 3) {
                FutureSend fs3 = d.send(Number160.ONE).start();
                //when finished broadcast
            }
        }
    }
    
    private static class MyTaskReduce {
        Number640 previousId;
        Number640 currentId;
        public void broadcastReceiver(NavigableMap<Number640, Data> input) throws Exception {
            FutureSend fs3 = d.send(Number160.ONE).start();
            //when finished broadcast
        }
    }
    
    private static class MyTaskMap2 {
        Number640 previousId;
        Number640 currentId;
        private PeerDHT d;
        public MyTaskMap2(PeerDHT d) {
            this.d = d;
        }

        public void broadcastReceiver(NavigableMap<Number640, Data> input) throws Exception {
            
            
            FutureSend fs3 = d.send(Number160.ONE).start(); //from input; //fp1 //dev should be able to hell how many //keep alive
            fg1.addListener(new BaseFutureAdapter<BaseFuture>() {
                @Override
                public void operationComplete(BaseFuture future) throws Exception {
                    FutureGet fg1 = d.get(Number160.ONE).start(); //from input; //fp1
                    FutureGet fg2 = d.get(Number160.ONE).start(); //from input; //fp2
                    //Futures.whenAll() ->
                    if(future.isSuccess()) {
                        //start // go for "auaoeu a oe" and ".py,.py gcrygc"
                        //emit(auaoeu, 1)
                        //emit(a oe, 1)
                        //emit(.py,.py, 1)
                        //emit(gcrygc, 1)
                        FuturePut fp = d.put(Number160.createHash("auaoeu")).domainKey(Number160.ONE).data(1);
                        //when finished broadcast
                        
                    } else {
                        //do nothing
                    }
                } 
                
            });
        }
        
    }
    
    public static NavigableMap<Number640, Data> make (Object o) throws IOException {
        NavigableMap<Number640, Data> tmp = new TreeMap<>();
        
        tmp.put(Number640.ZERO, new Data(o));
        return tmp;
    }
    
    public static void main(String[] args) throws IOException {
        final Peer p = new PeerBuilder(Number160.ONE).start();
        ObjectDataReply o =new ObjectDataReply() {
            private Set<String> executed = new HashSet<>();
            @Override
            public Object reply(PeerAddress sender, Object request) throws Exception {
                //if request is store -> store
                //if request is get
                if(!executed.contains(null)) // from request
                {
                    executed.add(null);
                    PeerConnection pc = null;
                    pc.closeFuture().addListener(new BaseFutureAdapter<BaseFuture>() {
                        @Override
                        public void operationComplete(BaseFuture future) throws Exception {
                            executed.remove(null);
                            p.broadcast(Number160.ONE).dataMap(make(o));
                        }
                    });
                }
                return null;
            }
        };
        p.objectDataReply(o);
        
        o.getClass().getName()
        
        MyTaskMap t = new MyTaskMap;
        
        t.class.get(t.getClass().getn) // java.lang.String -> java/lang/String.class -> java/lang/String$1.class
                
        PeerDHT d = new PeerBuilderDHT(p).start();
        MyTaskMap mapTask = new MyTaskMap(d); //TODO: input text!
        Submitter submitter = new Submitter();
        submitter.submit(new Task(mapTask)/* input data*/);
    }
    
}
