package edu.sjsu.cs249.chain;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.google.protobuf.GeneratedMessageV3;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChainClient extends TailClientGrpc.TailClientImplBase implements Watcher {
    private ZooKeeper zk;
    private Server server;
    private long currentChainTail;
    private long currentChainHead;
    private TailChainReplicaGrpc.TailChainReplicaBlockingStub  chainTail;
    private TailChainReplicaGrpc.TailChainReplicaBlockingStub  chainHead;
    private AtomicBoolean retryChainCheck = new AtomicBoolean();
    private static int cxid=1;
    private int _port = 4444; //port on which client is listening
    private String host= "192.168.43.10"; //Host on which client is running
    private String znode_path = "/chain";
    private static String zkAddr = "192.168.43.9:2181";
    private HashMap<Integer, GeneratedMessageV3> clientIds = new HashMap<Integer, GeneratedMessageV3>();

    private void setChainHead(long session) throws KeeperException, InterruptedException {
      //  System.out.println("In setChainHead");
        if (currentChainHead == session) {
            System.out.println("currentChainHead is equal to session");
            return;
        }
     //   System.out.println("Setting the head to "+Str);
        chainHead = null;
        chainHead = getStub(session);
        currentChainHead = session;
        System.out.println("Head is set to "+String.valueOf(session));
    }
    
    public ChainClient(int port) throws IOException {
		System.out.println("Listening on port " + port);
		this.server = ((ServerBuilder)ServerBuilder.forPort(port).addService(this)).build();
		this.server.start();
	}

    private TailChainReplicaGrpc.TailChainReplicaBlockingStub getStub(long session) throws KeeperException, InterruptedException {
       // System.out.println("Starting to retrieve data from zk session");
        byte data[] = zk.getData(String.valueOf(znode_path)+"/"+ Long.toHexString(session), false, null);
       // System.out.println("Data is retrieved from zk session" +data.toString());
        InetSocketAddress addr = str2addr(new String(data).split("\n")[0]);
        System.out.println("Address"+addr.getHostName());
        System.out.println("Port "+addr.getPort());
        ManagedChannel channel = ManagedChannelBuilder.forAddress(addr.getHostName(), addr.getPort()).usePlaintext().build();
        return TailChainReplicaGrpc.newBlockingStub(channel);
    }

    private void setChainTail(long session) throws KeeperException, InterruptedException {
        if (currentChainTail == session) {
            return;
        }
        chainTail = null;
        chainTail = getStub(session);
        currentChainTail = session;
        System.out.println("Tail is set");
    }

    private static InetSocketAddress str2addr(String addr) {
        int colon = addr.lastIndexOf(':');
        return new InetSocketAddress(addr.substring(0, colon), Integer.parseInt(addr.substring(colon+1)));
    }

    private ChainClient(String zkServer) throws KeeperException, InterruptedException, IOException {
        zk = new ZooKeeper(zkServer, 8000, this);
        this.server = (ServerBuilder.forPort(_port).addService(this)).build();
        System.out.println("ZooKeeper connected with session ID: " +zk.getSessionId());
        currentChainHead=-1;
        currentChainTail=-1;
        chainTail=null;
        chainHead = null;
        checkHeadTail();
        new ScheduledThreadPoolExecutor(1).scheduleAtFixedRate(
                () -> { if (retryChainCheck.getAndSet(false)) checkHeadTail();},
                5,
                5,
                TimeUnit.SECONDS
        );
    }
    
    public void cxidProcessed(CxidProcessedRequest request, StreamObserver<ChainResponse> responseObserver) {
    	ChainResponse rsp;
    	int cxid = request.getCxid();
    	if(clientIds.containsKey(cxid)) {
    		GeneratedMessageV3 req = clientIds.get(cxid);
    		if(req.getClass() == TailIncrementRequest.class) {
    			System.out.println("The increment request with key "+((TailIncrementRequest)req).getKey()+" and value "+((TailIncrementRequest)req).getIncrValue()+" is successful");
    		} else {
    			System.out.println("The delete request with key "+((TailDeleteRequest)req).getKey()+" is successful");
    		}    		
    	     clientIds.remove(request.getCxid());
    	     rsp = ChainResponse.newBuilder().setRc(0).build();
    	} else {
   	     rsp = ChainResponse.newBuilder().setRc(1).build();
    	}
    	responseObserver.onNext(rsp);
    	responseObserver.onCompleted();  	
    }

    private void get(String key) {
    	if(chainTail!=null) {
        GetResponse rsp = chainTail.get(GetRequest.newBuilder().setKey(key).build());
        if (rsp.getRc() != 0) {
        	if(rsp.getRc() == 2) 
        		System.out.println("Since key doesnt exist, value from get is 0");
        	else
        		System.out.printf("Error %d occurred\n", rsp.getRc());
        } else {
            System.out.printf("Value from get is %d\n", rsp.getValue());
        }
    	} else {
    		System.out.println("Tail is not available");
    	}
    }

    private void del(String key) {
    	if(chainHead != null) {
         int id = getCxid();
         TailDeleteRequest req = TailDeleteRequest.newBuilder().setKey(key).setCxid(id).setHost(host).setPort(_port).build();
         clientIds.put(id, req);
         HeadResponse rsp = chainHead.delete(req);
        if (rsp.getRc() != 0) {
            System.out.printf("Error %d occurred\n", rsp.getRc());
        } else {
            System.out.println("%s Delete Request for key "+key+" is started");
        }
      } else {
    	  System.out.println("Head is not available");
      }
    }

    private void inc(String key, int val) throws UnknownHostException {
        if(chainHead !=  null) {
        int id = getCxid();
        InetAddress inetAddress=null;
        try {
			 inetAddress = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        TailIncrementRequest req = TailIncrementRequest.newBuilder().setKey(key).setIncrValue(val).setCxid(id).setHost((inetAddress.getHostAddress()).toString()).setPort(_port).build();
        clientIds.put(id, req);
    	HeadResponse rsp = chainHead.increment(req);
        if (rsp.getRc() != 0) {
            System.out.printf("Error %d occurred\n", rsp.getRc());
        } else {
            System.out.println("Increment request with key "+key+" and value "+val+" started");
        }
       }else {
    	   System.out.println("Head is not available");
       }
    }
    private int getCxid() {
    	return cxid++;
    }

    public static void main(String args[]) throws Exception {
        ChainClient client = new ChainClient(zkAddr); // To connect to zookeeper
        client.server.start();
        Scanner scanner = new Scanner(System.in);
        String line;
        while ((line = scanner.nextLine()) != null) {
            String parts[] = line.split(" ");
            System.out.print(parts[0]);
            if (parts[0].equals("get")) {
                client.get(parts[1]);
            } else if (parts[0].equals("inc")) {
                client.inc(parts[1], Integer.parseInt(parts[2]));
            } else if (parts[0].equals("del")) {
                client.del(parts[1]);
            } else {
                System.out.println("don't know " + parts[0]);
                System.out.println("i know:");
                System.out.println("get key");
                System.out.println("inc key value");
                System.out.println("del key");
            }
        }
        client.server.awaitTermination();
    }

    private void checkHeadTail() {
    	//Determine the head and the tail
    	long head;
    	long tail;
        try {
            List<String> children = zk.getChildren(znode_path, new Watcher() {

				@Override
				public void process(WatchedEvent event) {
						checkHeadTail();
				}
				
			});
            ArrayList<Long> sessions = new ArrayList<Long>();
            for (String child: children) {
                try {
                    sessions.add(Long.parseLong(child, 16));
                    System.out.println(Long.parseLong(child, 16));
                } catch (NumberFormatException e) {

                }
            }
            if(sessions.size() == 0) {
                System.out.println("No nodes available");
            	return;
            } else if (sessions.size() == 1) {
            	  head = sessions.get(0);
            	  tail = sessions.get(children.size() - 1);
                  System.out.println("Only one node available");
                  setChainHead(head);
                  setChainTail(tail);
            } else {
            	sessions.sort(Long::compare);
                 head = sessions.get(0);
                System.out.println("Head"+head);
                 tail = sessions.get(children.size() - 1);
                 setChainHead(head);
                 setChainTail(tail);
            }     
        } catch (KeeperException | InterruptedException e) {
            retryChainCheck.set(true);
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getState().equals(Event.KeeperState.Expired) || watchedEvent.getState().equals(Event.KeeperState.Closed)) {
            System.err.println("disconnected from zookeeper");
            System.exit(2);
        }
        //System.out.println("In process");
        checkHeadTail();
    }
    
    
}
