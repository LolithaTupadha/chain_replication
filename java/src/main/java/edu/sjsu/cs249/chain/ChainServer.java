package edu.sjsu.cs249.chain;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import edu.sjsu.cs249.chain.TailStateUpdateRequest.Builder;

//import org.apache.zookeeper.Watcher.Event;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
//import java.util.TimerTask;


public class ChainServer extends TailChainReplicaGrpc.TailChainReplicaImplBase implements Watcher {

	private Server server;
	private static ZooKeeper zk;
	private static ZkConnector zkc;
	private long ssid;
	private long successor;
	private long prevsuccessor = -1L;
	private long predecessor;
	private long src;
    private static Long Xid;
	private boolean head, tail;
	private String znode_path = "/chain"; 
	private String zkAddress = "192.168.43.9:2181"; //Address on which zookeeper server is running
	private InetSocketAddress addr=null;
	List<TailStateUpdateRequest> sentList = new ArrayList<TailStateUpdateRequest>();
	HashMap<String, Integer> obj = new HashMap<String, Integer>();
	//private ManagedChannel channel;
    private TailChainReplicaGrpc.TailChainReplicaBlockingStub chainTailStub;
    private TailChainReplicaGrpc.TailChainReplicaBlockingStub successorStub;
    private TailChainReplicaGrpc.TailChainReplicaBlockingStub predecessorStub;
   // private TailClientGrpc.TailClientBlockingStub stub;

	public ChainServer(int port) {
		System.out.println("Starting the server on port " + port);
		this.server = ServerBuilder.forPort(port).addService(this).build();
	}
    
    public TailClientGrpc.TailClientBlockingStub connectToClient(String host, int port ) {
         ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
         return TailClientGrpc.newBlockingStub(channel);
    }
    private void insertValue(String key, int val) {
    	obj.put(key, val);
    }
    
    private void incrementValue(String key, int val) {
    	if (obj.containsKey(key))
			obj.put(key, obj.get(key) + val);
		else
			obj.put(key, val);	
    	System.out.println("HashMAp after increment");
    	for (String keys : obj.keySet())
    	{
    	   System.out.println(key+"   "+obj.get(key));
    	}
    }
    
    private void deleteValue(String key) {
    	obj.put(key, 0);
    }
    
    private void updateXid(Long Xid) {
    	this.Xid = Xid;
    }
    
    private Long getNewXid() {
    	return ++Xid;
    }
    public void proposeStateUpdate(TailStateUpdateRequest req, StreamObserver<ChainResponse> responseObserver) {
    	
    	if(req.getSrc() == predecessor) {
			System.out.println("Received to update key "+req.getKey()+" and value"+req.getValue());
    		insertValue(req.getKey(), req.getValue());
    		updateXid(req.getXid());
    		if(tail) {
    			//Respond back to client 
				System.out.println("State Update Successfull. Responding to client");
        		ManagedChannel channel = ((ManagedChannelBuilder)ManagedChannelBuilder.forAddress(req.getHost(), req.getPort()).usePlaintext()).build();
        		TailClientGrpc.newBlockingStub(channel).cxidProcessed(CxidProcessedRequest.newBuilder().setCxid(req.getCxid()).build());
    		}   
    		else {
        		//send this to next node in the chain
    	    	//zAblockingStub = connectToReplica(addr.getHostName(), addr.getPort());
    	        	Builder request = TailStateUpdateRequest.newBuilder();
    	        	request.setXid(req.getXid());
    	        	request.setKey(req.getKey());
    	        	request.setValue(req.getValue());
    	        	request.setHost(req.getHost());
    	        	request.setPort(req.getPort());
    	        	request.setCxid(req.getCxid());
    	        	request.setSrc(ssid);
    	            sentList.add(request.build());
    				System.out.println("State Update Successfull. Transfering state update to successor "+successor+" with Xid: "+request.getXid());
    			successorStub.proposeStateUpdate(request.build());	
    	    	}
    		ChainResponse rsp = ChainResponse.newBuilder().setRc(0).build();
            responseObserver.onNext(rsp);
            responseObserver.onCompleted();
    		
    	} else {
			System.out.println("My predecessor is "+predecessor+" but I got Propose state update request from "+req.getSrc());
    		ChainResponse rsp = ChainResponse.newBuilder().setRc(1).build();
            responseObserver.onNext(rsp);
            responseObserver.onCompleted();
    	}
      
    	
    }
    
    public void stateTransfer(TailStateTransferRequest req, StreamObserver<ChainResponse> responseObserver) {
    	//Node obsereved that there is a new tail. 
    	ChainResponse rsp;
    	if(req.getSrc() == predecessor) {
    		System.out.println("Performing state transfer: ");
    		System.out.println("Request received from: "+predecessor);
    		updateXid(req.getStateXid());
    		if(!req.getSentList().isEmpty())
    		     sentList = req.getSentList();
    		obj = new HashMap<String, Integer> (req.getStateMap());
    	    rsp = ChainResponse.newBuilder().setRc(0).build();			
    	}
    	 else {
     	    rsp = ChainResponse.newBuilder().setRc(1).build();			
    	}
    	responseObserver.onNext(rsp);
		responseObserver.onCompleted();	
    }
    
	public void increment(TailIncrementRequest req, StreamObserver<HeadResponse> responseObserver) {
		if(head) {
			String key = req.getKey();
			int val = req.getIncrValue();
			incrementValue(key, val);
			System.out.println("Value incremented by head");
			HeadResponse rsp = HeadResponse.newBuilder().setRc(0).build();
            responseObserver.onNext(rsp);
            responseObserver.onCompleted();
			TailStateUpdateRequest.Builder request = TailStateUpdateRequest.newBuilder();
			request.setXid(getNewXid());
			request.setKey(key);
			request.setValue(obj.get(key));
			request.setHost(req.getHost());
			request.setPort(req.getPort());
			request.setCxid(req.getCxid());
			request.setSrc(ssid);
			if(successor != -1) {
			sentList.add(request.build());
			System.out.println("Head started Propose state update to the its successor: "+successor+" with Xid"+request.getXid());
			ChainResponse response;
			try {
			 response = successorStub.proposeStateUpdate(request.build());
			}catch(Exception e) {
				e.printStackTrace();
			}
			} else if(tail) {
				//This is the only node
				System.out.println("Responding to the client about the incremented value");
				CxidProcessedRequest finishClient = CxidProcessedRequest.newBuilder().setCxid((int)req.getCxid()).build();
                ManagedChannel channel = ((ManagedChannelBuilder)ManagedChannelBuilder.forAddress(req.getHost(), req.getPort()).usePlaintext()).build();
				TailClientGrpc.newBlockingStub(channel).cxidProcessed(finishClient);
				//stub = connectToClient(req.getHost(), req.getPort());
        		//stub.cxidProcessed(CxidProcessedRequest.newBuilder().setCxid(req.getCxid()).build());
				
			}
		} else {
			//I am not head. I am not supposed to do increment
			HeadResponse rsp = HeadResponse.newBuilder().setRc(1).build();
			responseObserver.onNext(rsp);
			responseObserver.onCompleted();
		}
	}
	
	public void delete(TailDeleteRequest req, StreamObserver<HeadResponse> responseObserver) {
		
		if(head) {
			String key = req.getKey();
			deleteValue(key);
			System.out.println("Value deleted by head");
			HeadResponse rsp = HeadResponse.newBuilder().setRc(0).build();
            responseObserver.onNext(rsp);
            responseObserver.onCompleted();
			TailStateUpdateRequest.Builder request = TailStateUpdateRequest.newBuilder();
			request.setXid(getNewXid());
			request.setKey(key);
			request.setHost(req.getHost());
			request.setPort(req.getPort());
			request.setCxid(req.getCxid());
			request.setSrc(ssid);
			request.setValue(0);
			if(successor != -1) {
			sentList.add(request.build());
			System.out.println("Delete request propogated to the successor");
			successorStub.proposeStateUpdate(request.build());
			} else if(tail) {
				//This is the only node
				System.out.println("Delete response being sent to client");
				//stub = connectToClient(req.getHost(), req.getPort());
				ManagedChannel channel = ((ManagedChannelBuilder)ManagedChannelBuilder.forAddress(req.getHost(), req.getPort()).usePlaintext()).build();
        		TailClientGrpc.newBlockingStub(channel).cxidProcessed(CxidProcessedRequest.newBuilder().setCxid(req.getCxid()).build());
        		//stub.cxidProcessed(CxidProcessedRequest.newBuilder().setCxid(req.getCxid()).build());
				
			}
		} else {
			//I am not head. I am not supposed to do increment
			System.out.println("I am not head to do delete operation");
			HeadResponse rsp = HeadResponse.newBuilder().setRc(1).build();
			responseObserver.onNext(rsp);
			responseObserver.onCompleted();
		}
		
	}
	
	public void get(GetRequest req, StreamObserver<GetResponse> responseObserver) {
		String key = req.getKey();
		GetResponse rsp ;
		if(tail) {
			if(obj.containsKey(key))
				rsp = GetResponse.newBuilder().setRc(0).setValue(obj.get(key)).build();
			else 
				rsp = GetResponse.newBuilder().setRc(2).setValue(0).build();
		} else {
			System.out.println("I am not tail to do get operation");
			rsp = GetResponse.newBuilder().setRc(1).build();
		}
		responseObserver.onNext(rsp);
		responseObserver.onCompleted();	
		
	}
	
	public void getLatestXid(LatestXidRequest req, StreamObserver<LatestXidResponse> responseObserver) {
		//This has to happen periodically and clear Sent list
		LatestXidResponse rsp ;
		System.out.println("getLatestXid is called");
		if(tail) {
			rsp = LatestXidResponse.newBuilder().setRc(0).setXid(Xid).build();
		}else {
			//I'm not tail. rc =1
			rsp = LatestXidResponse.newBuilder().setRc(1).build();
		}
		responseObserver.onNext(rsp);
		responseObserver.onCompleted();	
	}
	
	private static InetSocketAddress str2addr(String addr) {
        int colon = addr.lastIndexOf(':');
        return new InetSocketAddress(addr.substring(0, colon), Integer.parseInt(addr.substring(colon+1)));
    }

	private void zkconnection() throws KeeperException, InterruptedException, IOException {
		
		System.out.println("Enter localhost and server port");
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		String host = reader.readLine();
		zkc = new ZkConnector();
		byte[] data = host.getBytes(); // IP Address and port of this replica, for others to make communication
		String sid = null;
		try {
			zk = zkc.connect(zkAddress);//Port on which zk is running
			ssid = zk.getSessionId();
			prevsuccessor = -1L;
			System.out.println("Created zk connection with session id"+ssid);
			sid = Long.toHexString(ssid);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		zk.create(znode_path+"/"+ sid, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		Xid=0L;//Initialize Xid
		initializeNodes();
	}
	
	private TailChainReplicaGrpc.TailChainReplicaBlockingStub getStub(long session) throws KeeperException, InterruptedException {
        byte data[] = zk.getData(znode_path+"/"+ Long.toHexString(session), false, null);
        InetSocketAddress addr = str2addr(new String(data).split("\n")[0]);
        ManagedChannel channel = ManagedChannelBuilder.forAddress(addr.getHostName(), addr.getPort()).usePlaintext().build();
        return TailChainReplicaGrpc.newBlockingStub(channel);
    }
	
	private void callStateTransfer(TailChainReplicaGrpc.TailChainReplicaBlockingStub succ) {
		//We call state transfer whenever a new successor is identified
		TailStateTransferRequest req = TailStateTransferRequest.newBuilder().setSrc(ssid).setStateXid(Xid).putAllState(obj).addAllSent(sentList).build();
        ChainResponse rsp = succ.stateTransfer(req);
        if(rsp.getRc() == 0) {
        	System.out.println("State Transfer Successfull");
        }
		}
	
	private void initializeNodes() throws InterruptedException, KeeperException {
		
			List<String> children = zk.getChildren(znode_path, new Watcher() {

				@Override
				public void process(WatchedEvent event) {
					try {
						initializeNodes();
					} catch (InterruptedException | KeeperException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
				
			});
			ArrayList<Long> sessions = new ArrayList<Long>();
			for (String child : children) {
				if(child.startsWith("0x")) child = child.substring(2);
				
		        sessions.add(Long.parseLong(child, 16));
				System.out.println(Long.parseLong(child, 16));
			}
			sessions.sort(Long::compare);
			int position = sessions.indexOf(ssid);
			//System.out.println("SSID is:"+ssid);
			//System.out.println("Position is:"+position);
			if(position == 0 && children.size()-1 ==0) {
				head = true;
				tail = true;
				successor = -1;
				predecessor = -1;
				successorStub = null;
				predecessorStub = null;
				chainTailStub = null;
				System.out.println("I am the only node");
			} else if(position == 0) {
				head = true;
				tail = false;
				predecessor = -1;
				successor = sessions.get(position+1);
				successorStub = getStub(successor);
				if(prevsuccessor == -1L || prevsuccessor!=successor) {
					prevsuccessor = successor;
				    callStateTransfer(successorStub);
				}
				predecessorStub = null;
				chainTailStub = getStub(sessions.get(sessions.size()-1));		
				System.out.println("I am the head");
			} else if(position == children.size()-1) {
				head = false;
				tail = true;
				successor = -1;
				predecessor = sessions.get(position-1);
				successorStub = null;
				predecessorStub = getStub(predecessor);
				System.out.println("I am the tail");
			} else {
				System.out.println("I am a replica");
				head = false;
				tail = false;
				successor = sessions.get(position+1);
				predecessor = sessions.get(position-1);
				predecessorStub = getStub(predecessor);
				successorStub = getStub(successor);
				if(prevsuccessor == -1L || prevsuccessor!=successor) {
					prevsuccessor = successor;
				    callStateTransfer(successorStub);
				}
				chainTailStub = getStub(sessions.get(sessions.size()-1));
			}
			
		
	}
	
	private void updateSentList() {
		try {
			initializeNodes();
		} catch (InterruptedException | KeeperException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		if(!sentList.isEmpty()) {
		if(head && tail) {
			sentList.clear();
			return;
		}else if(tail || chainTailStub==null) {
			sentList.clear();
			return;
		}
		}
		LatestXidResponse rsp = null;
		try {
		 rsp = chainTailStub.getLatestXid(LatestXidRequest.newBuilder().build());
		}catch(Exception e) {
			e.printStackTrace();
			if(rsp == null)
				return;
		}	
		
		if(rsp.getRc()==0) {
			ArrayList<Integer> lst = new ArrayList<Integer>();
			for(TailStateUpdateRequest rq:sentList) {
				if(rq.getXid() <= rsp.getXid())
					lst.add(sentList.indexOf(rq));
					//sentList.remove(sentList.indexOf(rq));
				else {
					//Tail has not seen some update. Hence we send that again
					ChainResponse chain_rsp = null;
					try {
					 chain_rsp = successorStub.proposeStateUpdate(rq);
					} catch (Exception e) {
						e.printStackTrace();
					}
					if(chain_rsp == null)
						return;
					if(chain_rsp.getRc() != 1)
						continue;
					else
						try {
							initializeNodes();
						} catch (InterruptedException | KeeperException e) {
							e.printStackTrace();
						}
				}
			}
			for(int ind:lst) {
	            System.out.println("Removing Xid:"+(sentList.get(ind)).getXid()+" from the sent list");
				sentList.remove(ind);
			}
		} else {
			//We need to talk to the latest tail
			//We do not end up in this case as with watcher, we have latest status of all nodes
			//But this case might occur during initializeNodes() is processing
			try {
				initializeNodes();
			} catch (InterruptedException | KeeperException e) {
				e.printStackTrace();
			}
			
		}
	}

	public static void main(String args[]) throws IOException, InterruptedException, KeeperException, Exception {

		ChainServer server = new ChainServer(2222);
		server.server.start();
		server.zkconnection();
		while(true) {
			server.updateSentList();
			Thread.sleep(5000);
		}
		//server.server.awaitTermination();

	}

	@Override
	public void process(WatchedEvent event) {
		if (event.getState().equals(Event.KeeperState.Expired) || event.getState().equals(Event.KeeperState.Closed)) {
            System.err.println("disconnected from zookeeper");
            System.exit(2);
        }
        try {
        	zkconnection();
		} catch (InterruptedException | KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
	

