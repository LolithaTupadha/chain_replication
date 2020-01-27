package edu.sjsu.cs249.chain;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class ZkConnector {
	private ZooKeeper zk;
    private CountDownLatch connSignal = new CountDownLatch(1);
    
    public ZooKeeper connect(String host) throws Exception {
        zk = new ZooKeeper(host, 3000, new Watcher() {
            public void process(WatchedEvent event) {
            	System.out.println("Event "+event);
                if (event.getState() == KeeperState.SyncConnected) {
                    connSignal.countDown();
                }
                
            }
        });
        connSignal.await();
        return zk;
    }

    public void close() throws InterruptedException {
        zk.close();
        System.out.print("Connection closed");
    }
    
    
	
}