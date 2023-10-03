

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.File;
import java.io.IOException;

public class Autohealer implements Watcher {

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;

    // Parent Znode where each worker stores an ephemeral child to indicate it is alive
    private static final String AUTOHEALER_ZNODES_PATH = "/workers";

    // Path to the worker jar
    private final String pathToProgram;

    // The number of worker instances we need to maintain at all times
    private final int numberOfWorkers;
    private ZooKeeper zooKeeper;
    private Watcher watcher;

    private int currentNumberOfWorkers ;

    public Autohealer(int numberOfWorkers, String pathToProgram) {
        this.numberOfWorkers = numberOfWorkers;
        this.pathToProgram = pathToProgram;
    }

    public void startWatchingWorkers() throws KeeperException, InterruptedException {
        if (zooKeeper.exists(AUTOHEALER_ZNODES_PATH, false) == null) {
            zooKeeper.create(AUTOHEALER_ZNODES_PATH, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        launchWorkersIfNecessary();
    }

    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to Zookeeper");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("Disconnected from Zookeeper event");
                        zooKeeper.notifyAll();
                    }
                }
                break;

            case NodeChildrenChanged:
                System.out.println("1 of workers is down");
                launchWorkersIfNecessary();

        }
    }

    private  void launchWorkersIfNecessary() {

            try {

                 currentNumberOfWorkers = zooKeeper.getChildren(AUTOHEALER_ZNODES_PATH, this).size();
                if (currentNumberOfWorkers < numberOfWorkers) {
                    System.out.println("number of current workers is " + currentNumberOfWorkers);
                    startNewWorker();
                }
              //  currentNumberOfWorkers = zooKeeper.getChildren(AUTOHEALER_ZNODES_PATH, this).size();
              //  Stat currentStatOfWorkerZode = zooKeeper.exists(AUTOHEALER_ZNODES_PATH, this);

            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }



    /**
     * Helper method to start a single worker
     * @throws IOException
     */
    private void startNewWorker() throws IOException {

        File file = new File(pathToProgram);
        String command = "java -jar " + file.getCanonicalPath();
        System.out.println(String.format("Launching worker instance : %s ", command));
        Runtime.getRuntime().exec(command, null, file.getParentFile());
      //  currentNumberOfWorkers = zooKeeper.getChildren(AUTOHEALER_ZNODES_PATH, this).size();
          }
}
