package io.confluent.examples.producer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import kafka.admin.RackAwareMode;
import scala.collection.JavaConversions;

/**
 * This class contains static methods to operate on queues, allowing queue
 * creation and deletion, checking whether queues exist on the broker, and
 * listing all queues on the broker.
 *
 * A topic represents a queue.
 */

public class ZookeeperUtil {
    private static final int DEFAULT_SESSION_TIMEOUT = 10 * 1000;
    private static final int DEFAULT_CONNECTION_TIMEOUT = 8 * 1000;
    private static final String ZOOKEEPER_CONNECT = "localhost:2181";

    /**
     * Opens a new ZooKeeper client to access the Kafka broker.
     */
    private static ZkClient connectToZookeeper() {
        return new ZkClient(ZOOKEEPER_CONNECT,
                DEFAULT_SESSION_TIMEOUT,
                DEFAULT_CONNECTION_TIMEOUT,
                ZKStringSerializer$.MODULE$);

    }

    /**
     * Given a ZooKeeper client instance, accesses the broker and returns
     * information about Kafka's contents.
     *
     * @param zookeeperClient A ZooKeeper client to access broker information
     *                        through.
     */
    private static ZkUtils zookeeperUtility(ZkClient zookeeperClient) {
        boolean isSecureCluster = false;
        return new ZkUtils(zookeeperClient,
                new ZkConnection(ZOOKEEPER_CONNECT),
                isSecureCluster);
    }

    /**
     * Given its name, checks if a topic exists on the Kafka broker.
     * 
     * @param name The name of the topic.
     * @return <code>true</code> if the topic exists on the broker,
     *         <code>false</code> if it doesn't
     */
    public static boolean existsTopic(String name) {
        ZkClient zkClient = connectToZookeeper();
        ZkUtils zkUtils = zookeeperUtility(zkClient);
        boolean topicExists = AdminUtils.topicExists(zkUtils, name);
        zkClient.close();
        return topicExists;
    }

    /**
     * Creates new topics, which remain persistent on the Kafka broker.
     * 
     * @param names       The names of the topic.
     * @param partitions  The number of partitions in the topic.
     * @param replication The number of brokers to host the topic.
     */
    public static void createTopics(ArrayList<String> names, int partitions, int replication) {
        ZkClient zkClient = connectToZookeeper();
        ZkUtils zkUtils = zookeeperUtility(zkClient);
        try {
            for (String name : names) {
                if (existsTopic(name))
                    continue;
                AdminUtils.createTopic(zkUtils, name, partitions, replication,
                        new Properties(), RackAwareMode.Disabled$.MODULE$);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }

    /**
     * Given its name, deletes a topic on the Kafka broker.
     * 
     * @param names The name of the topic.
     */
    public static void deleteTopics(ArrayList<String> names) {
        ZkClient zkClient = connectToZookeeper();
        ZkUtils zkUtils = zookeeperUtility(zkClient);
        try {
            for (String name : names) {
                if (!existsTopic(name))
                    return;
                AdminUtils.deleteTopic(zkUtils, name);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }

    /** Lists all topics on the Kafka broker */
    public static void listTopics() {
        ZkClient zkClient = connectToZookeeper();
        ZkUtils zkUtils = zookeeperUtility(zkClient);

        try {
            List<String> brokerTopics = JavaConversions.seqAsJavaList(zkUtils.getAllTopics());
            for (String topic : brokerTopics)
                System.out.println(topic);
        }

        catch (

        Exception ex) {
            ex.printStackTrace();
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }

    }

}
