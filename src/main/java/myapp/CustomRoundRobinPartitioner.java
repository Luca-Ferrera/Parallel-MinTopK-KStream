package myapp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

public class CustomRoundRobinPartitioner implements Partitioner {
    private final ConcurrentMap<String, AtomicInteger> topicCounterMap = new ConcurrentHashMap();

    public CustomRoundRobinPartitioner() {
    }

    public void configure(Map<String, ?> configs) {
    }

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        int nextValue = this.nextValue(topic);
        List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
        if (!availablePartitions.isEmpty()) {
            int part = Utils.toPositive(nextValue) % availablePartitions.size();
            return ((PartitionInfo)availablePartitions.get(part)).partition();
        } else {
            return Utils.toPositive(nextValue) % numPartitions;
        }
    }

    private int nextValue(String topic) {
        AtomicInteger counter = (AtomicInteger)this.topicCounterMap.computeIfAbsent(topic, (k) -> {
            return new AtomicInteger(0);
        });
        return counter.getAndIncrement();
    }

    @Override
    public void onNewBatch(String topic, Cluster cluster, int prevPartition) {
        AtomicInteger counter = (AtomicInteger)this.topicCounterMap.computeIfAbsent(topic, (k) -> {
            return new AtomicInteger(0);
        });
        counter.getAndDecrement();
    }

    public void close() {
    }
}
