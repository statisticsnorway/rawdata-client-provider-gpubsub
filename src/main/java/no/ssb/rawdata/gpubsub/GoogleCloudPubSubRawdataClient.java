package no.ssb.rawdata.gpubsub;

import com.google.api.gax.core.CredentialsProvider;
import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataCursor;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataNoSuchPositionException;
import no.ssb.rawdata.api.RawdataProducer;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

class GoogleCloudPubSubRawdataClient implements RawdataClient {

    final String projectId;
    final CredentialsProvider credentialsProvider;
    final int consumerPrefetchThreshold;
    final int consumerAckTimeIntervalMs;

    final AtomicBoolean closed = new AtomicBoolean(false);

    final List<GoogleCloudPubSubRawdataProducer> producers = new CopyOnWriteArrayList<>();
    final List<GoogleCloudPubSubRawdataConsumer> consumers = new CopyOnWriteArrayList<>();

    GoogleCloudPubSubRawdataClient(String projectId, CredentialsProvider credentialsProvider, int consumerPrefetchThreshold, int consumerAckTimeIntervalMs) {
        this.projectId = projectId;
        this.credentialsProvider = credentialsProvider;
        this.consumerPrefetchThreshold = consumerPrefetchThreshold;
        this.consumerAckTimeIntervalMs = consumerAckTimeIntervalMs;
    }

    @Override
    public RawdataProducer producer(String topic) {
        GoogleCloudPubSubRawdataProducer producer = new GoogleCloudPubSubRawdataProducer(projectId, credentialsProvider, topic);
        producers.add(producer);
        return producer;
    }

    @Override
    public RawdataConsumer consumer(String topic, RawdataCursor cursor) {
        GoogleCloudPubSubRawdataConsumer consumer = new GoogleCloudPubSubRawdataConsumer(projectId, credentialsProvider, topic, "test", (GoogleCloudPubSubRawdataCursor) cursor, consumerPrefetchThreshold, consumerAckTimeIntervalMs);
        consumers.add(consumer);
        return consumer;
    }

    @Override
    public RawdataCursor cursorOf(String topic, ULID.Value ulid, boolean inclusive) {
        return new GoogleCloudPubSubRawdataCursor(ulid, inclusive);
    }

    @Override
    public RawdataCursor cursorOf(String topic, String position, boolean inclusive, long approxTimestamp, Duration tolerance) throws RawdataNoSuchPositionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RawdataMessage lastMessage(String topic) throws RawdataClosedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() throws Exception {
        if (closed.compareAndSet(false, true)) {
            for (GoogleCloudPubSubRawdataProducer producer : producers) {
                try {
                    producer.close();
                } catch (Throwable t) {
                    // ignore
                }
            }
            producers.clear();
            for (GoogleCloudPubSubRawdataConsumer consumer : consumers) {
                try {
                    consumer.close();
                } catch (Throwable t) {
                    // ignore
                }
            }
            consumers.clear();
        }
    }
}
