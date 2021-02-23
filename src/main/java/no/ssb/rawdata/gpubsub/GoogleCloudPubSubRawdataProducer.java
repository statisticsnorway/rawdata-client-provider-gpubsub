package no.ssb.rawdata.gpubsub;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataNotBufferedException;
import no.ssb.rawdata.api.RawdataProducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class GoogleCloudPubSubRawdataProducer implements RawdataProducer {

    final String projectId;
    final CredentialsProvider credentialsProvider;
    final String topic;
    final Publisher publisher;
    final AtomicBoolean closed = new AtomicBoolean(false);
    final Map<String, GoogleCloudPubSubRawdataMessage.Builder> buffer = new ConcurrentHashMap<>();

    GoogleCloudPubSubRawdataProducer(String projectId, CredentialsProvider credentialsProvider, String topic) {
        this.projectId = projectId;
        this.credentialsProvider = credentialsProvider;
        this.topic = topic;
        ProjectTopicName topicName = ProjectTopicName.of(projectId, topic);
        try {
            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topicName).setCredentialsProvider(credentialsProvider).build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public GoogleCloudPubSubRawdataMessage.Builder builder() throws RawdataClosedException {
        return new GoogleCloudPubSubRawdataMessage.Builder();
    }

    @Override
    public GoogleCloudPubSubRawdataProducer buffer(RawdataMessage.Builder _builder) throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        GoogleCloudPubSubRawdataMessage.Builder builder = (GoogleCloudPubSubRawdataMessage.Builder) _builder;
        buffer.put(builder.position, builder);
        return this;
    }

    @Override
    public void publish(String... positions) throws RawdataClosedException, RawdataNotBufferedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }

        List<ApiFuture<String>> futures = new ArrayList<>();

        for (String position : positions) {
            GoogleCloudPubSubRawdataMessage.Builder builder = buffer.get(position);
            if (builder == null) {
                throw new RawdataNotBufferedException();
            }

            GoogleCloudPubSubRawdataMessage rawdataMessage = builder.build();
            byte[] data = MessageSerializer.serialize(rawdataMessage);

            PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                    .setData(ByteString.copyFrom(data))
                    .build();

            ApiFuture<String> future = publisher.publish(pubsubMessage);
            futures.add(future);
        }

        try {
            ApiFutures.allAsList(futures).get(5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Void> publishAsync(String... positions) {
        if (isClosed()) {
            throw new RawdataClosedException();
        }

        List<ApiFuture<String>> futures = new ArrayList<>();

        for (String position : positions) {
            GoogleCloudPubSubRawdataMessage.Builder builder = buffer.get(position);
            if (builder == null) {
                throw new RawdataNotBufferedException();
            }

            GoogleCloudPubSubRawdataMessage rawdataMessage = builder.build();
            byte[] data = MessageSerializer.serialize(rawdataMessage);

            PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                    .setData(ByteString.copyFrom(data))
                    .build();

            ApiFuture<String> future = publisher.publish(pubsubMessage);
            futures.add(future);
        }

        ApiFuture<List<String>> listApiFuture = ApiFutures.allAsList(futures);

        return CompletableFuture.runAsync(() -> {
            try {
                listApiFuture.get(5, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (TimeoutException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() throws Exception {
        if (closed.compareAndSet(false, true)) {
            publisher.shutdown();
        }
    }
}
