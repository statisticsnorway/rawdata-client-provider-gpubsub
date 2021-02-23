package no.ssb.rawdata.gpubsub;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

class GoogleCloudPubSubRawdataConsumer implements RawdataConsumer {

    static class PendingMessage {
        final RawdataMessage message;
        final String ackId;

        PendingMessage(RawdataMessage message, String ackId) {
            this.message = message;
            this.ackId = ackId;
        }
    }

    final String projectId;
    final String topic;
    final int prefetchThreshold;
    final int ackTimeIntervalMs;
    final SequenceBasedBufferedReordering<PendingMessage> bufferedReordering;
    final BlockingDeque<PendingMessage> deque = new LinkedBlockingDeque<>();
    final SubscriberStubSettings subscriberStubSettings;
    final AtomicBoolean prefetchPending = new AtomicBoolean();
    final String subscriptionName;
    final SubscriberStub subscriber;
    final String subscription;
    final Thread ackWindowTread;
    final BlockingDeque<String> outstandingAcks = new LinkedBlockingDeque<>();
    final AtomicBoolean closed = new AtomicBoolean(false);

    GoogleCloudPubSubRawdataConsumer(String projectId, CredentialsProvider credentialsProvider, String topic, String subscription, GoogleCloudPubSubRawdataCursor cursor, int prefetchThreshold, int ackTimeIntervalMs) {
        this.projectId = projectId;
        this.topic = topic;
        this.subscription = subscription;
        this.prefetchThreshold = prefetchThreshold;
        this.ackTimeIntervalMs = ackTimeIntervalMs;
        this.bufferedReordering = new SequenceBasedBufferedReordering<>(1);
        this.subscriptionName = ProjectSubscriptionName.format(projectId, subscription);
        try {
            this.subscriberStubSettings = SubscriberStubSettings.newBuilder()
                    .setCredentialsProvider(credentialsProvider)
                    .setTransportChannelProvider(
                            SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                                    .setMaxInboundMessageSize(20 << 20) // 20MB
                                    .build())
                    .build();
            this.subscriber = GrpcSubscriberStub.create(subscriberStubSettings);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ackWindowTread = new Thread(() -> {
            for (; ; ) {

                try {
                    Thread.sleep(ackTimeIntervalMs);
                } catch (InterruptedException e) {
                    // probably a close signal
                }

                if (closed.get()) {
                    return; // will kill thread
                }

                List<String> acks = new LinkedList<>();
                outstandingAcks.drainTo(acks);

                if (acks.isEmpty()) {
                    continue; // try again
                }

                // acknowledge received messages
                AcknowledgeRequest acknowledgeRequest =
                        AcknowledgeRequest.newBuilder()
                                .setSubscription(subscriptionName)
                                .addAllAckIds(acks)
                                .build();
                subscriber.acknowledgeCallable().futureCall(acknowledgeRequest); // no need to wait for ack to complete
            }
        }, "consumer-ack-" + topic + "-" + subscription);
        ackWindowTread.start();

        prefetchAsync();

        if (cursor != null) {
            waitForPrefetchToComplete();
            skipToCursor(cursor);
        }
    }

    private void skipToCursor(GoogleCloudPubSubRawdataCursor cursor) {
        for (; ; ) {
            PendingMessage pendingMessage = deque.poll();
            if (pendingMessage == null) {
                prefetchAsync();
                waitForPrefetchToComplete();
                pendingMessage = deque.poll();
                if (pendingMessage == null) {
                    // end-of-stream
                    return;
                }
            }
            ULID.Value ulid = pendingMessage.message.ulid();
            int diff = ulid.compareTo(cursor.startKey);
            if (diff < 0) {
                outstandingAcks.add(pendingMessage.ackId);
            } else if (diff == 0) {
                if (cursor.inclusive) {
                    // put message back on the queue
                    deque.addFirst(pendingMessage);
                }
                return;
            } else {
                // diff > 0
                deque.addFirst(pendingMessage);
                return;
            }
        }
    }

    private void waitForPrefetchToComplete() {
        while (prefetchPending.get()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    void prefetchAsync() {
        if (!prefetchPending.compareAndSet(false, true)) {
            return; // already pending
        }

        ApiFuture<PullResponse> pullResponseFuture;
        boolean successfullyTriggeredPrefetch = false;
        try {
            PullRequest pullRequest =
                    PullRequest.newBuilder()
                            .setMaxMessages(10)
                            .setReturnImmediately(true)
                            .setSubscription(subscriptionName)
                            .build();
            pullResponseFuture = subscriber.pullCallable().futureCall(pullRequest);
            successfullyTriggeredPrefetch = true;
        } finally {
            if (!successfullyTriggeredPrefetch) {
                prefetchPending.set(false); // reset prefetch status in case of failure
            }
        }
        pullResponseFuture.addListener(() -> {
            try {
                PullResponse pullResponse;
                try {
                    pullResponse = pullResponseFuture.get(); // should never block
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
                List<PendingMessage> reOrderedPendingMessages = new LinkedList<>();
                for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
                    // handle received message
                    RawdataMessage rawdataMessage = MessageSerializer.deserialize(message.getMessage().getData().toByteArray());
                    PendingMessage pendingMessage = new PendingMessage(rawdataMessage, message.getAckId());

                    // re-order messages
                    bufferedReordering.addCompleted(rawdataMessage.sequenceNumber(), pendingMessage,
                            orderedPendingMessages -> reOrderedPendingMessages.addAll(orderedPendingMessages));
                }
                deque.addAll(reOrderedPendingMessages); // atomic addAll to avoid receive to prematurely tripper a new prefetch before all messages are added to queue
            } finally {
                prefetchPending.set(false);
            }
        }, ForkJoinPool.commonPool());
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public RawdataMessage receive(int timeout, TimeUnit unit) throws InterruptedException, RawdataClosedException {
        if (closed.get()) {
            throw new RawdataClosedException();
        }

        PendingMessage pendingMessage = deque.poll();

        if (pendingMessage == null) {
            prefetchAsync();
            pendingMessage = deque.poll(timeout, unit);
            if (pendingMessage == null) {
                return null;
            }
        } else {
            if (deque.size() <= prefetchThreshold) {
                prefetchAsync();
            }
        }

        outstandingAcks.add(pendingMessage.ackId);

        return pendingMessage.message;
    }

    @Override
    public CompletableFuture<? extends RawdataMessage> receiveAsync() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return receive(30, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void seek(long timestamp) {
        throw new UnsupportedOperationException("seek not supported");
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() throws Exception {
        if (closed.compareAndSet(false, true)) {
            ackWindowTread.interrupt();
            ackWindowTread.join();
        }
    }
}
