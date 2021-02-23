package no.ssb.rawdata.gpubsub;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class SequenceBasedBufferedReordering<ELEMENT> {

    private final Object lock = new Object();
    private final AtomicLong nextExpectedSequence = new AtomicLong();
    private final Map<Long, ELEMENT> completed = new LinkedHashMap<>();

    public SequenceBasedBufferedReordering(long firstExpectedSequence) {
        this.nextExpectedSequence.set(firstExpectedSequence);
    }

    public void addCompleted(long sequence, ELEMENT element, Consumer<List<ELEMENT>> orderedElementsCallback) {
        List<ELEMENT> orderedElements = new ArrayList<>();
        synchronized (lock) {
            if (sequence > nextExpectedSequence.get()) {
                // not the next element in sequence, buffer and return
                ELEMENT previous = completed.putIfAbsent(sequence, element);
                if (previous != null) {
                    // duplicate message received
                }
                return;
            } else if (sequence < nextExpectedSequence.get()) {
                return; // duplicate message received, ignore it
            }
            // sequence == nextExpectedSequence.get()
            orderedElements.add(element);
            ELEMENT e = completed.get(nextExpectedSequence.incrementAndGet());
            while (e != null) {
                orderedElements.add(e);
                e = completed.get(nextExpectedSequence.incrementAndGet());
            }
            if (!orderedElements.isEmpty()) {
                orderedElementsCallback.accept(orderedElements);
            }
        }
    }
}
