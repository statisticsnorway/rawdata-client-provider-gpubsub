package no.ssb.rawdata.gpubsub;

import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class SequenceBasedBufferedReorderingTest {

    @Test
    public void testAddCompleted() {
        SequenceBasedBufferedReordering<String> reordering = new SequenceBasedBufferedReordering<>(1);
        List<String> callback = new LinkedList<>();
        reordering.addCompleted(1, "a", list -> callback.addAll(list));
        assertEquals(List.of("a"), callback);
        callback.clear();
        reordering.addCompleted(3, "c", list -> callback.addAll(list));
        assertEquals(Collections.emptyList(), callback);
        reordering.addCompleted(4, "d", list -> callback.addAll(list));
        reordering.addCompleted(4, "d", list -> {
            throw new RuntimeException("duplicate introduced in re-ordered stream");
        });
        reordering.addCompleted(1, "a", list -> {
            throw new RuntimeException("duplicate introduced in re-ordered stream");
        });
        assertEquals(Collections.emptyList(), callback);
        reordering.addCompleted(2, "b", list -> callback.addAll(list));
        assertEquals(List.of("b", "c", "d"), callback);
        callback.clear();
        reordering.addCompleted(7, "g", list -> callback.addAll(list));
        assertEquals(Collections.emptyList(), callback);
        reordering.addCompleted(5, "e", list -> callback.addAll(list));
        assertEquals(List.of("e"), callback);
        callback.clear();
        reordering.addCompleted(6, "f", list -> callback.addAll(list));
        assertEquals(List.of("f", "g"), callback);
        callback.clear();
        reordering.addCompleted(7, "g", list -> {
            throw new RuntimeException("duplicate introduced in re-ordered stream");
        });
    }
}