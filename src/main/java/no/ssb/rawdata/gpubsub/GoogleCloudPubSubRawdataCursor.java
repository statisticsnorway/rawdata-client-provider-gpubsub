package no.ssb.rawdata.gpubsub;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataCursor;

import java.util.Objects;

public class GoogleCloudPubSubRawdataCursor implements RawdataCursor {

    /**
     * Need not exactly match an existing ulid-value.
     */
    final ULID.Value startKey;

    /**
     * Whether or not to include the element with ulid-value matching the lower-bound exactly.
     */
    final boolean inclusive;

    GoogleCloudPubSubRawdataCursor(ULID.Value startKey, boolean inclusive) {
        this.startKey = startKey;
        this.inclusive = inclusive;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GoogleCloudPubSubRawdataCursor that = (GoogleCloudPubSubRawdataCursor) o;
        return inclusive == that.inclusive &&
                startKey.equals(that.startKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startKey, inclusive);
    }

    @Override
    public String toString() {
        return "GoogleCloudPubSubRawdataCursor{" +
                "startKey=" + startKey +
                ", inclusive=" + inclusive +
                '}';
    }
}
