package no.ssb.rawdata.gpubsub;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataMessage;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

class MessageSerializer {

    static final ObjectMapper mapper = new ObjectMapper(new MessagePackFactory());

    static byte[] serialize(RawdataMessage rawdataMessage) {
        ObjectNode root = mapper.createObjectNode();
        root.put("id", rawdataMessage.ulid().toString());
        root.put("grp", rawdataMessage.orderingGroup());
        root.put("seq", rawdataMessage.sequenceNumber());
        root.put("pos", rawdataMessage.position());
        ObjectNode data = root.putObject("data");
        for (Map.Entry<String, byte[]> e : rawdataMessage.data().entrySet()) {
            data.put(e.getKey(), e.getValue());
        }
        try {
            return mapper.writeValueAsBytes(root);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    static RawdataMessage deserialize(byte[] binary) {
        ObjectNode root;
        try {
            root = (ObjectNode) mapper.readTree(binary);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        GoogleCloudPubSubRawdataMessage.Builder builder = new GoogleCloudPubSubRawdataMessage.Builder();
        builder.ulid(ULID.parseULID(root.get("id").textValue()));
        builder.orderingGroup(root.get("grp").textValue());
        builder.sequenceNumber(root.get("seq").longValue());
        builder.position(root.get("pos").textValue());
        ObjectNode data = (ObjectNode) root.get("data");
        Iterator<Map.Entry<String, JsonNode>> fields = data.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> e = fields.next();
            try {
                builder.put(e.getKey(), e.getValue().binaryValue());
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        return builder.build();
    }
}
