import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.gpubsub.GoogleCloudPubSubRawdataClientInitializer;

module no.ssb.rawdata.gpubsub {
    requires no.ssb.rawdata.api;
    requires no.ssb.service.provider.api;
    requires org.slf4j;
    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.databind;
    requires jackson.dataformat.msgpack;

    requires google.cloud.pubsub;
    requires google.cloud.core;
    requires com.google.auth.oauth2;
    requires com.google.auth;
    requires com.google.api.apicommon;
    requires com.google.protobuf;
    requires proto.google.cloud.pubsub.v1;
    requires gax;

    provides RawdataClientInitializer with GoogleCloudPubSubRawdataClientInitializer;
}