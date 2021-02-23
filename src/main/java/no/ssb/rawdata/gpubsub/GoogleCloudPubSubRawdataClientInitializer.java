package no.ssb.rawdata.gpubsub;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.service.provider.api.ProviderName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

@ProviderName("gpubsub")
public class GoogleCloudPubSubRawdataClientInitializer implements RawdataClientInitializer {

    @Override
    public String providerId() {
        return "gpubsub";
    }

    @Override
    public Set<String> configurationKeys() {
        return Set.of();
    }

    @Override
    public RawdataClient initialize(Map<String, String> configuration) {
        GoogleCredentials credentials = loadCredentials(Path.of("secret/direct-landing-258019-9345e4b83456.json"));
        return new GoogleCloudPubSubRawdataClient("direct-landing-258019", () -> credentials, 5, 3000);
    }

    GoogleCredentials loadCredentials(Path serviceAccountKeyPath) {
        ServiceAccountCredentials sourceCredentials;
        try {
            sourceCredentials = ServiceAccountCredentials.fromStream(Files.newInputStream(serviceAccountKeyPath, StandardOpenOption.READ));
        } catch (
                IOException e) {
            throw new RuntimeException(e);
        }
        GoogleCredentials scopedCredentials = sourceCredentials.createScoped(Arrays.asList("https://www.googleapis.com/auth/pubsub"));
        return scopedCredentials;
    }
}
