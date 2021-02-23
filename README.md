# rawdata-client-provider-gpubsub
Google Cloud Pub/Sub provider for Rawdata

### Prepare split-package workaround
```shell script
mvn -Pdeps clean dependency:copy-dependencies
mkdir -p target/deps
mv target/dependency/grpc-google-cloud-pubsub-v1-1.82.0.jar target/deps
mv target/dependency/proto-google-common-protos-1.17.0.jar target/deps
mv target/dependency/grpc-context-1.22.1.jar target/deps
```

# module patch switches must be added 
```
--patch-module proto.google.cloud.pubsub.v1=$PWD/target/deps/grpc-google-cloud-pubsub-v1-1.82.0.jar --patch-module gax.grpc=$PWD/target/deps/proto-google-common-protos-1.17.0.jar --patch-module grpc-api=$PWD/target/deps/grpc-context-1.22.1.jar
```

# Compilation example using the --patch-module switch
```
find src/main/java -name '*.java' | xargs javac -p target/dependency -d target/classes --patch-module proto.google.cloud.pubsub.v1=$PWD/target/deps/grpc-google-cloud-pubsub-v1-1.82.0.jar --patch-module gax.grpc=$PWD/target/deps/proto-google-common-protos-1.17.0.jar --patch-module grpc-api=$PWD/target/deps/grpc-context-1.22.1.jar
```
