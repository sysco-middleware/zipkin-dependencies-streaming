package no.sysco.middleware.zipkin.dependencies.streaming.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import zipkin2.DependencyLink;
import zipkin2.codec.DependencyLinkBytesDecoder;
import zipkin2.codec.DependencyLinkBytesEncoder;

import java.util.List;
import java.util.Map;

public class DependencyLinksSerde implements Serde<List<DependencyLink>> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //Nothing to configure
    }

    @Override
    public void close() {
        //No resources to close
    }

    @Override
    public Serializer<List<DependencyLink>> serializer() {
        return new DependencyLinksSerializer();
    }

    @Override
    public Deserializer<List<DependencyLink>> deserializer() {
        return new DependencyLinksDeserializer();
    }

    public static class DependencyLinksDeserializer implements Deserializer<List<DependencyLink>> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            //Nothing to configure
        }

        @Override
        public List<DependencyLink> deserialize(String topic, byte[] data) {
            return DependencyLinkBytesDecoder.JSON_V1.decodeList(data);
        }

        @Override
        public void close() {
            //No resources to close
        }
    }

    public static class DependencyLinksSerializer implements Serializer<List<DependencyLink>> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            //Nothing to configure
        }

        @Override
        public byte[] serialize(String topic, List<DependencyLink> data) {
            return DependencyLinkBytesEncoder.JSON_V1.encodeList(data);
        }

        @Override
        public void close() {
            //No resources to close
        }
    }
}
