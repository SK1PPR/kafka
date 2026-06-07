#include <kafka/protocol/apis/create_topics.hpp>

namespace kafka::protocol {

CreateTopicsRequestBody read_create_topics_request(Decoder& decoder) {
    CreateTopicsRequestBody request;

    request.topics = decoder.read_compact_array<CreateTopicsRequestTopic>(
        [](Decoder& decoder) {
            CreateTopicsRequestTopic topic;
            topic.name = decoder.read_compact_string();
            topic.num_partitions = decoder.read_int32();
            topic.replication_factor = decoder.read_int16();
            decoder.read_compact_array<std::int8_t>([](Decoder& decoder) {
                decoder.read_int32();
                decoder.read_compact_array<std::int32_t>([](Decoder& decoder) {
                    return decoder.read_int32();
                });
                decoder.read_tag_buffer();
                return 0;
            });
            decoder.read_compact_array<std::int8_t>([](Decoder& decoder) {
                decoder.read_compact_string();
                decoder.read_bytes(decoder.read_unsigned_varint() - 1);
                decoder.read_tag_buffer();
                return 0;
            });
            decoder.read_tag_buffer();
            return topic;
        }
    );
    request.timeout_ms = decoder.read_int32();
    decoder.read_int8();
    decoder.read_tag_buffer();

    return request;
}

void write_create_topics_response(Encoder& encoder, const CreateTopicsResponseBody& response) {
    encoder.write_int32(response.throttle_time_ms);
    encoder.write_compact_array(response.topics, [](Encoder& encoder, const CreateTopicsResponseTopic& topic) {
        encoder.write_compact_string(topic.name);
        encoder.write_int16(topic.error_code);
        encoder.write_unsigned_varint(0);
        encoder.write_int32(topic.num_partitions);
        encoder.write_int16(topic.replication_factor);
        encoder.write_compact_array<std::int8_t>({}, [](Encoder& encoder, std::int8_t) {
            (void)encoder;
        });
        encoder.write_tag_buffer();
    });
    encoder.write_tag_buffer();
}

} // namespace kafka::protocol
