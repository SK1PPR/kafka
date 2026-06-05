#include <kafka/protocol/apis/produce.hpp>

namespace kafka::protocol {

namespace {
    std::vector<char> read_compact_nullable_records(Decoder& decoder) {
        std::uint32_t encoded_size = decoder.read_unsigned_varint();
        if (encoded_size == 0) {
            return {};
        }

        return decoder.read_bytes(encoded_size - 1);
    }
}

ProduceRequestBody read_produce_request(Decoder& decoder) {
    ProduceRequestBody request;

    decoder.read_compact_nullable_string();
    decoder.read_int16();
    decoder.read_int32();

    request.topics = decoder.read_compact_array<ProduceRequestTopic>(
        [](Decoder& decoder) {
            ProduceRequestTopic topic;
            topic.name = decoder.read_compact_string();
            topic.partitions = decoder.read_compact_array<ProduceRequestPartition>(
                [](Decoder& decoder) {
                    ProduceRequestPartition partition;
                    partition.index = decoder.read_int32();
                    partition.records = read_compact_nullable_records(decoder);
                    decoder.read_tag_buffer();
                    return partition;
                }
            );
            decoder.read_tag_buffer();
            return topic;
        }
    );

    decoder.read_tag_buffer();
    return request;
}

void write_produce_response(Encoder& encoder, const ProduceResponseBody& response) {
    encoder.write_compact_array(response.topics, [](Encoder& encoder, const ProduceResponseTopic& topic) {
        encoder.write_compact_string(topic.name);
        encoder.write_compact_array(topic.partitions, [](Encoder& encoder, const ProduceResponsePartition& partition) {
            encoder.write_int32(partition.index);
            encoder.write_int16(partition.error_code);
            encoder.write_int64(partition.base_offset);
            encoder.write_int64(partition.log_append_time_ms);
            encoder.write_int64(partition.log_start_offset);
            encoder.write_compact_array<std::int32_t>({}, [](Encoder& encoder, std::int32_t) {
                (void)encoder;
            });
            encoder.write_unsigned_varint(0);
            encoder.write_tag_buffer();
        });
        encoder.write_tag_buffer();
    });
    encoder.write_int32(response.throttle_time_ms);
    encoder.write_tag_buffer();
}

} // namespace kafka::protocol
