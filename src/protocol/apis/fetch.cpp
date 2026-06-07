#include <kafka/protocol/apis/fetch.hpp>

#include <algorithm>

namespace kafka::protocol {

FetchRequestBody read_fetch_request(Decoder& decoder) {
    FetchRequestBody request;

    request.max_wait_ms = decoder.read_int32();
    request.min_bytes = decoder.read_int32();
    request.max_bytes = decoder.read_int32();
    decoder.read_int8();
    decoder.read_int32();
    decoder.read_int32();

    request.topics = decoder.read_compact_array<FetchRequestTopic>(
        [](Decoder& decoder) {
            FetchRequestTopic topic;
            decoder.read_bytes(16);
            topic.name = decoder.read_compact_string();
            topic.partitions = decoder.read_compact_array<FetchRequestPartition>(
                [](Decoder& decoder) {
                    FetchRequestPartition partition;
                    partition.index = decoder.read_int32();
                    decoder.read_int32();
                    partition.fetch_offset = decoder.read_int64();
                    decoder.read_int64();
                    partition.partition_max_bytes = decoder.read_int32();
                    decoder.read_tag_buffer();
                    return partition;
                }
            );
            decoder.read_tag_buffer();
            return topic;
        }
    );

    decoder.read_compact_array<std::int32_t>([](Decoder& decoder) {
        return decoder.read_int32();
    });
    decoder.read_tag_buffer();
    return request;
}

void write_fetch_response(Encoder& encoder, const FetchResponseBody& response) {
    encoder.write_int32(response.throttle_time_ms);
    encoder.write_int16(response.error_code);
    encoder.write_int32(response.session_id);
    encoder.write_compact_array(response.topics, [](Encoder& encoder, const FetchResponseTopic& topic) {
        for (int i = 0; i < 16; ++i) {
            encoder.write_int8(0);
        }
        encoder.write_compact_string(topic.name);
        encoder.write_compact_array(topic.partitions, [](Encoder& encoder, const FetchResponsePartition& partition) {
            encoder.write_int32(partition.index);
            encoder.write_int16(partition.error_code);
            encoder.write_int64(partition.high_watermark);
            encoder.write_int64(partition.log_start_offset);
            encoder.write_int64(0);
            encoder.write_compact_array<std::int32_t>({}, [](Encoder& encoder, std::int32_t) {
                (void)encoder;
            });
            encoder.write_compact_nullable_bytes(partition.records);
            encoder.write_tag_buffer();
        });
        encoder.write_tag_buffer();
    });
    encoder.write_tag_buffer();
}

} // namespace kafka::protocol
