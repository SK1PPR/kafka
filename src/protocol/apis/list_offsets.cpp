#include <kafka/protocol/apis/list_offsets.hpp>

namespace kafka::protocol {

ListOffsetsRequestBody read_list_offsets_request(Decoder& decoder) {
    ListOffsetsRequestBody request;

    decoder.read_int32();
    decoder.read_int8();
    request.topics = decoder.read_compact_array<ListOffsetsRequestTopic>(
        [](Decoder& decoder) {
            ListOffsetsRequestTopic topic;
            topic.name = decoder.read_compact_string();
            topic.partitions = decoder.read_compact_array<ListOffsetsRequestPartition>(
                [](Decoder& decoder) {
                    ListOffsetsRequestPartition partition;
                    partition.index = decoder.read_int32();
                    partition.timestamp = decoder.read_int64();
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

void write_list_offsets_response(Encoder& encoder, const ListOffsetsResponseBody& response) {
    encoder.write_int32(response.throttle_time_ms);
    encoder.write_compact_array(response.topics, [](Encoder& encoder, const ListOffsetsResponseTopic& topic) {
        encoder.write_compact_string(topic.name);
        encoder.write_compact_array(topic.partitions, [](Encoder& encoder, const ListOffsetsResponsePartition& partition) {
            encoder.write_int32(partition.index);
            encoder.write_int16(partition.error_code);
            encoder.write_int64(partition.timestamp);
            encoder.write_int64(partition.offset);
            encoder.write_tag_buffer();
        });
        encoder.write_tag_buffer();
    });
    encoder.write_tag_buffer();
}

} // namespace kafka::protocol
