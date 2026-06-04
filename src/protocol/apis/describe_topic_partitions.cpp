#include <kafka/protocol/apis/describe_topic_partitions.hpp>

namespace kafka::protocol {

DescribeTopicPartitionsRequestBody read_describe_topic_partitions_request(Decoder& decoder) {
    DescribeTopicPartitionsRequestBody request;

    request.topics = decoder.read_compact_array<DescribeTopicPartitionsRequestTopic>(
        [](Decoder& decoder) {
            DescribeTopicPartitionsRequestTopic topic;
            topic.name = decoder.read_compact_string();
            decoder.read_tag_buffer();
            return topic;
        }
    );
    request.response_partition_limit = decoder.read_int32();
    request.cursor = decoder.read_int8();
    decoder.read_tag_buffer();

    return request;
}

void write_describe_topic_partitions_response(
    Encoder& encoder,
    const DescribeTopicPartitionsResponseBody& response
) {
    encoder.write_int32(response.throttle_time_ms);
    encoder.write_compact_array(response.topics, [](Encoder& encoder, const DescribeTopicPartitionsResponseTopic& topic) {
        encoder.write_int16(topic.error_code);
        encoder.write_compact_string(topic.name);
        for (std::uint8_t byte : topic.topic_id) {
            encoder.write_int8(static_cast<std::int8_t>(byte));
        }
        encoder.write_int8(topic.is_internal ? 1 : 0);
        encoder.write_compact_array(topic.partitions, [](Encoder& encoder, const DescribeTopicPartitionsResponsePartition& partition) {
            encoder.write_int16(partition.error_code);
            encoder.write_int32(partition.partition_index);
            encoder.write_int32(partition.leader_id);
            encoder.write_int32(partition.leader_epoch);
            encoder.write_compact_array(partition.replica_nodes, [](Encoder& encoder, std::int32_t node) {
                encoder.write_int32(node);
            });
            encoder.write_compact_array(partition.isr_nodes, [](Encoder& encoder, std::int32_t node) {
                encoder.write_int32(node);
            });
            encoder.write_compact_array(partition.eligible_leader_replicas, [](Encoder& encoder, std::int32_t node) {
                encoder.write_int32(node);
            });
            encoder.write_compact_array(partition.last_known_elr, [](Encoder& encoder, std::int32_t node) {
                encoder.write_int32(node);
            });
            encoder.write_compact_array(partition.offline_replicas, [](Encoder& encoder, std::int32_t node) {
                encoder.write_int32(node);
            });
            encoder.write_tag_buffer();
        });
        encoder.write_int32(topic.topic_authorized_operations);
        encoder.write_tag_buffer();
    });
    encoder.write_int8(response.next_cursor);
    encoder.write_tag_buffer();
}

}
