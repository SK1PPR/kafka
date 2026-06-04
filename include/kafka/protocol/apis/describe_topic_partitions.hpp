#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <vector>

#include <kafka/protocol/decoder.hpp>
#include <kafka/protocol/encoder.hpp>

namespace kafka::protocol {

struct DescribeTopicPartitionsRequestTopic {
    std::string name;
};

struct DescribeTopicPartitionsRequestBody {
    std::vector<DescribeTopicPartitionsRequestTopic> topics;
    std::int32_t response_partition_limit;
    std::int8_t cursor;
};

struct DescribeTopicPartitionsResponsePartition {
    std::int16_t error_code;
    std::int32_t partition_index;
    std::int32_t leader_id;
    std::int32_t leader_epoch;
    std::vector<std::int32_t> replica_nodes;
    std::vector<std::int32_t> isr_nodes;
    std::vector<std::int32_t> eligible_leader_replicas;
    std::vector<std::int32_t> last_known_elr;
    std::vector<std::int32_t> offline_replicas;
};

struct DescribeTopicPartitionsResponseTopic {
    std::int16_t error_code;
    std::string name;
    std::array<std::uint8_t, 16> topic_id{};
    bool is_internal;
    std::vector<DescribeTopicPartitionsResponsePartition> partitions;
    std::int32_t topic_authorized_operations;
};

struct DescribeTopicPartitionsResponseBody {
    std::int32_t throttle_time_ms;
    std::vector<DescribeTopicPartitionsResponseTopic> topics;
    std::int8_t next_cursor;
};

DescribeTopicPartitionsRequestBody read_describe_topic_partitions_request(Decoder& decoder);
void write_describe_topic_partitions_response(
    Encoder& encoder,
    const DescribeTopicPartitionsResponseBody& response
);

}
