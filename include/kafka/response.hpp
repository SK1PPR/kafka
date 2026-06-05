#pragma once

#include <cstdint>
#include <vector>

#include <kafka/protocol/apis/api_versions.hpp>
#include <kafka/protocol/apis/describe_topic_partitions.hpp>
#include <kafka/protocol/apis/produce.hpp>

namespace kafka {

struct Response {
    enum class Type {
        ApiVersions,
        DescribeTopicPartition,
        Produce,
        Error
    };

    Type type;
    std::int32_t correlation_id;
    std::int16_t error_code;
    protocol::ApiVersionsResponseBody api_versions;
    protocol::DescribeTopicPartitionsResponseBody describe_topic_partition;
    protocol::ProduceResponseBody produce;
};

} // namespace kafka
