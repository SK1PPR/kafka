#pragma once

#include <cstdint>
#include <vector>

#include <kafka/protocol/apis/api_versions.hpp>
#include <kafka/protocol/apis/describe_topic_partitions.hpp>

namespace kafka {

struct Response {
    enum class Type {
        ApiVersions,
        DescribeTopicPartition,
        Error
    };

    Type type;
    std::int32_t correlation_id;
    std::int16_t error_code;
    protocol::ApiVersionsResponseBody api_versions;
    protocol::DescribeTopicPartitionsResponseBody describe_topic_partition;
};

} // namespace kafka
