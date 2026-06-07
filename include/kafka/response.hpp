#pragma once

#include <cstdint>
#include <vector>

#include <kafka/protocol/apis/api_versions.hpp>
#include <kafka/protocol/apis/create_topics.hpp>
#include <kafka/protocol/apis/describe_topic_partitions.hpp>
#include <kafka/protocol/apis/fetch.hpp>
#include <kafka/protocol/apis/list_offsets.hpp>
#include <kafka/protocol/apis/produce.hpp>

namespace kafka {

struct Response {
    enum class Type {
        ApiVersions,
        DescribeTopicPartition,
        Produce,
        Fetch,
        ListOffsets,
        CreateTopics,
        Error
    };

    Type type = Type::Error;
    std::int32_t correlation_id = 0;
    std::int16_t error_code = 0;
    protocol::ApiVersionsResponseBody api_versions{};
    protocol::DescribeTopicPartitionsResponseBody describe_topic_partition{};
    protocol::ProduceResponseBody produce{};
    protocol::FetchResponseBody fetch{};
    protocol::ListOffsetsResponseBody list_offsets{};
    protocol::CreateTopicsResponseBody create_topics{};
};

} // namespace kafka
