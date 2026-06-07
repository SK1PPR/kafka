#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <kafka/protocol/decoder.hpp>
#include <kafka/protocol/encoder.hpp>

namespace kafka::protocol {

struct FetchRequestPartition {
    std::int32_t index;
    std::int64_t fetch_offset;
    std::int32_t partition_max_bytes;
};

struct FetchRequestTopic {
    std::string name;
    std::vector<FetchRequestPartition> partitions;
};

struct FetchRequestBody {
    std::int32_t max_wait_ms;
    std::int32_t min_bytes;
    std::int32_t max_bytes;
    std::vector<FetchRequestTopic> topics;
};

struct FetchResponsePartition {
    std::int32_t index;
    std::int16_t error_code;
    std::int64_t high_watermark;
    std::int64_t log_start_offset;
    std::vector<char> records;
};

struct FetchResponseTopic {
    std::string name;
    std::vector<FetchResponsePartition> partitions;
};

struct FetchResponseBody {
    std::int32_t throttle_time_ms;
    std::int16_t error_code;
    std::int32_t session_id;
    std::vector<FetchResponseTopic> topics;
};

FetchRequestBody read_fetch_request(Decoder& decoder);
void write_fetch_response(Encoder& encoder, const FetchResponseBody& response);

} // namespace kafka::protocol
