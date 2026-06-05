#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <kafka/protocol/decoder.hpp>
#include <kafka/protocol/encoder.hpp>

namespace kafka::protocol {

struct ProduceRequestPartition {
    std::int32_t index;
};

struct ProduceRequestTopic {
    std::string name;
    std::vector<ProduceRequestPartition> partitions;
};

struct ProduceRequestBody {
    std::vector<ProduceRequestTopic> topics;
};

struct ProduceResponsePartition {
    std::int32_t index;
    std::int16_t error_code;
    std::int64_t base_offset;
    std::int64_t log_append_time_ms;
    std::int64_t log_start_offset;
};

struct ProduceResponseTopic {
    std::string name;
    std::vector<ProduceResponsePartition> partitions;
};

struct ProduceResponseBody {
    std::vector<ProduceResponseTopic> topics;
    std::int32_t throttle_time_ms;
};

ProduceRequestBody read_produce_request(Decoder& decoder);
void write_produce_response(Encoder& encoder, const ProduceResponseBody& response);

} // namespace kafka::protocol
