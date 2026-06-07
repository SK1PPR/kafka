#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <kafka/protocol/decoder.hpp>
#include <kafka/protocol/encoder.hpp>

namespace kafka::protocol {

struct CreateTopicsRequestTopic {
    std::string name;
    std::int32_t num_partitions;
    std::int16_t replication_factor;
};

struct CreateTopicsRequestBody {
    std::vector<CreateTopicsRequestTopic> topics;
    std::int32_t timeout_ms;
};

struct CreateTopicsResponseTopic {
    std::string name;
    std::int16_t error_code;
    std::int32_t num_partitions;
    std::int16_t replication_factor;
};

struct CreateTopicsResponseBody {
    std::int32_t throttle_time_ms;
    std::vector<CreateTopicsResponseTopic> topics;
};

CreateTopicsRequestBody read_create_topics_request(Decoder& decoder);
void write_create_topics_response(Encoder& encoder, const CreateTopicsResponseBody& response);

} // namespace kafka::protocol
