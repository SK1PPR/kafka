#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <kafka/protocol/decoder.hpp>
#include <kafka/protocol/encoder.hpp>

namespace kafka::protocol {

struct ListOffsetsRequestPartition {
    std::int32_t index;
    std::int64_t timestamp;
};

struct ListOffsetsRequestTopic {
    std::string name;
    std::vector<ListOffsetsRequestPartition> partitions;
};

struct ListOffsetsRequestBody {
    std::vector<ListOffsetsRequestTopic> topics;
};

struct ListOffsetsResponsePartition {
    std::int32_t index;
    std::int16_t error_code;
    std::int64_t timestamp;
    std::int64_t offset;
};

struct ListOffsetsResponseTopic {
    std::string name;
    std::vector<ListOffsetsResponsePartition> partitions;
};

struct ListOffsetsResponseBody {
    std::int32_t throttle_time_ms;
    std::vector<ListOffsetsResponseTopic> topics;
};

ListOffsetsRequestBody read_list_offsets_request(Decoder& decoder);
void write_list_offsets_response(Encoder& encoder, const ListOffsetsResponseBody& response);

} // namespace kafka::protocol
