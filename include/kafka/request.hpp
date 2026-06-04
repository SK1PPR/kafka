#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <optional>

#include <kafka/protocol/api_key.hpp>

namespace kafka {

struct RequestHeader {
    int32_t message_size;
    int32_t correlation_id;
    std::optional<std::string> client_id;
    int8_t tagged_fields;
    protocol::ApiKey api_key;
    int16_t api_version;
    int16_t header_version;
};

struct Request {
    RequestHeader header;
    std::vector<char> buffer;
};

} // namespace kafka
