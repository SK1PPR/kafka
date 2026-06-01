#pragma once

#include <vector>
#include <cstdint>

namespace kafka {

struct Request {
    int32_t message_size;
    int16_t request_api_key;
    int16_t request_api_version;
    int32_t correlation_id;
    std::vector<char> buffer;
};

} // namespace kafka
