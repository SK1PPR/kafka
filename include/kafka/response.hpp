#pragma once

#include <cstdint>
#include <vector>

#include <kafka/protocol/api_key.hpp>

namespace kafka {

struct ApiVersionsResponseBody {
    std::int16_t error_code;
    std::vector<protocol::ApiSpec> api_keys;
    std::int32_t throttle_time_ms;
};

struct Response {
    enum class Type {
        ApiVersions,
        Error
    };

    Type type;
    std::int32_t correlation_id;
    std::int16_t error_code;
    ApiVersionsResponseBody api_versions;
};

} // namespace kafka
