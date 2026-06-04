#pragma once

#include <cstdint>
#include <vector>

#include <kafka/protocol/api_key.hpp>
#include <kafka/protocol/encoder.hpp>

namespace kafka::protocol {

struct ApiVersionsResponseBody {
    std::int16_t error_code;
    std::vector<ApiSpec> api_keys;
    std::int32_t throttle_time_ms;
};

void write_api_versions_response(Encoder& encoder, const ApiVersionsResponseBody& response);

}
