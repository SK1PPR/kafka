#include <kafka/protocol/apis/api_versions.hpp>

namespace kafka::protocol {

void write_api_versions_response(Encoder& encoder, const ApiVersionsResponseBody& response) {
    encoder.write_int16(response.error_code);
    encoder.write_compact_array(response.api_keys, [](Encoder& encoder, const ApiSpec& api) {
        encoder.write_int16(static_cast<std::int16_t>(api.key));
        encoder.write_int16(api.min_version);
        encoder.write_int16(api.max_version);
        encoder.write_tag_buffer();
    });
    encoder.write_int32(response.throttle_time_ms);
    encoder.write_tag_buffer();
}

}
