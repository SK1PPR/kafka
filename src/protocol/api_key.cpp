#include <kafka/protocol/api_key.hpp>

#include <array>

namespace kafka {
    namespace protocol {
        inline constexpr std::array<ApiSpec, 6> SUPPORTED_APIS = {{
            {ApiKey::ApiVersion, 0, 4, 0, 0},
            {ApiKey::DescribeTopicPartition, 0, 0, 1, 1},
            {ApiKey::Produce, 0, 11, 1, 1},
            {ApiKey::Fetch, 11, 16, 1, 1},
            {ApiKey::ListOffsets, 7, 10, 1, 1},
            {ApiKey::CreateTopics, 5, 7, 1, 1}
        }};

        std::span<const ApiSpec> supported_apis() {
            return std::span<const ApiSpec>(SUPPORTED_APIS);
        }

        std::optional<ApiKey> api_key_from_int(std::int16_t key) {
            for (const auto& spec : SUPPORTED_APIS) {
                if (static_cast<std::int16_t>(spec.key) == key) {
                    return spec.key;
                }
            }
            return std::nullopt;
        }

        std::optional<ApiSpec> spec_for(ApiKey key, std::int16_t version) {
            for (const auto& spec : SUPPORTED_APIS) {
                if (spec.key == key && (version >= spec.min_version && version <= spec.max_version)) {
                    return spec;
                }
            }
            return std::nullopt;
        }

        bool supports_version(ApiKey key, std::int16_t version) {
            return spec_for(key, version).has_value();
        }

        int16_t request_header_version(ApiKey key, std::int16_t version) {
            return spec_for(key, version).value().request_header_version;
        }
    }
}
