#include <kafka/protocol/api_key.hpp>

#include <array>

namespace kafka {
    namespace protocol {
        inline constexpr std::array<ApiSpec, 2> SUPPORTED_APIS = {{
            {ApiKey::ApiVersion, 0, 4},
            {ApiKey::DescribeTopicPartition, 0, 0}
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

        std::optional<ApiSpec> spec_for(ApiKey key) {
            for (const auto& spec : SUPPORTED_APIS) {
                if (spec.key == key) {
                    return spec;
                }
            }
            return std::nullopt;
        }

        bool supports_version(ApiKey key, std::int16_t version) {
            if (auto spec = spec_for(key)) {
                return version >= spec->min_version && version <= spec->max_version;
            }
            return false;
        }

    }
}
