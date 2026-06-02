#pragma once

#include <cstdint>
#include <optional>
#include <span>

namespace kafka {
    namespace protocol {
        enum class ApiKey : std::int16_t {
          ApiVersion = 18,
          DescribeTopicPartition = 75
        };

        struct ApiSpec {
            ApiKey key;
            std::int16_t min_version;
            std::int16_t max_version;
        };

        std::span<const ApiSpec> supported_apis();
        std::optional<ApiKey> api_key_from_int(std::int16_t key);
        std::optional<ApiSpec> spec_for(ApiKey key);
        bool supports_version(ApiKey key, std::int16_t version);
        
    }
}