#pragma once

#include <cstdint>

namespace kafka {
    namespace protocol {
        namespace error {
            constexpr std::int16_t None = 0;
            constexpr std::int16_t UnknownTopicOrPartition = 3;
            constexpr std::int16_t UnsupportedError = 35;
        }
    }
}