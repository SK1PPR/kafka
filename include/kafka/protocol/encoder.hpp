#pragma once

#include <cstdint>
#include <vector>

namespace kafka {
    namespace protocol {
    class Encoder {
        public:
            void write_int8(std::int8_t value);
            void write_int16(std::int16_t value);
            void write_int32(std::int32_t value);
            void write_int64(std::int64_t value);
            void write_message_size();

            void write_bytes(const std::vector<char>& bytes);

            const std::vector<char>& buffer() const;

        private:
            std::vector<char> _buffer;
    };
    }
}
