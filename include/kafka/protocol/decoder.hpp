#pragma once

#include <cstdint>
#include <cstddef>
#include <vector>

namespace kafka {
    namespace protocol {
    class Decoder {
        public:
            Decoder(const char* data, std::size_t size, std::size_t position = 0);

            std::int8_t read_int8();
            std::int16_t read_int16();
            std::int32_t read_int32();
            std::int64_t read_int64();

            std::vector<char> read_bytes(std::size_t size);

            std::size_t position() const;
        private:
            const char* _data;
            std::size_t _size;
            std::size_t _position;
    };
    }
}
