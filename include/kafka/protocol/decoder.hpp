#pragma once

#include <cstdint>
#include <cstddef>
#include <optional>
#include <stdexcept>
#include <string>
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
            std::uint32_t read_unsigned_varint();
            std::optional<std::string> read_nullable_string();
            std::optional<std::string> read_compact_nullable_string();
            std::string read_compact_string();
            void read_tag_buffer();

            std::vector<char> read_bytes(std::size_t size);
            std::vector<char> read_body();

            template <typename T, typename ReadItem>
            std::vector<T> read_compact_array(ReadItem read_item) {
                std::uint32_t encoded_size = read_unsigned_varint();
                if (encoded_size == 0) {
                    throw std::runtime_error("Decoder::read_compact_array: null array");
                }

                std::vector<T> items;
                items.reserve(encoded_size - 1);

                for (std::uint32_t i = 0; i < encoded_size - 1; ++i) {
                    items.push_back(read_item(*this));
                }

                return items;
            }

            std::size_t position() const;
        private:
            const char* _data;
            std::size_t _size;
            std::size_t _position;
    };
    }
}
