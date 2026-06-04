#pragma once

#include <cstdint>
#include <string>
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
            void write_unsigned_varint(std::uint32_t value);
            void write_compact_string(const std::string& value);
            void write_tag_buffer();

            void write_bytes(const std::vector<char>& bytes);

            template <typename T, typename WriteItem>
            void write_compact_array(const std::vector<T>& items, WriteItem write_item) {
                write_unsigned_varint(static_cast<std::uint32_t>(items.size() + 1));

                for (const auto& item : items) {
                    write_item(*this, item);
                }
            }

            const std::vector<char>& buffer() const;

        private:
            std::vector<char> _buffer;
    };
    }
}
