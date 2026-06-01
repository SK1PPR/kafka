#include <kafka/protocol/encoder.hpp>

namespace kafka {
    namespace protocol {
        void Encoder::write_int8(std::int8_t value) {
            auto raw = static_cast<std::uint8_t>(value);

            _buffer.push_back(static_cast<char>(raw));
        }

        void Encoder::write_int16(std::int16_t value) {
            auto raw = static_cast<std::uint16_t>(value);

            _buffer.push_back(static_cast<char>((raw >> 8) & 0xff));
            _buffer.push_back(static_cast<char>(raw & 0xff));
        }

        void Encoder::write_int32(std::int32_t value) {
            auto raw = static_cast<std::uint32_t>(value);

            _buffer.push_back(static_cast<char>((raw >> 24) & 0xff));
            _buffer.push_back(static_cast<char>((raw >> 16) & 0xff));
            _buffer.push_back(static_cast<char>((raw >> 8) & 0xff));
            _buffer.push_back(static_cast<char>(raw & 0xff));
        }

        void Encoder::write_int64(std::int64_t value) {
            auto raw = static_cast<std::uint64_t>(value);

            _buffer.push_back(static_cast<char>((raw >> 56) & 0xff));
            _buffer.push_back(static_cast<char>((raw >> 48) & 0xff));
            _buffer.push_back(static_cast<char>((raw >> 40) & 0xff));
            _buffer.push_back(static_cast<char>((raw >> 32) & 0xff));
            _buffer.push_back(static_cast<char>((raw >> 24) & 0xff));
            _buffer.push_back(static_cast<char>((raw >> 16) & 0xff));
            _buffer.push_back(static_cast<char>((raw >> 8) & 0xff));
            _buffer.push_back(static_cast<char>(raw & 0xff));
        }

        void Encoder::write_bytes(const std::vector<char>& bytes) {
            _buffer.insert(_buffer.end(), bytes.begin(), bytes.end());
        }

        void Encoder::write_message_size() {
            auto _old_buffer = _buffer;
            _buffer.clear();
            write_int32(_old_buffer.size());
            _buffer.insert(_buffer.end(), _old_buffer.begin(), _old_buffer.end());
        }

        const std::vector<char>& Encoder::buffer() const {
            return _buffer;
        }
    }
}
