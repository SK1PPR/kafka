#include <kafka/protocol/decoder.hpp>

#include <limits>
#include <stdexcept>

namespace kafka {
    namespace protocol {
        Decoder::Decoder(const char* data, std::size_t size, std::size_t position)
            : _data(data), _size(size), _position(position) {
        }

        std::int8_t Decoder::read_int8() {
            if (_size - _position < 1) {
                throw std::out_of_range("Decoder::read_uint8: out of range");
            }
            return static_cast<std::int8_t>(_data[_position++]);
        }

        std::int16_t Decoder::read_int16() {
            if (_size - _position < 2) {
                throw std::out_of_range("Decoder::read_int16: out of range");
            }
            std::uint16_t value = static_cast<std::uint16_t>(static_cast<std::uint8_t>(_data[_position++])) << 8;
            value |= static_cast<std::uint16_t>(static_cast<std::uint8_t>(_data[_position++]));
            return static_cast<std::int16_t>(value);
        }

        std::int32_t Decoder::read_int32() {
            if (_size - _position < 4) {
                throw std::out_of_range("Decoder::read_int32: out of range");
            }
            std::uint32_t value = static_cast<std::uint32_t>(static_cast<std::uint8_t>(_data[_position++])) << 24;
            value |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(_data[_position++])) << 16;
            value |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(_data[_position++])) << 8;
            value |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(_data[_position++]));
            return static_cast<std::int32_t>(value);
        }

        std::int64_t Decoder::read_int64() {
            if (_size - _position < 8) {
                throw std::out_of_range("Decoder::read_int64: out of range");
            }
            std::uint64_t value = static_cast<std::uint64_t>(static_cast<std::uint8_t>(_data[_position++])) << 56;
            value |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(_data[_position++])) << 48;
            value |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(_data[_position++])) << 40;
            value |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(_data[_position++])) << 32;
            value |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(_data[_position++])) << 24;
            value |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(_data[_position++])) << 16;
            value |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(_data[_position++])) << 8;
            value |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(_data[_position++]));
            return static_cast<std::int64_t>(value);
        }

        std::uint32_t Decoder::read_unsigned_varint() {
            std::uint32_t value = 0;
            int shift = 0;

            for (int i = 0; i < 5; ++i) {
                std::uint8_t byte = static_cast<std::uint8_t>(read_int8());
                value |= static_cast<std::uint32_t>(byte & 0x7f) << shift;

                if ((byte & 0x80) == 0) {
                    return value;
                }

                shift += 7;
            }

            throw std::runtime_error("Decoder::read_unsigned_varint: too long");
        }

        std::optional<std::string> Decoder::read_nullable_string() {
            std::int16_t length = read_int16();
            if (length < 0) {
                return std::nullopt;
            }

            auto bytes = read_bytes(static_cast<std::size_t>(length));
            return std::string(bytes.begin(), bytes.end());
        }

        std::optional<std::string> Decoder::read_compact_nullable_string() {
            std::uint32_t encoded_size = read_unsigned_varint();
            if (encoded_size == 0) {
                return std::nullopt;
            }

            auto bytes = read_bytes(static_cast<std::size_t>(encoded_size - 1));
            return std::string(bytes.begin(), bytes.end());
        }

        std::string Decoder::read_compact_string() {
            std::uint32_t encoded_size = read_unsigned_varint();
            if (encoded_size == 0) {
                throw std::runtime_error("Decoder::read_compact_string: null string");
            }

            auto length = static_cast<std::size_t>(encoded_size - 1);
            auto bytes = read_bytes(length);
            return std::string(bytes.begin(), bytes.end());
        }

        void Decoder::read_tag_buffer() {
            std::uint32_t tagged_field_count = read_unsigned_varint();

            for (std::uint32_t i = 0; i < tagged_field_count; ++i) {
                read_unsigned_varint();
                std::uint32_t size = read_unsigned_varint();
                read_bytes(size);
            }
        }

        std::vector<char> Decoder::read_bytes(std::size_t len) {
            if (_size - _position < len) {
                throw std::out_of_range("Decoder::read_bytes: out of range");
            }
            std::vector<char> result(len);
            std::copy(_data + _position, _data + _position + len, result.begin());
            _position += len;
            return result;
        }

        std::vector<char> Decoder::read_body() {
            return read_bytes(_size - _position);
        }

        std::size_t Decoder::position() const {
            return _position;
        }
    }
}
