#include <kafka/protocol/decoder.hpp>

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
    }
}
