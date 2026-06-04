#pragma once

#include <exception>
#include <cstdint>

namespace kafka {
    class KafkaRequestError : public std::exception {

        public:
            KafkaRequestError(std::int32_t correlation_id, std::int16_t error_code)
                : _correlation_id(correlation_id), _error_code(error_code) {}

            std::int32_t correlation_id() const {
                return _correlation_id;
            }

            std::int16_t error_code() const {
                return _error_code;
            }

            const char* what() const noexcept override {
                return "Kafka Request Error";
            }

        private:
            std::int32_t _correlation_id;
            std::int16_t _error_code;
    };
}
