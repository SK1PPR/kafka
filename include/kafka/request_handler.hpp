#pragma once

#include <kafka/request.hpp>
#include <kafka/response.hpp>
#include <kafka/protocol/decoder.hpp>
#include <vector>

namespace kafka {
    class RequestHandler {
        public:
            static std::vector<char> handle_request(const std::vector<char>& request);

        private:
            static RequestHeader decode_request_header(protocol::Decoder& decoder);
            static Request decode_request(const std::vector<char>& input_buffer);
            static Response handle_api_versions(const Request& request);
            static Response handle_error(const std::int32_t correlation_id, const std::int16_t error_code);
            static Response handle_describe_topic_partition(const Request& request);
            static Response handle_produce(const Request& request);
            static std::vector<char> encode_response(const Response& response);
    };
}
