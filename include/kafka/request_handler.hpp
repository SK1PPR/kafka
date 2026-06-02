#pragma once

#include "kafka/request.hpp"
#include "kafka/response.hpp"
#include <vector>

namespace kafka {
    class RequestHandler {
        public:
            static std::vector<char> handle_request(const std::vector<char>& request);

        private:
            static Request decode_request(const std::vector<char>& input_buffer);
            static Response handle_api_versions(const Request& request);
            static Response handle_unsupported(const Request& request);
            static Response handle_unsupported_version(const Request& request);
            static std::vector<char> encode_response(const Response& response);
    };
}
