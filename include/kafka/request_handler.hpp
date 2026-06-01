#pragma once

#include <vector>

namespace kafka {
    class RequestHandler {
        public:
            static std::vector<char> handle_request(const std::vector<char>& request);
    };
}