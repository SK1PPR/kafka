#include <kafka/connection.hpp>

#include <sys/socket.h>
#include <algorithm>
#include <cstdint>
#include <cstddef>
#include <unistd.h>
#include <vector>

#include <kafka/request_handler.hpp>

namespace kafka {

    static std::int32_t read_int32_be(const char* buffer);

    Connection::Connection(int client_fd): _client_fd(client_fd) {}
    Connection::~Connection() {
        if (_client_fd >= 0) {
            close(_client_fd);
        }
    }

    void Connection::handle() {

        while (true) {
            std::vector<char> frame = read_frame();

            if(frame.empty()) {
                break;
            }

            std::vector<char> response = RequestHandler::handle_request(frame);
            send_frame(response);
        }
    }

    std::vector<char> Connection::read_frame() {
        char buffer[4];
        if (!read_exact(buffer, 4)) {
            return {};
        }
        std::int32_t length = read_int32_be(buffer);
        if (length <= 0) {
            return {};
        }

        std::vector<char> frame(4 + length);
        std::copy(buffer, buffer + 4, frame.begin());
        if (!read_exact(frame.data() + 4, length)) {
            return {};
        }

        return frame;
    }

    void Connection::send_frame(const std::vector<char>& response) {
        std::size_t total_sent = 0;

        while (total_sent < response.size()) {
            ssize_t bytes_sent = send(_client_fd, response.data() + total_sent, response.size() - total_sent, 0);

            if (bytes_sent <= 0) {
                return;
            }

            total_sent += bytes_sent;
        }
    }

    bool Connection::read_exact(char* buffer, std::size_t size) {
        std::size_t total_read = 0;
        while (total_read < size) {
            ssize_t bytes_read = recv(_client_fd, buffer + total_read, size - total_read, 0);
            if (bytes_read <= 0) {
                return false;
            }
            total_read += bytes_read;
        }
        return true;
    }

    static std::int32_t read_int32_be(const char* buffer) {
        std::uint32_t value =
            static_cast<std::uint32_t>(static_cast<std::uint8_t>(buffer[0])) << 24 |
            static_cast<std::uint32_t>(static_cast<std::uint8_t>(buffer[1])) << 16 |
            static_cast<std::uint32_t>(static_cast<std::uint8_t>(buffer[2])) << 8 |
            static_cast<std::uint32_t>(static_cast<std::uint8_t>(buffer[3]));
        
        return static_cast<std::int32_t>(value);
    }
}
