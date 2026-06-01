#pragma once

#include <vector>

namespace kafka {
    class Connection {
      public:
        explicit Connection(int client_fd);
        ~Connection();
        void handle();

      private:
          int _client_fd;
          std::vector<char> read_frame();
          void send_frame(const std::vector<char>& response);
          bool read_exact(char* buffer, std::size_t size);
    };
}