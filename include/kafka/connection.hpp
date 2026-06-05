#pragma once

#include <string>
#include <vector>

namespace kafka {
    class Connection {
      public:
        Connection(int client_fd, std::string log_dir);
        ~Connection();
        void handle();

      private:
          int _client_fd;
          std::string _log_dir;
          std::vector<char> read_frame();
          void send_frame(const std::vector<char>& response);
          bool read_exact(char* buffer, std::size_t size);
    };
}
