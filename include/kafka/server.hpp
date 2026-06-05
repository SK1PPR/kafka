#pragma once

#include <string>

namespace kafka {
    class Server {
        public:
            explicit Server(int port, std::string log_dir = "/tmp/kraft-combined-logs");
            ~Server();
            int run();

            // Making server uncopyable
            Server(const Server&) = delete;
            Server& operator=(const Server&) = delete;

        private:
            int _port;
            int _server_fd;
            std::string _log_dir;

            int setup_socket();
            int accept_connection();
    };
}
