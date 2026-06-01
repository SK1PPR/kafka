#pragma once

namespace kafka {
    class Server {
        public:
            explicit Server(int port);
            ~Server();
            int run();

            // Making server uncopyable
            Server(const Server&) = delete;
            Server& operator=(const Server&) = delete;

        private:
            int _port;
            int _server_fd;

            int setup_socket();
            int accept_connection();
    };
}
