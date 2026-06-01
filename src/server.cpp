#include <kafka/server.hpp>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <iostream>
#include <thread>

#include <kafka/connection.hpp>

namespace kafka {
    Server::Server(int port) : _port(port), _server_fd(-1) {}

    int Server::run() {
        int setup_result = setup_socket();
        if (setup_result < 0) {
            return setup_result;
        }
 
        while(true) {
            int client_fd = accept_connection();
            if (client_fd < 0) {
                break;
            }

            std::thread([client_fd]() {
                Connection conn(client_fd);
                conn.handle();
            }).detach();
        }

        return 0;
    }

    int Server::setup_socket() {
        _server_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (_server_fd < 0) {
            std::cerr << "Failed to create server socket: " << std::endl;
            return _server_fd;
        }

        // Avoid the 'Address already in use' error
        // https://stackoverflow.com/questions/14388706/how-do-so-reuseaddr-and-so-reuseport-differ
        int opt = 1;
        int set_sockopt = setsockopt(_server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        if (set_sockopt < 0) {
            std::cerr << "Failed to set socket option: " << std::endl;
            return set_sockopt;
        }

        // Bind the socket to the port
        struct sockaddr_in server_addr{};
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(_port);
        server_addr.sin_addr.s_addr = INADDR_ANY;
        int bind_result = bind(_server_fd, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr));
        if (bind_result < 0) {
            std::cerr << "Failed to bind socket: " << std::endl;
            return bind_result;
        }

        int connection_backlog = 5;
        int listen_result = listen(_server_fd, connection_backlog);
        if (listen_result < 0) {
            std::cerr << "Failed to listen on socket: " << std::endl;
            return listen_result;
        }

        return 0;
    }

    int Server::accept_connection() {
        struct sockaddr_in client_addr{};
        socklen_t client_addr_len = sizeof(client_addr);
        int client_fd = accept(_server_fd, reinterpret_cast<struct sockaddr*>(&client_addr), &client_addr_len);
        if (client_fd < 0) {
            std::cerr << "Failed to accept connection: " << std::endl;
            return client_fd;
        }

        return client_fd;
    }

    Server::~Server() {
        if (_server_fd >= 0) {
            close(_server_fd);
        }
    }
}
