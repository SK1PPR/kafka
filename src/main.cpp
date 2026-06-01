#include <iostream>

#include <kafka/server.hpp>

int main(int argc, char* argv[]) {
    // Disable output buffering
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    kafka::Server server(9092);
    return server.run();;
}
