#include <fstream>
#include <iostream>
#include <string>

#include <kafka/server.hpp>

namespace {
    std::string read_log_dir(const char* properties_path) {
        std::ifstream file(properties_path);
        if (!file) {
            return properties_path;
        }

        std::string line;
        while (std::getline(file, line)) {
            constexpr std::string_view key = "log.dirs=";
            if (line.rfind(key, 0) == 0) {
                return line.substr(key.size());
            }
        }

        return "/tmp/kraft-combined-logs";
    }
}

int main(int argc, char* argv[]) {
    // Disable output buffering
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    std::string log_dir = "/tmp/kraft-combined-logs";
    if (argc > 1) {
        log_dir = read_log_dir(argv[1]);
    }

    kafka::Server server(9092, log_dir);
    return server.run();;
}
