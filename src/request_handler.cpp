#include <kafka/request_handler.hpp>

#include <kafka/protocol/decoder.hpp>
#include <kafka/protocol/encoder.hpp>
#include <kafka/request.hpp>

namespace kafka {
    std::vector<char> RequestHandler::handle_request(const std::vector<char>& input_buffer) {
        const char* buffer = input_buffer.data();
        protocol::Decoder decoder(buffer, input_buffer.size());

        Request request;
        request.message_size = decoder.read_int32();
        request.request_api_key = decoder.read_int16();
        request.request_api_version = decoder.read_int16();
        request.correlation_id = decoder.read_int32();
        // request.buffer = decoder.read_bytes(request.message_size - 8);

        protocol::Encoder encoder;
    
        encoder.write_int32(request.correlation_id);
        // Response body
        if (request.request_api_key == 18 && request.request_api_version <= 4) {
            encoder.write_int16(0); // Success
            encoder.write_int8(2); // API Keys Array Length
            encoder.write_int16(18); // API Key
            encoder.write_int16(0); // Min API Version
            encoder.write_int16(4); // Max API Version
            encoder.write_int8(0); // TAG Buffer
            encoder.write_int32(0); // Throttle time
            encoder.write_int8(0); // TAG Buffer
        } else {
            encoder.write_int16(static_cast<int16_t>(35)); // Fixed error code UNSUPPORTED_VERSION for now
        }
        encoder.write_message_size();

        return encoder.buffer();
    }
}