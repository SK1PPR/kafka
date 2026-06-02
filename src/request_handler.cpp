#include <kafka/request_handler.hpp>

#include <kafka/protocol/api_key.hpp>
#include <kafka/protocol/decoder.hpp>
#include <kafka/protocol/encoder.hpp>
#include <kafka/request.hpp>
#include <kafka/response.hpp>

namespace kafka {
    std::vector<char> RequestHandler::handle_request(const std::vector<char>& input_buffer) {
        Request request = decode_request(input_buffer);

        if (auto api_key = protocol::api_key_from_int(request.request_api_key)) {
            if (!protocol::supports_version(*api_key, request.request_api_version)) {
                return encode_response(handle_unsupported_version(request));
            }

            if (*api_key == protocol::ApiKey::ApiVersion) {
                return encode_response(handle_api_versions(request));
            }
        }

        return encode_response(handle_unsupported(request));
    }

    Request RequestHandler::decode_request(const std::vector<char>& input_buffer) {
        const char* buffer = input_buffer.data();
        protocol::Decoder decoder(buffer, input_buffer.size());

        Request request;
        request.message_size = decoder.read_int32();
        request.request_api_key = decoder.read_int16();
        request.request_api_version = decoder.read_int16();
        request.correlation_id = decoder.read_int32();
        request.buffer = decoder.read_bytes(request.message_size - 8);

        return request;
    }

    Response RequestHandler::handle_api_versions(const Request& request) {
        Response response{
            Response::Type::ApiVersions,
            request.correlation_id,
            0,
            ApiVersionsResponseBody{
                0,
                std::vector<protocol::ApiSpec>(protocol::supported_apis().begin(), protocol::supported_apis().end()),
                0
            }
        };

        return response;
    }

    Response RequestHandler::handle_unsupported(const Request& request) {
        return Response{
            Response::Type::Error,
            request.correlation_id,
            static_cast<std::int16_t>(35),
            ApiVersionsResponseBody{}
        };
    }

    Response RequestHandler::handle_unsupported_version(const Request& request) {
        return Response{
            Response::Type::Error,
            request.correlation_id,
            static_cast<std::int16_t>(35),
            ApiVersionsResponseBody{}
        };
    }

    std::vector<char> RequestHandler::encode_response(const Response& response) {
        protocol::Encoder encoder;

        encoder.write_int32(response.correlation_id);

        if (response.type == Response::Type::ApiVersions) {
            encoder.write_int16(response.api_versions.error_code);
            encoder.write_int8(static_cast<std::int8_t>(response.api_versions.api_keys.size() + 1));

            for (const auto& api : response.api_versions.api_keys) {
                encoder.write_int16(static_cast<std::int16_t>(api.key));
                encoder.write_int16(api.min_version);
                encoder.write_int16(api.max_version);
                encoder.write_int8(0); // TAG_BUFFER
            }

            encoder.write_int32(response.api_versions.throttle_time_ms);
            encoder.write_int8(0); // TAG_BUFFER
        } else if (response.type == Response::Type::Error) {
            encoder.write_int16(response.error_code);
        }

        encoder.write_message_size();

        return encoder.buffer();
    }
}
