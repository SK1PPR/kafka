#include <kafka/request_handler.hpp>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <string>
#include <utility>

#include <kafka/cluster_metadata.hpp>
#include <kafka/protocol/api_key.hpp>
#include <kafka/protocol/encoder.hpp>
#include <kafka/request.hpp>
#include <kafka/response.hpp>
#include <kafka/error.hpp>
#include <kafka/protocol/apis/api_versions.hpp>
#include <kafka/protocol/apis/describe_topic_partitions.hpp>
#include <kafka/protocol/apis/produce.hpp>
#include <kafka/protocol/error_codes.hpp>

namespace kafka {
    namespace {
        thread_local std::string LOG_DIR = "/tmp/kraft-combined-logs";

        std::filesystem::path topic_partition_log_path(const std::string& topic, std::int32_t partition_index) {
            return std::filesystem::path(LOG_DIR) /
                (topic + "-" + std::to_string(partition_index)) /
                "00000000000000000000.log";
        }

        bool write_partition_records(
            const std::string& topic,
            std::int32_t partition_index,
            const std::vector<char>& records
        ) {
            auto path = topic_partition_log_path(topic, partition_index);
            std::filesystem::create_directories(path.parent_path());

            std::ofstream file(path, std::ios::binary | std::ios::app);
            if (!file) {
                return false;
            }

            file.write(records.data(), static_cast<std::streamsize>(records.size()));
            return file.good();
        }
    }

    void RequestHandler::set_log_dir(std::string log_dir) {
        LOG_DIR = std::move(log_dir);
    }

    std::vector<char> RequestHandler::handle_request(const std::vector<char>& input_buffer) {
        try {
            Request request = decode_request(input_buffer);

            switch (request.header.api_key) {
                case protocol::ApiKey::ApiVersion:
                    return encode_response(handle_api_versions(request));
                case protocol::ApiKey::DescribeTopicPartition:
                    return encode_response(handle_describe_topic_partition(request));
                case protocol::ApiKey::Produce:
                    return encode_response(handle_produce(request));
                default:
                    return encode_response(handle_error(request.header.correlation_id, protocol::error::UnsupportedError));
            }
        }
        catch (const KafkaRequestError& error) {
            return encode_response(handle_error(error.correlation_id(), error.error_code()));
        }

    }

    RequestHeader RequestHandler::decode_request_header(protocol::Decoder& decoder) {
        RequestHeader header;
        header.message_size = decoder.read_int32();
        int16_t request_api_key = decoder.read_int16();
        header.api_version = decoder.read_int16();
        header.correlation_id = decoder.read_int32();
        auto api_key = protocol::api_key_from_int(request_api_key);
        if (!api_key) {
            throw KafkaRequestError{header.correlation_id, protocol::error::UnsupportedError};
        }
        header.api_key = *api_key;
        if (!protocol::supports_version(header.api_key, header.api_version)) {
            throw KafkaRequestError{header.correlation_id, protocol::error::UnsupportedError};
        }
        header.header_version = protocol::request_header_version(header.api_key, header.api_version);

        if (header.header_version == 1) {
            header.client_id = decoder.read_nullable_string();
            if (header.api_key == protocol::ApiKey::DescribeTopicPartition ||
                header.api_key == protocol::ApiKey::Produce) {
                decoder.read_tag_buffer();
            }
        } else if (header.header_version == 2) {
            header.client_id = decoder.read_compact_nullable_string();
            decoder.read_tag_buffer();
        }

        return header;
    }

    Request RequestHandler::decode_request(const std::vector<char>& input_buffer) {
        const char* buffer = input_buffer.data();
        protocol::Decoder decoder(buffer, input_buffer.size());

        Request request;
        request.header = decode_request_header(decoder);
        request.buffer = decoder.read_body();

        return request;
    }

    Response RequestHandler::handle_api_versions(const Request& request) {
        Response response{
            Response::Type::ApiVersions,
            request.header.correlation_id,
            0,
            protocol::ApiVersionsResponseBody{
                0,
                std::vector<protocol::ApiSpec>(protocol::supported_apis().begin(), protocol::supported_apis().end()),
                0
            },
            protocol::DescribeTopicPartitionsResponseBody{},
            protocol::ProduceResponseBody{}
        };

        return response;
    }

    Response RequestHandler::handle_error(const std::int32_t correlation_id, const std::int16_t error_code) {
        return Response{
            Response::Type::Error,
            correlation_id,
            error_code,
            protocol::ApiVersionsResponseBody{},
            protocol::DescribeTopicPartitionsResponseBody{},
            protocol::ProduceResponseBody{}
        };
    }

    Response RequestHandler::handle_describe_topic_partition(const Request& request) {
        protocol::Decoder decoder(request.buffer.data(), request.buffer.size());
        auto describe_request = protocol::read_describe_topic_partitions_request(decoder);

        protocol::DescribeTopicPartitionsResponseBody body;
        body.throttle_time_ms = 0;
        body.next_cursor = -1;
        auto metadata = ClusterMetadata::read_from_default_path();

        for (const auto& requested_topic : describe_request.topics) {
            const auto* topic_metadata = metadata.find_topic(requested_topic.name);
            if (topic_metadata != nullptr) {
                protocol::DescribeTopicPartitionsResponseTopic topic_response{
                    protocol::error::None,
                    requested_topic.name,
                    topic_metadata->topic_id,
                    false,
                    {},
                    0
                };

                for (const auto& partition_metadata : topic_metadata->partitions) {
                    topic_response.partitions.push_back(protocol::DescribeTopicPartitionsResponsePartition{
                        protocol::error::None,
                        partition_metadata.partition_index,
                        partition_metadata.leader_id,
                        partition_metadata.leader_epoch,
                        partition_metadata.replica_nodes,
                        partition_metadata.isr_nodes,
                        {},
                        {},
                        {}
                    });
                }

                body.topics.push_back(topic_response);
                continue;
            }

            body.topics.push_back(protocol::DescribeTopicPartitionsResponseTopic{
                protocol::error::UnknownTopicOrPartition,
                requested_topic.name,
                {},
                false,
                {},
                0
            });
        }

        std::sort(body.topics.begin(), body.topics.end(), [](const auto& lhs, const auto& rhs) {
            return lhs.name < rhs.name;
        });

        return Response{
            Response::Type::DescribeTopicPartition,
            request.header.correlation_id,
            protocol::error::None,
            protocol::ApiVersionsResponseBody{},
            body,
            protocol::ProduceResponseBody{}
        };
    }

    Response RequestHandler::handle_produce(const Request& request) {
        protocol::Decoder decoder(request.buffer.data(), request.buffer.size());
        auto produce_request = protocol::read_produce_request(decoder);

        protocol::ProduceResponseBody body;
        body.throttle_time_ms = 0;
        auto metadata = ClusterMetadata::read_from_default_path();

        for (const auto& requested_topic : produce_request.topics) {
            protocol::ProduceResponseTopic topic_response;
            topic_response.name = requested_topic.name;
            const auto* topic_metadata = metadata.find_topic(requested_topic.name);

            for (const auto& requested_partition : requested_topic.partitions) {
                const bool partition_exists = topic_metadata != nullptr &&
                    std::any_of(topic_metadata->partitions.begin(), topic_metadata->partitions.end(),
                        [&](const auto& partition_metadata) {
                            return partition_metadata.partition_index == requested_partition.index;
                        });

                if (partition_exists) {
                    write_partition_records(requested_topic.name, requested_partition.index, requested_partition.records);
                    topic_response.partitions.push_back(protocol::ProduceResponsePartition{
                        requested_partition.index,
                        protocol::error::None,
                        0,
                        -1,
                        0
                    });
                    continue;
                }

                topic_response.partitions.push_back(protocol::ProduceResponsePartition{
                    requested_partition.index,
                    protocol::error::UnknownTopicOrPartition,
                    -1,
                    -1,
                    -1
                });
            }

            body.topics.push_back(topic_response);
        }

        return Response{
            Response::Type::Produce,
            request.header.correlation_id,
            protocol::error::None,
            protocol::ApiVersionsResponseBody{},
            protocol::DescribeTopicPartitionsResponseBody{},
            body
        };
    }

    std::vector<char> RequestHandler::encode_response(const Response& response) {
        protocol::Encoder encoder;

        encoder.write_int32(response.correlation_id);

        if (response.type == Response::Type::ApiVersions) {
            protocol::write_api_versions_response(encoder, response.api_versions);
        } else if (response.type == Response::Type::Error) {
            encoder.write_int16(response.error_code);
        } else if (response.type == Response::Type::DescribeTopicPartition) {
            encoder.write_tag_buffer();
            protocol::write_describe_topic_partitions_response(encoder, response.describe_topic_partition);
        } else if (response.type == Response::Type::Produce) {
            encoder.write_tag_buffer();
            protocol::write_produce_response(encoder, response.produce);
        }

        encoder.write_message_size();

        return encoder.buffer();
    }
}
