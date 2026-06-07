#include <kafka/request_handler.hpp>

#include <algorithm>
#include <cstddef>
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
#include <kafka/protocol/apis/create_topics.hpp>
#include <kafka/protocol/apis/describe_topic_partitions.hpp>
#include <kafka/protocol/apis/fetch.hpp>
#include <kafka/protocol/apis/list_offsets.hpp>
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

        std::int64_t partition_log_size(const std::string& topic, std::int32_t partition_index) {
            auto path = topic_partition_log_path(topic, partition_index);
            if (!std::filesystem::exists(path)) {
                return 0;
            }

            return static_cast<std::int64_t>(std::filesystem::file_size(path));
        }

        std::int64_t write_partition_records(
            const std::string& topic,
            std::int32_t partition_index,
            const std::vector<char>& records
        ) {
            auto path = topic_partition_log_path(topic, partition_index);
            std::filesystem::create_directories(path.parent_path());
            std::int64_t base_offset = partition_log_size(topic, partition_index);

            std::ofstream file(path, std::ios::binary | std::ios::app);
            if (!file) {
                return -1;
            }

            file.write(records.data(), static_cast<std::streamsize>(records.size()));
            return file.good() ? base_offset : -1;
        }

        std::vector<char> read_partition_records(
            const std::string& topic,
            std::int32_t partition_index,
            std::int64_t offset,
            std::int32_t max_bytes
        ) {
            auto path = topic_partition_log_path(topic, partition_index);
            if (!std::filesystem::exists(path) || offset < 0) {
                return {};
            }

            std::ifstream file(path, std::ios::binary);
            if (!file) {
                return {};
            }

            auto size = partition_log_size(topic, partition_index);
            if (offset >= size) {
                return {};
            }

            auto bytes_to_read = size - offset;
            if (max_bytes > 0) {
                bytes_to_read = std::min<std::int64_t>(bytes_to_read, max_bytes);
            }

            std::vector<char> records(static_cast<std::size_t>(bytes_to_read));
            file.seekg(offset);
            file.read(records.data(), static_cast<std::streamsize>(records.size()));
            records.resize(static_cast<std::size_t>(file.gcount()));
            return records;
        }

        bool create_topic_partition_log(const std::string& topic, std::int32_t partition_index) {
            auto path = topic_partition_log_path(topic, partition_index);
            std::filesystem::create_directories(path.parent_path());
            std::ofstream file(path, std::ios::binary | std::ios::app);
            return file.good();
        }

        bool partition_exists(const TopicMetadata* topic_metadata, std::int32_t partition_index) {
            return topic_metadata != nullptr &&
                std::any_of(topic_metadata->partitions.begin(), topic_metadata->partitions.end(),
                    [&](const auto& partition_metadata) {
                        return partition_metadata.partition_index == partition_index;
                    });
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
                case protocol::ApiKey::Fetch:
                    return encode_response(handle_fetch(request));
                case protocol::ApiKey::ListOffsets:
                    return encode_response(handle_list_offsets(request));
                case protocol::ApiKey::CreateTopics:
                    return encode_response(handle_create_topics(request));
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
            if (header.api_key != protocol::ApiKey::ApiVersion) {
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
        auto metadata = ClusterMetadata::read_from_log_dir(LOG_DIR);

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
        auto metadata = ClusterMetadata::read_from_log_dir(LOG_DIR);

        for (const auto& requested_topic : produce_request.topics) {
            protocol::ProduceResponseTopic topic_response;
            topic_response.name = requested_topic.name;
            const auto* topic_metadata = metadata.find_topic(requested_topic.name);

            for (const auto& requested_partition : requested_topic.partitions) {
                if (partition_exists(topic_metadata, requested_partition.index)) {
                    auto base_offset = write_partition_records(requested_topic.name, requested_partition.index, requested_partition.records);
                    topic_response.partitions.push_back(protocol::ProduceResponsePartition{
                        requested_partition.index,
                        base_offset >= 0 ? protocol::error::None : protocol::error::UnsupportedError,
                        base_offset,
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

    Response RequestHandler::handle_fetch(const Request& request) {
        protocol::Decoder decoder(request.buffer.data(), request.buffer.size());
        auto fetch_request = protocol::read_fetch_request(decoder);

        protocol::FetchResponseBody body;
        body.throttle_time_ms = 0;
        body.error_code = protocol::error::None;
        body.session_id = 0;
        auto metadata = ClusterMetadata::read_from_log_dir(LOG_DIR);

        for (const auto& requested_topic : fetch_request.topics) {
            protocol::FetchResponseTopic topic_response;
            topic_response.name = requested_topic.name;
            const auto* topic_metadata = metadata.find_topic(requested_topic.name);

            for (const auto& requested_partition : requested_topic.partitions) {
                auto high_watermark = partition_log_size(requested_topic.name, requested_partition.index);
                auto max_bytes = requested_partition.partition_max_bytes > 0
                    ? requested_partition.partition_max_bytes
                    : fetch_request.max_bytes;

                if (partition_exists(topic_metadata, requested_partition.index)) {
                    topic_response.partitions.push_back(protocol::FetchResponsePartition{
                        requested_partition.index,
                        protocol::error::None,
                        high_watermark,
                        0,
                        read_partition_records(
                            requested_topic.name,
                            requested_partition.index,
                            requested_partition.fetch_offset,
                            max_bytes
                        )
                    });
                    continue;
                }

                topic_response.partitions.push_back(protocol::FetchResponsePartition{
                    requested_partition.index,
                    protocol::error::UnknownTopicOrPartition,
                    -1,
                    -1,
                    {}
                });
            }

            body.topics.push_back(topic_response);
        }

        return Response{
            Response::Type::Fetch,
            request.header.correlation_id,
            protocol::error::None,
            protocol::ApiVersionsResponseBody{},
            protocol::DescribeTopicPartitionsResponseBody{},
            protocol::ProduceResponseBody{},
            body
        };
    }

    Response RequestHandler::handle_list_offsets(const Request& request) {
        protocol::Decoder decoder(request.buffer.data(), request.buffer.size());
        auto list_offsets_request = protocol::read_list_offsets_request(decoder);

        protocol::ListOffsetsResponseBody body;
        body.throttle_time_ms = 0;
        auto metadata = ClusterMetadata::read_from_log_dir(LOG_DIR);

        for (const auto& requested_topic : list_offsets_request.topics) {
            protocol::ListOffsetsResponseTopic topic_response;
            topic_response.name = requested_topic.name;
            const auto* topic_metadata = metadata.find_topic(requested_topic.name);

            for (const auto& requested_partition : requested_topic.partitions) {
                if (partition_exists(topic_metadata, requested_partition.index)) {
                    const auto offset = requested_partition.timestamp == -2
                        ? 0
                        : partition_log_size(requested_topic.name, requested_partition.index);
                    topic_response.partitions.push_back(protocol::ListOffsetsResponsePartition{
                        requested_partition.index,
                        protocol::error::None,
                        requested_partition.timestamp,
                        offset
                    });
                    continue;
                }

                topic_response.partitions.push_back(protocol::ListOffsetsResponsePartition{
                    requested_partition.index,
                    protocol::error::UnknownTopicOrPartition,
                    requested_partition.timestamp,
                    -1
                });
            }

            body.topics.push_back(topic_response);
        }

        return Response{
            Response::Type::ListOffsets,
            request.header.correlation_id,
            protocol::error::None,
            protocol::ApiVersionsResponseBody{},
            protocol::DescribeTopicPartitionsResponseBody{},
            protocol::ProduceResponseBody{},
            protocol::FetchResponseBody{},
            body
        };
    }

    Response RequestHandler::handle_create_topics(const Request& request) {
        protocol::Decoder decoder(request.buffer.data(), request.buffer.size());
        auto create_topics_request = protocol::read_create_topics_request(decoder);

        protocol::CreateTopicsResponseBody body;
        body.throttle_time_ms = 0;
        auto metadata = ClusterMetadata::read_from_log_dir(LOG_DIR);

        for (const auto& requested_topic : create_topics_request.topics) {
            auto partition_count = std::max<std::int32_t>(requested_topic.num_partitions, 1);
            auto replication_factor = requested_topic.replication_factor > 0
                ? requested_topic.replication_factor
                : static_cast<std::int16_t>(1);

            if (metadata.find_topic(requested_topic.name) != nullptr) {
                body.topics.push_back(protocol::CreateTopicsResponseTopic{
                    requested_topic.name,
                    protocol::error::TopicAlreadyExists,
                    partition_count,
                    replication_factor
                });
                continue;
            }

            bool created = true;
            for (std::int32_t partition = 0; partition < partition_count; ++partition) {
                created = create_topic_partition_log(requested_topic.name, partition) && created;
            }

            body.topics.push_back(protocol::CreateTopicsResponseTopic{
                requested_topic.name,
                created ? protocol::error::None : protocol::error::UnsupportedError,
                partition_count,
                replication_factor
            });
        }

        return Response{
            Response::Type::CreateTopics,
            request.header.correlation_id,
            protocol::error::None,
            protocol::ApiVersionsResponseBody{},
            protocol::DescribeTopicPartitionsResponseBody{},
            protocol::ProduceResponseBody{},
            protocol::FetchResponseBody{},
            protocol::ListOffsetsResponseBody{},
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
        } else if (response.type == Response::Type::Fetch) {
            encoder.write_tag_buffer();
            protocol::write_fetch_response(encoder, response.fetch);
        } else if (response.type == Response::Type::ListOffsets) {
            encoder.write_tag_buffer();
            protocol::write_list_offsets_response(encoder, response.list_offsets);
        } else if (response.type == Response::Type::CreateTopics) {
            encoder.write_tag_buffer();
            protocol::write_create_topics_response(encoder, response.create_topics);
        }

        encoder.write_message_size();

        return encoder.buffer();
    }
}
