#include <kafka/request_handler.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

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
        struct PartitionState {
            std::vector<char> records;
        };

        struct TopicState {
            std::array<std::uint8_t, 16> topic_id{};
            std::unordered_map<std::int32_t, PartitionState> partitions;
        };

        std::mutex STORE_MUTEX;
        std::unordered_map<std::string, TopicState> TOPICS;

        std::array<std::uint8_t, 16> deterministic_topic_id(const std::string& topic) {
            std::array<std::uint8_t, 16> uuid{};
            auto first = std::hash<std::string>{}(topic);
            auto second = std::hash<std::string>{}("kafka-cpp:" + topic);

            for (int i = 0; i < 8; ++i) {
                uuid[static_cast<std::size_t>(i)] = static_cast<std::uint8_t>((first >> (i * 8)) & 0xff);
                uuid[static_cast<std::size_t>(i + 8)] = static_cast<std::uint8_t>((second >> (i * 8)) & 0xff);
            }

            return uuid;
        }

        const TopicState* find_topic(const std::string& topic) {
            auto iter = TOPICS.find(topic);
            if (iter == TOPICS.end()) {
                return nullptr;
            }
            return &iter->second;
        }

        TopicState* find_mutable_topic(const std::string& topic) {
            auto iter = TOPICS.find(topic);
            if (iter == TOPICS.end()) {
                return nullptr;
            }
            return &iter->second;
        }

        bool partition_exists(const TopicState* topic, std::int32_t partition_index) {
            return topic != nullptr && topic->partitions.find(partition_index) != topic->partitions.end();
        }

        std::vector<std::int32_t> sorted_partition_indexes(const TopicState& topic) {
            std::vector<std::int32_t> indexes;
            indexes.reserve(topic.partitions.size());
            for (const auto& [partition_index, _] : topic.partitions) {
                indexes.push_back(partition_index);
            }
            std::sort(indexes.begin(), indexes.end());
            return indexes;
        }

        std::int64_t partition_log_size(const TopicState* topic, std::int32_t partition_index) {
            if (!partition_exists(topic, partition_index)) {
                return 0;
            }
            return static_cast<std::int64_t>(topic->partitions.at(partition_index).records.size());
        }

        std::int64_t write_partition_records(TopicState* topic, std::int32_t partition_index, const std::vector<char>& records) {
            if (!partition_exists(topic, partition_index)) {
                return -1;
            }

            auto& partition_records = topic->partitions[partition_index].records;
            auto base_offset = static_cast<std::int64_t>(partition_records.size());
            partition_records.insert(partition_records.end(), records.begin(), records.end());
            return base_offset;
        }

        std::vector<char> read_partition_records(
            const TopicState* topic,
            std::int32_t partition_index,
            std::int64_t offset,
            std::int32_t max_bytes
        ) {
            if (!partition_exists(topic, partition_index) || offset < 0) {
                return {};
            }

            const auto& records = topic->partitions.at(partition_index).records;
            auto size = static_cast<std::int64_t>(records.size());
            if (offset >= size) {
                return {};
            }

            auto bytes_to_read = size - offset;
            if (max_bytes > 0) {
                bytes_to_read = std::min<std::int64_t>(bytes_to_read, max_bytes);
            }

            auto begin = records.begin() + static_cast<std::ptrdiff_t>(offset);
            auto end = begin + static_cast<std::ptrdiff_t>(bytes_to_read);
            return {begin, end};
        }
    }

    void RequestHandler::set_log_dir(std::string log_dir) {
        (void)log_dir;
    }

    void RequestHandler::reset_state() {
        std::lock_guard<std::mutex> lock(STORE_MUTEX);
        TOPICS.clear();
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
        std::lock_guard<std::mutex> lock(STORE_MUTEX);

        for (const auto& requested_topic : describe_request.topics) {
            const auto* topic = find_topic(requested_topic.name);
            if (topic != nullptr) {
                protocol::DescribeTopicPartitionsResponseTopic topic_response{
                    protocol::error::None,
                    requested_topic.name,
                    topic->topic_id,
                    false,
                    {},
                    0
                };

                for (const auto partition_index : sorted_partition_indexes(*topic)) {
                    topic_response.partitions.push_back(protocol::DescribeTopicPartitionsResponsePartition{
                        protocol::error::None,
                        partition_index,
                        0,
                        0,
                        {0},
                        {0},
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
        std::lock_guard<std::mutex> lock(STORE_MUTEX);

        for (const auto& requested_topic : produce_request.topics) {
            protocol::ProduceResponseTopic topic_response;
            topic_response.name = requested_topic.name;
            auto* topic = find_mutable_topic(requested_topic.name);

            for (const auto& requested_partition : requested_topic.partitions) {
                if (partition_exists(topic, requested_partition.index)) {
                    auto base_offset = write_partition_records(topic, requested_partition.index, requested_partition.records);
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
        std::lock_guard<std::mutex> lock(STORE_MUTEX);

        for (const auto& requested_topic : fetch_request.topics) {
            protocol::FetchResponseTopic topic_response;
            topic_response.name = requested_topic.name;
            const auto* topic = find_topic(requested_topic.name);

            for (const auto& requested_partition : requested_topic.partitions) {
                auto high_watermark = partition_log_size(topic, requested_partition.index);
                auto max_bytes = requested_partition.partition_max_bytes > 0
                    ? requested_partition.partition_max_bytes
                    : fetch_request.max_bytes;

                if (partition_exists(topic, requested_partition.index)) {
                    topic_response.partitions.push_back(protocol::FetchResponsePartition{
                        requested_partition.index,
                        protocol::error::None,
                        high_watermark,
                        0,
                        read_partition_records(
                            topic,
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
        std::lock_guard<std::mutex> lock(STORE_MUTEX);

        for (const auto& requested_topic : list_offsets_request.topics) {
            protocol::ListOffsetsResponseTopic topic_response;
            topic_response.name = requested_topic.name;
            const auto* topic = find_topic(requested_topic.name);

            for (const auto& requested_partition : requested_topic.partitions) {
                if (partition_exists(topic, requested_partition.index)) {
                    const auto offset = requested_partition.timestamp == -2
                        ? 0
                        : partition_log_size(topic, requested_partition.index);
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
        std::lock_guard<std::mutex> lock(STORE_MUTEX);

        for (const auto& requested_topic : create_topics_request.topics) {
            auto partition_count = std::max<std::int32_t>(requested_topic.num_partitions, 1);
            auto replication_factor = requested_topic.replication_factor > 0
                ? requested_topic.replication_factor
                : static_cast<std::int16_t>(1);

            if (find_topic(requested_topic.name) != nullptr) {
                body.topics.push_back(protocol::CreateTopicsResponseTopic{
                    requested_topic.name,
                    protocol::error::TopicAlreadyExists,
                    partition_count,
                    replication_factor
                });
                continue;
            }

            TopicState topic;
            topic.topic_id = deterministic_topic_id(requested_topic.name);
            for (std::int32_t partition = 0; partition < partition_count; ++partition) {
                topic.partitions.emplace(partition, PartitionState{});
            }
            TOPICS.emplace(requested_topic.name, std::move(topic));

            body.topics.push_back(protocol::CreateTopicsResponseTopic{
                requested_topic.name,
                protocol::error::None,
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
