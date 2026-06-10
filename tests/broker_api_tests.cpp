#include <cassert>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include <kafka/protocol/api_key.hpp>
#include <kafka/protocol/decoder.hpp>
#include <kafka/protocol/encoder.hpp>
#include <kafka/protocol/error_codes.hpp>
#include <kafka/request_handler.hpp>

namespace {

using kafka::protocol::ApiKey;

constexpr std::int32_t CorrelationId = 42;

void write_nullable_string(kafka::protocol::Encoder& encoder, const std::string& value) {
    encoder.write_int16(static_cast<std::int16_t>(value.size()));
    for (char c : value) {
        encoder.write_int8(static_cast<std::int8_t>(c));
    }
}

std::vector<char> request_frame(ApiKey api_key, std::int16_t api_version, const std::vector<char>& body) {
    kafka::protocol::Encoder encoder;
    encoder.write_int16(static_cast<std::int16_t>(api_key));
    encoder.write_int16(api_version);
    encoder.write_int32(CorrelationId);
    write_nullable_string(encoder, "test-client");
    encoder.write_tag_buffer();
    encoder.write_bytes(body);
    encoder.write_message_size();
    return encoder.buffer();
}

std::vector<char> create_topics_body(const std::string& topic, std::int32_t partitions) {
    kafka::protocol::Encoder encoder;
    encoder.write_compact_array<std::string>({topic}, [&](kafka::protocol::Encoder& encoder, const std::string& topic_name) {
        encoder.write_compact_string(topic_name);
        encoder.write_int32(partitions);
        encoder.write_int16(1);
        encoder.write_compact_array<std::int32_t>({}, [](kafka::protocol::Encoder& encoder, std::int32_t) {
            (void)encoder;
        });
        encoder.write_compact_array<std::int32_t>({}, [](kafka::protocol::Encoder& encoder, std::int32_t) {
            (void)encoder;
        });
        encoder.write_tag_buffer();
    });
    encoder.write_int32(1000);
    encoder.write_int8(0);
    encoder.write_tag_buffer();
    return encoder.buffer();
}

std::vector<char> produce_body(const std::string& topic, std::int32_t partition, const std::vector<char>& records) {
    kafka::protocol::Encoder encoder;
    encoder.write_unsigned_varint(0);
    encoder.write_int16(1);
    encoder.write_int32(1000);
    encoder.write_compact_array<std::string>({topic}, [&](kafka::protocol::Encoder& encoder, const std::string& topic_name) {
        encoder.write_compact_string(topic_name);
        encoder.write_compact_array<std::int32_t>({partition}, [&](kafka::protocol::Encoder& encoder, std::int32_t partition_index) {
            encoder.write_int32(partition_index);
            encoder.write_compact_nullable_bytes(records);
            encoder.write_tag_buffer();
        });
        encoder.write_tag_buffer();
    });
    encoder.write_tag_buffer();
    return encoder.buffer();
}

std::vector<char> list_offsets_body(const std::string& topic, std::int32_t partition, std::int64_t timestamp) {
    kafka::protocol::Encoder encoder;
    encoder.write_int32(-1);
    encoder.write_int8(0);
    encoder.write_compact_array<std::string>({topic}, [&](kafka::protocol::Encoder& encoder, const std::string& topic_name) {
        encoder.write_compact_string(topic_name);
        encoder.write_compact_array<std::int32_t>({partition}, [&](kafka::protocol::Encoder& encoder, std::int32_t partition_index) {
            encoder.write_int32(partition_index);
            encoder.write_int64(timestamp);
            encoder.write_tag_buffer();
        });
        encoder.write_tag_buffer();
    });
    encoder.write_tag_buffer();
    return encoder.buffer();
}

std::vector<char> fetch_body(const std::string& topic, std::int32_t partition, std::int64_t offset, std::int32_t max_bytes) {
    kafka::protocol::Encoder encoder;
    encoder.write_int32(0);
    encoder.write_int32(1);
    encoder.write_int32(max_bytes);
    encoder.write_int8(0);
    encoder.write_int32(0);
    encoder.write_int32(0);
    encoder.write_compact_array<std::string>({topic}, [&](kafka::protocol::Encoder& encoder, const std::string& topic_name) {
        for (int i = 0; i < 16; ++i) {
            encoder.write_int8(0);
        }
        encoder.write_compact_string(topic_name);
        encoder.write_compact_array<std::int32_t>({partition}, [&](kafka::protocol::Encoder& encoder, std::int32_t partition_index) {
            encoder.write_int32(partition_index);
            encoder.write_int32(-1);
            encoder.write_int64(offset);
            encoder.write_int64(0);
            encoder.write_int32(max_bytes);
            encoder.write_tag_buffer();
        });
        encoder.write_tag_buffer();
    });
    encoder.write_compact_array<std::int32_t>({}, [](kafka::protocol::Encoder& encoder, std::int32_t) {
        (void)encoder;
    });
    encoder.write_tag_buffer();
    return encoder.buffer();
}

kafka::protocol::Decoder response_decoder(const std::vector<char>& response) {
    kafka::protocol::Decoder decoder(response.data(), response.size());
    decoder.read_int32();
    assert(decoder.read_int32() == CorrelationId);
    decoder.read_tag_buffer();
    return decoder;
}

void assert_create_topic_response(const std::vector<char>& response, std::int16_t expected_error) {
    auto decoder = response_decoder(response);
    assert(decoder.read_int32() == 0);
    auto topics = decoder.read_compact_array<std::int16_t>([](kafka::protocol::Decoder& decoder) {
        decoder.read_compact_string();
        auto error_code = decoder.read_int16();
        decoder.read_unsigned_varint();
        decoder.read_int32();
        decoder.read_int16();
        decoder.read_compact_array<std::int8_t>([](kafka::protocol::Decoder& decoder) {
            decoder.read_compact_string();
            decoder.read_compact_string();
            decoder.read_tag_buffer();
            return 0;
        });
        decoder.read_tag_buffer();
        return error_code;
    });
    assert(topics.size() == 1);
    assert(topics[0] == expected_error);
}

std::int64_t produce_base_offset(const std::vector<char>& response) {
    auto decoder = response_decoder(response);
    auto topics = decoder.read_compact_array<std::int64_t>([](kafka::protocol::Decoder& decoder) {
        decoder.read_compact_string();
        auto partitions = decoder.read_compact_array<std::int64_t>([](kafka::protocol::Decoder& decoder) {
            decoder.read_int32();
            assert(decoder.read_int16() == kafka::protocol::error::None);
            auto base_offset = decoder.read_int64();
            decoder.read_int64();
            decoder.read_int64();
            decoder.read_compact_array<std::int32_t>([](kafka::protocol::Decoder& decoder) {
                return decoder.read_int32();
            });
            decoder.read_unsigned_varint();
            decoder.read_tag_buffer();
            return base_offset;
        });
        decoder.read_tag_buffer();
        assert(partitions.size() == 1);
        return partitions[0];
    });
    assert(topics.size() == 1);
    assert(decoder.read_int32() == 0);
    decoder.read_tag_buffer();
    return topics[0];
}

std::int64_t list_offset(const std::vector<char>& response) {
    auto decoder = response_decoder(response);
    assert(decoder.read_int32() == 0);
    auto topics = decoder.read_compact_array<std::int64_t>([](kafka::protocol::Decoder& decoder) {
        decoder.read_compact_string();
        auto partitions = decoder.read_compact_array<std::int64_t>([](kafka::protocol::Decoder& decoder) {
            decoder.read_int32();
            assert(decoder.read_int16() == kafka::protocol::error::None);
            decoder.read_int64();
            auto offset = decoder.read_int64();
            decoder.read_tag_buffer();
            return offset;
        });
        decoder.read_tag_buffer();
        assert(partitions.size() == 1);
        return partitions[0];
    });
    decoder.read_tag_buffer();
    assert(topics.size() == 1);
    return topics[0];
}

std::vector<char> fetched_records(const std::vector<char>& response, std::int64_t expected_high_watermark) {
    auto decoder = response_decoder(response);
    assert(decoder.read_int32() == 0);
    assert(decoder.read_int16() == kafka::protocol::error::None);
    assert(decoder.read_int32() == 0);
    auto topics = decoder.read_compact_array<std::vector<char>>([&](kafka::protocol::Decoder& decoder) {
        decoder.read_bytes(16);
        decoder.read_compact_string();
        auto partitions = decoder.read_compact_array<std::vector<char>>([&](kafka::protocol::Decoder& decoder) {
            decoder.read_int32();
            assert(decoder.read_int16() == kafka::protocol::error::None);
            assert(decoder.read_int64() == expected_high_watermark);
            assert(decoder.read_int64() == 0);
            decoder.read_int64();
            decoder.read_compact_array<std::int32_t>([](kafka::protocol::Decoder& decoder) {
                return decoder.read_int32();
            });
            auto encoded_size = decoder.read_unsigned_varint();
            assert(encoded_size > 0);
            auto records = decoder.read_bytes(encoded_size - 1);
            decoder.read_tag_buffer();
            return records;
        });
        decoder.read_tag_buffer();
        assert(partitions.size() == 1);
        return partitions[0];
    });
    decoder.read_tag_buffer();
    assert(topics.size() == 1);
    return topics[0];
}

void test_create_produce_list_offsets_and_fetch() {
    kafka::RequestHandler::reset_state();

    const std::string topic = "orders";
    const std::vector<char> first_records{'a', 'b', 'c'};
    const std::vector<char> second_records{'d', 'e'};

    auto create_response = kafka::RequestHandler::handle_request(
        request_frame(ApiKey::CreateTopics, 5, create_topics_body(topic, 2))
    );
    assert_create_topic_response(create_response, kafka::protocol::error::None);

    auto duplicate_response = kafka::RequestHandler::handle_request(
        request_frame(ApiKey::CreateTopics, 5, create_topics_body(topic, 2))
    );
    assert_create_topic_response(duplicate_response, kafka::protocol::error::TopicAlreadyExists);

    auto first_produce_response = kafka::RequestHandler::handle_request(
        request_frame(ApiKey::Produce, 11, produce_body(topic, 0, first_records))
    );
    assert(produce_base_offset(first_produce_response) == 0);

    auto second_produce_response = kafka::RequestHandler::handle_request(
        request_frame(ApiKey::Produce, 11, produce_body(topic, 0, second_records))
    );
    assert(produce_base_offset(second_produce_response) == static_cast<std::int64_t>(first_records.size()));

    auto earliest_response = kafka::RequestHandler::handle_request(
        request_frame(ApiKey::ListOffsets, 7, list_offsets_body(topic, 0, -2))
    );
    assert(list_offset(earliest_response) == 0);

    auto latest_response = kafka::RequestHandler::handle_request(
        request_frame(ApiKey::ListOffsets, 7, list_offsets_body(topic, 0, -1))
    );
    assert(list_offset(latest_response) == static_cast<std::int64_t>(first_records.size() + second_records.size()));

    auto fetch_all_response = kafka::RequestHandler::handle_request(
        request_frame(ApiKey::Fetch, 11, fetch_body(topic, 0, 0, 1024))
    );
    const std::vector<char> all_records{'a', 'b', 'c', 'd', 'e'};
    assert(fetched_records(fetch_all_response, all_records.size()) == all_records);

    auto fetch_from_second_response = kafka::RequestHandler::handle_request(
        request_frame(ApiKey::Fetch, 11, fetch_body(topic, 0, first_records.size(), 1024))
    );
    assert(fetched_records(fetch_from_second_response, all_records.size()) == second_records);
}

} // namespace

int main() {
    test_create_produce_list_offsets_and_fetch();
    std::cout << "broker_api_tests passed\n";
    return 0;
}
