#include <kafka/cluster_metadata.hpp>

#include <algorithm>
#include <fstream>
#include <stdexcept>
#include <unordered_map>

namespace kafka {

namespace {
    constexpr const char* DEFAULT_METADATA_LOG_PATH =
        "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";

    std::string uuid_key(const std::array<std::uint8_t, 16>& uuid) {
        return std::string(reinterpret_cast<const char*>(uuid.data()), uuid.size());
    }

    class MetadataReader {
        public:
            explicit MetadataReader(const std::vector<char>& data) : _data(data) {}

            bool empty() const {
                return _position >= _data.size();
            }

            std::size_t remaining() const {
                return _data.size() - _position;
            }

            std::size_t position() const {
                return _position;
            }

            void seek(std::size_t position) {
                if (position > _data.size()) {
                    throw std::out_of_range("MetadataReader::seek: out of range");
                }
                _position = position;
            }

            std::int8_t read_int8() {
                ensure(1);
                return static_cast<std::int8_t>(_data[_position++]);
            }

            std::int16_t read_int16() {
                ensure(2);
                std::uint16_t value = static_cast<std::uint16_t>(read_uint8()) << 8;
                value |= read_uint8();
                return static_cast<std::int16_t>(value);
            }

            std::int32_t read_int32() {
                ensure(4);
                std::uint32_t value = static_cast<std::uint32_t>(read_uint8()) << 24;
                value |= static_cast<std::uint32_t>(read_uint8()) << 16;
                value |= static_cast<std::uint32_t>(read_uint8()) << 8;
                value |= read_uint8();
                return static_cast<std::int32_t>(value);
            }

            std::int64_t read_int64() {
                ensure(8);
                std::uint64_t value = static_cast<std::uint64_t>(read_uint8()) << 56;
                value |= static_cast<std::uint64_t>(read_uint8()) << 48;
                value |= static_cast<std::uint64_t>(read_uint8()) << 40;
                value |= static_cast<std::uint64_t>(read_uint8()) << 32;
                value |= static_cast<std::uint64_t>(read_uint8()) << 24;
                value |= static_cast<std::uint64_t>(read_uint8()) << 16;
                value |= static_cast<std::uint64_t>(read_uint8()) << 8;
                value |= read_uint8();
                return static_cast<std::int64_t>(value);
            }

            std::int32_t read_varint() {
                std::uint32_t value = read_unsigned_varint();
                return static_cast<std::int32_t>((value >> 1) ^ -static_cast<std::int32_t>(value & 1));
            }

            std::int64_t read_varlong() {
                std::uint64_t value = 0;
                int shift = 0;

                for (int i = 0; i < 10; ++i) {
                    std::uint8_t byte = read_uint8();
                    value |= static_cast<std::uint64_t>(byte & 0x7f) << shift;

                    if ((byte & 0x80) == 0) {
                        return static_cast<std::int64_t>((value >> 1) ^ -static_cast<std::int64_t>(value & 1));
                    }

                    shift += 7;
                }

                throw std::runtime_error("MetadataReader::read_varlong: too long");
            }

            std::uint32_t read_unsigned_varint() {
                std::uint32_t value = 0;
                int shift = 0;

                for (int i = 0; i < 5; ++i) {
                    std::uint8_t byte = read_uint8();
                    value |= static_cast<std::uint32_t>(byte & 0x7f) << shift;

                    if ((byte & 0x80) == 0) {
                        return value;
                    }

                    shift += 7;
                }

                throw std::runtime_error("MetadataReader::read_unsigned_varint: too long");
            }

            std::string read_compact_string() {
                std::uint32_t encoded_size = read_unsigned_varint();
                if (encoded_size == 0) {
                    return {};
                }

                auto bytes = read_bytes(encoded_size - 1);
                return std::string(bytes.begin(), bytes.end());
            }

            std::array<std::uint8_t, 16> read_uuid() {
                std::array<std::uint8_t, 16> uuid{};
                for (auto& byte : uuid) {
                    byte = read_uint8();
                }
                return uuid;
            }

            std::vector<std::int32_t> read_compact_int32_array() {
                std::uint32_t encoded_size = read_unsigned_varint();
                if (encoded_size == 0) {
                    return {};
                }

                std::vector<std::int32_t> values;
                values.reserve(encoded_size - 1);
                for (std::uint32_t i = 0; i < encoded_size - 1; ++i) {
                    values.push_back(read_int32());
                }
                return values;
            }

            void read_tag_buffer() {
                std::uint32_t count = read_unsigned_varint();
                for (std::uint32_t i = 0; i < count; ++i) {
                    read_unsigned_varint();
                    auto size = read_unsigned_varint();
                    skip(size);
                }
            }

            std::vector<char> read_bytes(std::size_t size) {
                ensure(size);
                std::vector<char> bytes(_data.begin() + static_cast<std::ptrdiff_t>(_position),
                                        _data.begin() + static_cast<std::ptrdiff_t>(_position + size));
                _position += size;
                return bytes;
            }

            void skip(std::size_t size) {
                ensure(size);
                _position += size;
            }

        private:
            void ensure(std::size_t size) const {
                if (_data.size() - _position < size) {
                    throw std::out_of_range("MetadataReader: out of range");
                }
            }

            std::uint8_t read_uint8() {
                ensure(1);
                return static_cast<std::uint8_t>(_data[_position++]);
            }

            const std::vector<char>& _data;
            std::size_t _position = 0;
    };

    void parse_topic_record(MetadataReader& reader, std::unordered_map<std::string, TopicMetadata>& topics_by_uuid) {
        TopicMetadata topic;
        topic.name = reader.read_compact_string();
        topic.topic_id = reader.read_uuid();
        reader.read_tag_buffer();
        topics_by_uuid[uuid_key(topic.topic_id)] = topic;
    }

    void parse_partition_record(
        MetadataReader& reader,
        std::unordered_map<std::string, std::vector<PartitionMetadata>>& partitions_by_uuid
    ) {
        PartitionMetadata partition;
        partition.partition_index = reader.read_int32();
        auto topic_id = reader.read_uuid();
        partition.replica_nodes = reader.read_compact_int32_array();
        partition.isr_nodes = reader.read_compact_int32_array();
        reader.read_compact_int32_array();
        reader.read_compact_int32_array();
        partition.leader_id = reader.read_int32();
        partition.leader_epoch = reader.read_int32();
        reader.read_int32();

        partitions_by_uuid[uuid_key(topic_id)].push_back(partition);
    }

    void parse_metadata_record(
        const std::vector<char>& value,
        std::unordered_map<std::string, TopicMetadata>& topics_by_uuid,
        std::unordered_map<std::string, std::vector<PartitionMetadata>>& partitions_by_uuid
    ) {
        MetadataReader reader(value);
        reader.read_unsigned_varint();
        auto record_type = reader.read_unsigned_varint();
        reader.read_unsigned_varint();

        if (record_type == 2) {
            parse_topic_record(reader, topics_by_uuid);
        } else if (record_type == 3) {
            parse_partition_record(reader, partitions_by_uuid);
        }
    }

    void parse_record_batch(
        MetadataReader& reader,
        std::unordered_map<std::string, TopicMetadata>& topics_by_uuid,
        std::unordered_map<std::string, std::vector<PartitionMetadata>>& partitions_by_uuid
    ) {
        if (reader.remaining() < 12) {
            reader.seek(reader.position() + reader.remaining());
            return;
        }

        reader.read_int64();
        std::int32_t batch_length = reader.read_int32();
        if (batch_length <= 0 || static_cast<std::size_t>(batch_length) > reader.remaining()) {
            reader.seek(reader.position() + reader.remaining());
            return;
        }

        std::size_t batch_end = reader.position() + static_cast<std::size_t>(batch_length);

        reader.read_int32();
        reader.read_int8();
        reader.read_int32();
        auto attributes = reader.read_int16();
        reader.read_int32();
        reader.read_int64();
        reader.read_int64();
        reader.read_int64();
        reader.read_int16();
        reader.read_int32();

        std::int32_t record_count = reader.read_int32();
        if ((attributes & 0x0007) != 0) {
            reader.seek(batch_end);
            return;
        }

        for (std::int32_t i = 0; i < record_count && reader.position() < batch_end; ++i) {
            std::int32_t record_length = reader.read_varint();
            if (record_length < 0 || static_cast<std::size_t>(record_length) > batch_end - reader.position()) {
                reader.seek(batch_end);
                return;
            }

            std::size_t record_end = reader.position() + static_cast<std::size_t>(record_length);
            reader.read_int8();
            reader.read_varlong();
            reader.read_varint();

            std::int32_t key_length = reader.read_varint();
            if (key_length > 0) {
                reader.skip(static_cast<std::size_t>(key_length));
            }

            std::int32_t value_length = reader.read_varint();
            if (value_length > 0) {
                auto value = reader.read_bytes(static_cast<std::size_t>(value_length));
                parse_metadata_record(value, topics_by_uuid, partitions_by_uuid);
            }

            reader.seek(record_end);
        }

        reader.seek(batch_end);
    }
}

ClusterMetadata ClusterMetadata::read_from_default_path() {
    return read_from_path(DEFAULT_METADATA_LOG_PATH);
}

ClusterMetadata ClusterMetadata::read_from_path(const std::string& path) {
    ClusterMetadata metadata;
    std::ifstream file(path, std::ios::binary);
    if (!file) {
        return metadata;
    }

    std::vector<char> data((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    MetadataReader reader(data);
    std::unordered_map<std::string, TopicMetadata> topics_by_uuid;
    std::unordered_map<std::string, std::vector<PartitionMetadata>> partitions_by_uuid;

    while (!reader.empty()) {
        try {
            parse_record_batch(reader, topics_by_uuid, partitions_by_uuid);
        } catch (const std::exception&) {
            break;
        }
    }

    for (auto& [topic_id, topic] : topics_by_uuid) {
        if (auto partitions = partitions_by_uuid.find(topic_id); partitions != partitions_by_uuid.end()) {
            topic.partitions = partitions->second;
            std::sort(topic.partitions.begin(), topic.partitions.end(), [](const auto& lhs, const auto& rhs) {
                return lhs.partition_index < rhs.partition_index;
            });
        }
        metadata._topics.push_back(topic);
    }

    return metadata;
}

const TopicMetadata* ClusterMetadata::find_topic(const std::string& name) const {
    for (const auto& topic : _topics) {
        if (topic.name == name) {
            return &topic;
        }
    }
    return nullptr;
}

} // namespace kafka
