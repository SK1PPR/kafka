#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace kafka {

struct PartitionMetadata {
    std::int32_t partition_index;
    std::int32_t leader_id;
    std::int32_t leader_epoch;
    std::vector<std::int32_t> replica_nodes;
    std::vector<std::int32_t> isr_nodes;
};

struct TopicMetadata {
    std::string name;
    std::array<std::uint8_t, 16> topic_id{};
    std::vector<PartitionMetadata> partitions;
};

class ClusterMetadata {
    public:
        static ClusterMetadata read_from_default_path();
        static ClusterMetadata read_from_path(const std::string& path);

        const TopicMetadata* find_topic(const std::string& name) const;

    private:
        std::vector<TopicMetadata> _topics;
};

} // namespace kafka
