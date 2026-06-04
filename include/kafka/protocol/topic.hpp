#pragma once

#include <string>
#include <cstdint>
#include <vector>

namespace kafka {
    namespace protocol {

        struct TopicPartition {
            std::int32_t partition_index;
            std::int32_t leader_id;
            std::int32_t leader_epoch;
            std::vector<std::int32_t> replica_nodes;
            std::vector<std::int32_t> isr_nodes;
            std::vector<std::int32_t> eligible_leader_replicas;
            std::vector<std::int32_t> last_known_elr;
            std::vector<std::int32_t> offline_replicas;
        };
        
        struct Topic {
            std::string name;
            // Topic id is 128 bits
            std::int64_t _lower_topic_id;
            std::int64_t _higher_topic_id;
            bool is_internal;
            std::int32_t topic_authorized_operations;
            std::vector<TopicPartition> partitions;
        };

    }
}