#ifndef ASOF_JOIN_BTREE_HPP
#define ASOF_JOIN_BTREE_HPP

#include <iostream>
#include <optional>
#include <vector>

#include "asof_join.hpp"

#include "tbb/enumerable_thread_specific.h"
#include "tbb/parallel_for.h"
#include "tbb/parallel_for_each.h"

template<typename Entry,
    typename=std::enable_if<std::is_base_of<ASOFJoin::JoinEntry, Entry>::value>::type>
class Btree {
    using KeyT = uint64_t;

public:
    explicit Btree(std::vector<Entry>& data) {
        auto nodes = build_leaf_nodes(data);
        //std::cout << "After build leaf node" << std::endl;
        while (nodes.size() > 1) {
            //std::cout << "Build inner nodes" << std::endl;
            nodes = build_inner_nodes(nodes);
        }
        root = nodes.front();
    }

    ~Btree() { /*delete_tree(root);*/ }

    std::optional<Entry> find_less_equal_than(KeyT target) {
        Node* node = root;
        while (node && !node->is_leaf()) {
            auto* inner = static_cast<InnerNode*>(node);
            node = inner->lower_bound(target);
        }

        if (!node) {
            return std::nullopt;
        }

        auto* leaf = static_cast<LeafNode*>(node);
        return leaf->lower_bound(target);
    }

    std::optional<Entry> find_greater_equal_than(KeyT target) {
        Node* node = root;
        while (node && !node->is_leaf()) {
            auto* inner = static_cast<InnerNode*>(node);
            node = inner->upper_bound(target);
        }

        if (!node) {
            return std::nullopt;
        }

        auto* leaf = static_cast<LeafNode*>(node);
        return leaf->upper_bound(target);
    }

    void print_tree() {
        print_tree(root);
    }

private:
    struct Node; struct InnerNode; struct LeafNode;
    static constexpr size_t capacity = 32;
    Node* root;

    struct Node {
        size_t level;
        size_t count;

        Node(size_t level, size_t count): level(level), count(count) {}

        virtual ~Node() = default;

        [[nodiscard]] inline bool is_leaf() const { return level == 0; }

        virtual KeyT get_max_key() = 0;
    };

    struct InnerNode: Node {
        std::array<KeyT, capacity> keys;
        std::array<Node*, capacity + 1> children;

        explicit InnerNode(size_t level): Node(level, /* count= */ 0), keys(), children() {}

        ~InnerNode() override = default;

        inline KeyT get_max_key() override {
            return children[this->count]->get_max_key();
        }

        Node* lower_bound(const KeyT& target) {
            auto end = keys.begin() + this->count;
            for (auto iter = keys.begin(); iter != end; ++iter) {
                if (*iter >= target) {
                    return children[iter - keys.begin()];
                }
            }
            return children[end - keys.begin()];

            //auto iter = std::lower_bound(
            //    keys.begin(),
            //    keys.begin() + this->count,
            //    target,
            //    [](KeyT a, KeyT b) {
            //        return a <= b;
            //    });

            //return children[iter - keys.begin()];
        }

        Node* upper_bound(const KeyT& target) {
            auto end = keys.begin() + this->count;
            auto iter = std::lower_bound(
                keys.begin(),
                end,
                target);

            return children[iter - keys.begin()];
        }
    };

    struct LeafNode: Node {
        std::vector<Entry> data;

        LeafNode(): Node(/* level= */ 0, /* count= */ 0), data() {
            data.reserve(capacity);
        }

        ~LeafNode() override = default;

        inline KeyT get_max_key() override {
            return (data.begin() + this->count - 1)->getKey();
        }

        std::optional<Entry> lower_bound(const KeyT& target) {
            //std::cout << "Leaf lower bound" << std::endl;
            //std::cout << "Data size: " << data.size() << std::endl;
            //std::cout << "Count: " << this->count << std::endl;
            //std::cout << "Search for target " << target << std::endl;
            //std::cout << "Data[0]: " << data[0].str() << std::endl;
            auto end = data.begin() + this->count;
            for (auto iter = data.begin(); iter != end; ++iter) {
                if (iter->getKey() > target) {
                    return iter != data.begin()
                        ? std::optional(*--iter)
                        : std::nullopt;
                }
            }
            auto last_elem = *--end;
            return last_elem.getKey() <= target
                ? std::optional(last_elem)
                : std::nullopt;
        }

        std::optional<Entry> upper_bound(const KeyT& target) {
            //std::cout << "Leaf upper bound bound" << std::endl;
            //std::cout << "Data size: " << data.size() << std::endl;
            //std::cout << "Count: " << this->count << std::endl;
            //std::cout << "Search for target " << target << std::endl;
            //std::cout << "Data[0]: " << data[0].str() << std::endl;
            //std::cout << "Max: " << (data.begin() + this->count - 1)->str() << std::endl;

            auto end = data.begin() + this->count;
            for (auto iter = end; iter != data.begin(); --iter) {
                auto& curr = *(iter - 1);
                if (curr.getKey() < target) {
                    return iter != end
                        ? std::optional(*iter)
                        : std::nullopt;
                }
            }
            auto first_elem = *data.begin();
            return first_elem.getKey() >= target
                ? std::optional(first_elem)
                : std::nullopt;
        }
    };

    std::vector<Node*> build_leaf_nodes(std::vector<Entry>& entries) {
        std::vector<Node*> leaves;
        for (size_t i = 0; i < entries.size(); i += capacity) {
            auto* leaf = new LeafNode();
            size_t end = std::min(i + capacity, entries.size());
            leaf->data.assign(entries.begin() + i, entries.begin() + end);
            leaf->count = end - i;
            leaves.push_back(leaf);
        }
        return leaves;
    }

    std::vector<Node*> build_inner_nodes(std::vector<Node*>& children) {
        std::vector<Node*> parents;
        size_t next_level = children[0]->level + 1;
        for (size_t i = 0; i < children.size(); i += capacity) {
            auto* parent = new InnerNode(next_level);
            size_t end = std::min(i + capacity, children.size());
            std::copy(
                children.begin() + i,
                children.begin() + end,
                parent->children.begin());

            size_t count = end - i;
            parent->count = count - 1;

            for (size_t j = 0; j < parent->count; ++j) {
                parent->keys[j] = children[i + j]->get_max_key();
            }

            parents.push_back(parent);
        }
        return parents;
    }

    void print_tree(Node* node) {
        if (!node) {
            return;
        }

        if (node->is_leaf()) {
            auto* leaf = static_cast<LeafNode*>(node);
            std::cout << "Leaf Node: " << std::endl;

            for (auto& e : leaf->data) {
                std::cout << e.str() << ", ";
            }
            std::cout << std::endl;

        }  else {
            auto* inner = static_cast<InnerNode*>(node);
            std::cout << "Inner Node" << std::endl;

            for (size_t i = 0; i < inner->count; ++i) {
                std::cout << "key: " << inner->keys[i] << ", ";
            }
            std::cout << std::endl;

            for (size_t i = 0; i < inner->count + 1; ++i) {
                print_tree(inner->children[i]);
            }
        }
    }

    void delete_tree(Node* node) {
        if (!node) {
            return;
        }

        if (!node->is_leaf()) {
            auto* inner = static_cast<InnerNode*>(node);
            for (size_t i = 0; i < inner->count + 1; ++i) {
                delete_tree(inner->children[i]);
            }
        }

        delete node;
    }
};

#endif // ASOF_JOIN_BTREE_HPP
