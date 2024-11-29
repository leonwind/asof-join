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
    struct Node; struct InnerNode; struct LeafNode;
    using KeyT = uint64_t;
    using iterator = std::vector<LeafNode*>::iterator;

public:
    explicit Btree(std::vector<Entry>& data) {
        auto nodes = build_leaf_nodes(data);
        while (nodes.size() > 1) {
            nodes = build_inner_nodes(nodes);
        }
        root = nodes.front();
    }

    ~Btree() { /*delete_tree(root);*/ }

    Entry* find_less_equal_than(KeyT target) {
        Node* node = root;
        while (node && !node->is_leaf()) {
            auto* inner = static_cast<InnerNode*>(node);
            node = inner->get_closest_child_smaller_equal(target);
        }

        if (!node) {
            return nullptr;
        }

        auto* leaf = static_cast<LeafNode*>(node);
        return leaf->get_closest_entry_less_equal(target);
    }

    Entry* find_greater_equal_than(KeyT target) {
        Node* node = root;
        while (node && !node->is_leaf()) {
            auto* inner = static_cast<InnerNode*>(node);
            node = inner->get_closest_child_greater_equal(target);
        }

        if (!node) {
            return nullptr;
        }

        auto* leaf = static_cast<LeafNode*>(node);
        return leaf->get_closest_entry_greater_equal(target);
    }

    void print_tree() {
        print_tree(root);
    }

    [[nodiscard]] inline iterator leaves_begin() {
        return leaf_nodes.begin();
    }

    [[nodiscard]] inline iterator leaves_end() {
        return leaf_nodes.end();
    }

    [[nodiscard]] size_t num_leaves() const {
        return leaf_nodes.size();
    }

    [[nodiscard]] inline std::vector<Entry>& operator[](size_t idx) {
        return leaf_nodes[idx]->data;
    }

private:
    static constexpr size_t capacity = 32;
    Node* root;
    std::vector<LeafNode*> leaf_nodes;

    struct Node {
        size_t level;
        size_t count;

        Node(size_t level, size_t count): level(level), count(count) {}

        virtual ~Node() = default;

        [[nodiscard]] inline bool is_leaf() const { return level == 0; }

        virtual KeyT get_min_key() = 0;
    };

    struct InnerNode: Node {
        std::array<KeyT, capacity> keys;
        std::array<Node*, capacity + 1> children;

        explicit InnerNode(size_t level): Node(level, /* count= */ 0), keys(), children() {}

        ~InnerNode() override = default;

        inline KeyT get_min_key() override {
            return children[0]->get_min_key();
        }

        Node* get_closest_child_smaller_equal(const KeyT& target) {
            auto end = keys.begin() + this->count;
            auto iter = std::upper_bound(keys.begin(), end, target);
            return children[iter - keys.begin()];
        }

        Node* get_closest_child_greater_equal(const KeyT& target) {
            auto end = keys.begin() + this->count;
            auto iter = std::lower_bound(keys.begin(), end, target);
            return children[iter - keys.begin()];
        }
    };

    struct LeafNode: Node {
        std::vector<Entry> data;
        LeafNode* prev;
        LeafNode* next;

        LeafNode(): Node(/* level= */ 0, /* count= */ 0), data(), prev(nullptr), next(nullptr) {
            data.reserve(capacity);
        }

        ~LeafNode() override = default;

        inline KeyT get_min_key() override {
            return data[0].get_key();
        }

        Entry* get_closest_entry_less_equal(const KeyT& target) {
            auto end = data.begin() + this->count;
            auto iter = std::upper_bound(data.begin(), end, target,
                [](const KeyT& value, const Entry& entry) {
                    return value < entry.get_key();
            });

            return iter != data.begin()
                ? &*--iter
                : nullptr;
        }

        Entry* get_closest_entry_greater_equal(const KeyT& target) {
            auto iter = std::lower_bound(data.begin(), data.end(), target,
                [](const Entry& entry, const KeyT& value) {
                    return entry.get_key() < value;
            });

            if (iter != data.end()) {
                return &*iter;
            } else if (next != nullptr) {
                return next->get_closest_entry_greater_equal(target);
            }

            return nullptr;
            }
    };

    std::vector<Node*> build_leaf_nodes(std::vector<Entry>& entries) {
        std::vector<Node*> leaves;
        LeafNode* prev_leaf = nullptr;
        for (size_t i = 0; i < entries.size(); i += capacity) {
            auto* curr_leaf = new LeafNode();
            size_t end = std::min(i + capacity, entries.size());
            curr_leaf->data.assign(entries.begin() + i, entries.begin() + end);
            curr_leaf->count = end - i;
            if (prev_leaf != nullptr) {
                prev_leaf->next = curr_leaf;
                curr_leaf->prev = prev_leaf;
            }
            prev_leaf = curr_leaf;

            leaves.push_back(curr_leaf);
            leaf_nodes.push_back(curr_leaf);
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
                parent->keys[j] = parent->children[j + 1]->get_min_key();
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
