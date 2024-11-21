#ifndef ASOF_JOIN_BTREE_HPP
#define ASOF_JOIN_BTREE_HPP

#include <iostream>
#include <optional>
#include <vector>

#include "tbb/enumerable_thread_specific.h"
#include "tbb/parallel_for.h"
#include "tbb/parallel_for_each.h"

template<typename Key, typename Value>
class Btree {

    Btree(std::vector<Key>& keys, std::vector<Value>& values) {
        root = create_tree_from_sorted_data(keys, values);
    }

    ~Btree() { delete_tree(root); }

    std::optional<std::pair<Key, Value>> find_less_equal_than(Key& target) {
        Node* node = root;
        while (!node->is_leaf()) {
            InnerNode* inner = static_cast<InnerNode*>(node);
            auto opt_idx = inner->lower_bound(target);
            if (!opt_idx.has_value()) {
                return {};
            }
            node = inner->children[opt_idx.value()];
        }

        LeafNode* leaf = static_cast<LeafNode*>(node);
        auto opt_idx = leaf->lower_bound(target);
        return opt_idx.has_value()
            ? std::optional{leaf->keys[opt_idx.value()], leaf->values[opt_idx.value()]}
            : std::nullopt;
    }

    std::optional<std::pair<Key, Value>> find_greater_equal_than(Key& target) {
        Node* node = root;
        while (!node->is_leaf()) {
            InnerNode* inner = static_cast<InnerNode*>(node);
            auto opt_idx = inner->upper_bound(target);
            if (!opt_idx.has_value()) {
                return {};
            }
            node = inner->children[opt_idx.value()];
        }

        LeafNode* leaf = static_cast<LeafNode*>(node);
        auto opt_idx = leaf->upper_bound(target);
        return opt_idx.has_value()
            ? std::optional{leaf->keys[opt_idx.value()], leaf->values[opt_idx.value()]}
            : std::nullopt;
    }

private:
    static constexpr size_t capacity = 64;

    struct InnerNode;
    struct LeafNode;

    struct Node {
        size_t level;
        size_t count;
        std::array<Key, capacity> keys;

        Node(size_t level, size_t count): level(level), count(count) {}

        inline bool is_leaf() const { return level == 0; }

        inline Key& get_middle_key() {
            return keys[count / 2];
        }

        std::optional<size_t> lower_bound(Key& target) {
            auto iter = std::lower_bound(
                    this->keys.begin(),
                    this->keys.begin() + this->count, // TODO: Maybe - 1 here?
                    target);

            return iter != this->keys.end()
                ? std::optional(iter - this->keys.begin())
                : std::nullopt;
        }

        std::optional<size_t> upper_bound(Key& target) {
            auto iter = std::upper_bound(
                    this->keys.begin(),
                    this->keys.begin() + this->count, // TODO: Maybe - 1 here?
                    target);

            return iter != this->keys.begin()
                ? std::optional(iter - this->keys.begin())
                : std::nullopt;
        }
    };

    struct LeafNode: Node {
        std::array<Value, capacity> values;

        Node* next;
        Node* prev;

        LeafNode(): Node(0, 0) {}
    };

    struct InnerNode: Node {
        std::array<Node*, capacity + 1> children;

        explicit InnerNode(size_t level): Node(level, /* count= */ 0) {}
    };

    Node* root;

    std::vector<LeafNode*> build_leaf_nodes(std::vector<Key>& keys, std::vector<Value>& values) {
        std::vector<LeafNode*> leaves;
        for (size_t i = 0; i < keys.size(); i += capacity) {
            LeafNode leaf = new LeafNode();
            size_t end = std::min(i + capacity, keys.size());
            size_t count = end - i;
            std::memmove(
               /* dest= */ leaf.keys,
               /* src= */ keys.begin() + i,
               /* count= */ count * sizeof(Key));
            std::memmove(
               /* dest= */ leaf.values,
               /* src= */ values.begin() + i,
               /* count= */ count * sizeof(Value));
            leaf.count += count;
            leaves.push_back(&leaf);
        }
        return std::move(leaves);
    }

    std::vector<InnerNode*> build_inner_nodes(std::vector<Node*>& children) {
        std::vector<InnerNode*> parents;
        size_t next_level = children[0]->level;
        for (size_t i = 0; i < children.size(); i += capacity) {
            InnerNode parent = new InnerNode(next_level);
            size_t end = std::min(i + capacity, children.size());
            size_t count = end - i;
            std::copy(
                /* first= */ children.begin() + i,
                /* last= */ children.begin() + end,
                /* dest= */ parent.children.begin());
            for (size_t j = i, pos = j - i; j < end; ++j) {
                parent.keys[pos] = children[i + j].get_middle_key();
            }
            parent.count += count;
            parents.push_back(std::move(parent));
        }
        return std::move(parents);
    }

    Node* create_tree_from_sorted_data(std::vector<Key>& keys, std::vector<Value>& values) {
        auto& nodes = build_leaf_nodes(keys, values);
        while (nodes.size() > 1) {
            nodes = build_inner_nodes(nodes);
        }
        return nodes.front();
    }

    void delete_tree(Node* node) {
        if (!node) {
            return;
        }
        if (!node->is_leaf()) {
            InnerNode* inner = static_cast<InnerNode*>(node);
            for (size_t i = 0; i < inner->count; ++i) {
                delete_tree(inner->children[i]);
            }
        }
        delete node;
    }
};

#endif // ASOF_JOIN_BTREE_HPP
