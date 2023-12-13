// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <fmt/core.h>
#include <parallel_hashmap/phmap.h>

#include <algorithm>
#include <memory>
#include <mutex>
#include <type_traits>
#include <vector>

#include "common/logging.h"
#include "olap/iterators.h"
#include "olap/olap_common.h"
#include "olap/tablet_schema.h"
#include "util/time.h"

namespace doris {

// The SchemaPBCache is utilized to cache pre-allocated data structures,
// eliminating the need for frequent allocation and deallocation during usage.
// This caching mechanism proves immensely advantageous, particularly in scenarios
// with high concurrency, where queries are executed simultaneously.
class SchemaPBCache : public LRUCachePolicy {
public:
    static SchemaPBCache* instance() { return _s_instance; }

    static void create_global_instance(size_t capacity);

    // get cache schema key, delimiter with "-"
    static std::string get_schema_pb_key(int32_t tablet_id, const std::vector<TabletColumn>& columns,
                                      int32_t version);

    // Get a shared cached schema from cache, schema_meta_pb_key is a subset of column unique ids
    TabletSchemaPB* get_schema_pb(const std::string& schema_pb_key) {
        if (!_s_instance || schema_pb_key.empty()) {
            return {};
        }
        auto lru_handle = _cache->lookup(schema_pb_key);
        if (lru_handle) {
            Defer release([cache = _cache.get(), lru_handle] { cache->release(lru_handle); });
            auto value = (CacheValue*)_cache->value(lru_handle);
            value->last_visit_time = UnixMillis();
            VLOG_DEBUG << "get value->schema_pb, address:" << value->schema_pb;
            return value->schema_pb.get();
        }
        return {};
    }

    // Insert a shared Schema into cache, schema_meta_pb_key is full column unique ids
    void insert_schema_pb(const std::string& key, TabletSchemaPB* schema_pb) {
        if (!_s_instance || key.empty()) {
            return;
        }
        CacheValue* value = new CacheValue;
        value->last_visit_time = UnixMillis();
        value->schema_pb = std::make_shared<TabletSchemaPB>();
        value->schema_pb->CopyFrom(*schema_pb);
        VLOG_DEBUG << "set value->schema_pb, address:" << value->schema_pb;
        auto deleter = [](const doris::CacheKey& key, void* value) {
            CacheValue* cache_value = (CacheValue*)value;
            delete cache_value;
        };
        auto lru_handle = _cache->insert(key, value, sizeof(CacheValue), deleter,
                                         CachePriority::NORMAL, schema_pb->ByteSizeLong());
        _cache->release(lru_handle);
    }

    // Try to prune the cache if expired.
    Status prune();

    struct CacheValue : public LRUCacheValueBase {
        std::shared_ptr<TabletSchemaPB> schema_pb = nullptr;
    };

private:
    SchemaPBCache(size_t capacity)
            : LRUCachePolicy("SchemaPBCache", capacity, LRUCacheType::NUMBER,
                             config::schema_cache_sweep_time_sec) {}
    static SchemaPBCache* _s_instance;
    google::protobuf::Arena _arena;
};
} // namespace doris