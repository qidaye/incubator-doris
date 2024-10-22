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

#include "inverted_index_compaction.h"

#include "inverted_index_file_writer.h"
#include "inverted_index_fs_directory.h"
#include "io/fs/local_file_system.h"
#include "olap/tablet_schema.h"
#include "util/debug_points.h"

namespace doris::segment_v2 {
Status compact_column(int64_t index_id, std::vector<lucene::store::Directory*>& src_index_dirs,
                      std::vector<lucene::store::Directory*>& dest_index_dirs,
                      std::string_view tmp_path,
                      const std::vector<std::vector<std::pair<uint32_t, uint32_t>>>& trans_vec,
                      const std::vector<uint32_t>& dest_segment_num_rows) {
    DBUG_EXECUTE_IF("index_compaction_compact_column_throw_error", {
        if (index_id % 2 == 0) {
            _CLTHROWA(CL_ERR_IO, "debug point: test throw error in index compaction");
        }
    })
    DBUG_EXECUTE_IF("index_compaction_compact_column_status_not_ok", {
        if (index_id % 2 == 1) {
            return Status::Error<ErrorCode::INVERTED_INDEX_COMPACTION_ERROR>(
                    "debug point: index compaction error");
        }
    })
    // 1. 创建索引写入器：
    //   - 创建一个临时目录，用于存储中间结果。
    //   - 创建一个简单的分析器。
    //   - 创建一个索引写入器，用于执行索引压缩。
    bool can_use_ram_dir = true;
    lucene::store::Directory* dir = DorisFSDirectoryFactory::getDirectory(
            io::global_local_filesystem(), tmp_path.data(), can_use_ram_dir);
    lucene::analysis::SimpleAnalyzer<char> analyzer;
    auto* index_writer = _CLNEW lucene::index::IndexWriter(dir, &analyzer, true /* create */,
                                                           true /* closeDirOnShutdown */);
    // 2. 确保源索引目录的数量与转换向量的数量一致。
    DCHECK_EQ(src_index_dirs.size(), trans_vec.size());
    // 3. 执行索引压缩。
    index_writer->indexCompaction(src_index_dirs, dest_index_dirs, trans_vec,
                                  dest_segment_num_rows);
    // TODO 增加 debug_point 模拟 index_compaction 出错

    // 4. 关闭删除索引写入器。
    index_writer->close();
    _CLDELETE(index_writer);
    // 5. 处理目录引用计数
    //   - 减少目录的引用计数，当引用计数减少到 1 时，目录将被销毁。
    // NOTE: need to ref_cnt-- for dir,
    // when index_writer is destroyed, if closeDir is set, dir will be close
    // _CLDECDELETE(dir) will try to ref_cnt--, when it decreases to 1, dir will be destroyed.
    _CLDECDELETE(dir)
    // 6. 关闭并删除源索引目录
    for (auto* d : src_index_dirs) {
        if (d != nullptr) {
            d->close();
            _CLDELETE(d);
        }
    }
    // 7. 处理目标索引目录
    // 不关闭目标索引目录，因为它们将在目标索引写入器完成时关闭。
    for (auto* d : dest_index_dirs) {
        if (d != nullptr) {
            // NOTE: DO NOT close dest dir here, because it will be closed when dest index writer finalize.
            //d->close();
            //_CLDELETE(d);
        }
    }

    // 8. 删除临时目录：
    // delete temporary segment_path, only when inverted_index_ram_dir_enable is false
    if (!config::inverted_index_ram_dir_enable) {
        std::ignore = io::global_local_filesystem()->delete_directory(tmp_path.data());
    }
    return Status::OK();
}
} // namespace doris::segment_v2
