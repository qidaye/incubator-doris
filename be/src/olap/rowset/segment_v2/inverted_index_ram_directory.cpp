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

#include "olap/rowset/segment_v2/inverted_index_ram_directory.h"

#include "CLucene/store/RAMDirectory.h"

#include <CLucene/clucene-config.h>
#include <CLucene/store/LockFactory.h>

namespace doris {
namespace segment_v2 {

DorisRAMDirectory::DorisRAMDirectory() {
    lucene::store::RAMDirectory();
}

void DorisRAMDirectory::close() {
    // do nothing here
    // we cache files in memory to write compound file.
    // directory will be closed after writing compound file done in #finish().
}

void DorisRAMDirectory::finish() {
    // do real dir close
    lucene::store::RAMDirectory::close();
}

} // namespace segment_v2
} // namespace doris
