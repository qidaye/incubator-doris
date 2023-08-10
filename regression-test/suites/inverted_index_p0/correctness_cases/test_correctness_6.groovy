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

// File: /Users/luennng/develop/workspace/elasticsearch/rest-api-spec/src/yamlRestTest/resources/rest-api-spec/test/search/190_index_prefix_search.yml
suite("test_correctness_6") {
    def tableName = "test_correctness_6"

    // create table
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName}(text VARCHAR(500),index index_text (text) using inverted properties("parser"="english")) DISTRIBUTED BY HASH(text) BUCKETS 1 PROPERTIES("replication_num" = "1");
        """

    // insert data
    sql """ INSERT INTO ${tableName} (text) VALUES ("some short words with a stupendously long one");
        """

    // test select
    qt_select """ select * from ${tableName} where text match_any 'shor' """
    qt_select """ select * from ${tableName} where text match_any 'a' """
    qt_select """ select * from ${tableName} where text match_any 'word' """
}
