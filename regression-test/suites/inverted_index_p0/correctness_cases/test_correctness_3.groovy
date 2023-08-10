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

// File: /Users/luennng/develop/workspace/elasticsearch/rest-api-spec/src/yamlRestTest/resources/rest-api-spec/test/search/110_field_collapsing.yml
suite("test_correctness_3") {
    def tableName = "test_correctness_3"

    // create table
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName}(numeric_group INT,tag VARCHAR(500),sort INT, index index_numeric_group (numeric_group) using inverted,index index_tag (tag) using inverted properties("parser"="english"), index index_sort (sort) using inverted) DISTRIBUTED BY HASH(numeric_group) BUCKETS 1 PROPERTIES("replication_num" = "1")
        
        """

    // insert data
    sql """ INSERT INTO ${tableName} (numeric_group, tag, sort) VALUES (1, "A", 10);
        """

    sql """ INSERT INTO ${tableName} (numeric_group, tag, sort) VALUES (1, "B", 6);
        """

    sql """ INSERT INTO ${tableName} (numeric_group, tag, sort) VALUES (1, "A", 24);
        """

    sql """ INSERT INTO ${tableName} (numeric_group, tag, sort) VALUES (25, "B", 10);
        """

    sql """ INSERT INTO ${tableName} (numeric_group, tag, sort) VALUES (25, "A", 5);
        """

    sql """ INSERT INTO ${tableName} (numeric_group, tag, sort) VALUES (3, "B", 36);
        """

    // test select
//    qt_select1 """ select numeric_group, tag, sort from ( select numeric_group, tag, sort, row_number() over(partition by numeric_group order by sort desc) as rank from ${tableName})a where a.rank=1 order by sort, numeric_group, tag; """
//    qt_select2 """ select numeric_group, tag, sort from ( select numeric_group, tag, sort, row_number() over(partition by numeric_group order by sort desc) as rank from ${tableName})a where a.rank=1 order by sort desc, numeric_group, tag limit 2,100; """
    qt_select """ select numeric_group, count() from ${tableName} where sort >= 10  group by numeric_group order by numeric_group; """
    qt_select """ select numeric_group, count() from ${tableName} where tag match_any 'a b'  group by numeric_group order by numeric_group; """
}
