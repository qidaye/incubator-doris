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

// File: /Users/luennng/develop/workspace/elasticsearch/rest-api-spec/src/yamlRestTest/resources/rest-api-spec/test/search/170_terms_query.yml
suite("test_correctness_1") {
    def tableName = "test_correctness_1"

    // create table
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
            user VARCHAR(500),
            followers ARRAY<STRING>,
            index index_user (user) using inverted properties("parser"="english"),
            index index_followers (followers) using inverted properties("parser"="english")) 
        DISTRIBUTED BY HASH(user) BUCKETS 1 
        PROPERTIES("replication_num" = "1")
        """

    // insert data
    sql """ INSERT INTO ${tableName} VALUES
         ("u1", ["u2","u3"])
        """

    sql """ INSERT INTO ${tableName} VALUES
         ("u2", ["u1","u3","u4"])
        """

    sql """ INSERT INTO ${tableName} VALUES
         ("u3", ["u1"])
        """

    sql """ INSERT INTO ${tableName} VALUES
         ("u4", ["u3"])
        """

    // test select
    qt_select """ SELECT * FROM ${tableName} WHERE user MATCH_ANY 'u1, u2' ORDER BY user LIMIT 10; """
    qt_select """ SELECT * FROM ${tableName} WHERE user MATCH_ANY 'u2, u3' ORDER BY user LIMIT 10; """
    qt_select """ SELECT * FROM ${tableName} WHERE user MATCH_ANY 'u1, u2, u3' ORDER BY user LIMIT 10; """
    qt_select """ SELECT * FROM ${tableName} WHERE user MATCH_ANY 'u1, u3, u4' ORDER BY user LIMIT 10; """
}
