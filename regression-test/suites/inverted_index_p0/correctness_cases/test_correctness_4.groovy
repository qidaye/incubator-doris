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

// File: /Users/luennng/develop/workspace/elasticsearch/rest-api-spec/src/yamlRestTest/resources/rest-api-spec/test/search/500_date_range.yml
suite("test_correctness_4") {
    def tableName1 = "test_correctness_4_dates_year_only"
    def tableName2 = "test_correctness_4_dates"

    // create table
    sql """ DROP TABLE IF EXISTS ${tableName1} """
    sql """ DROP TABLE IF EXISTS ${tableName2} """
    sql """
        CREATE TABLE ${tableName1}(date DATEV2, field INT, index index_date (date) using inverted, index index_field (field) using inverted) DISTRIBUTED BY HASH(date) BUCKETS 1 PROPERTIES("replication_num" = "1");
        """
    sql """
        CREATE TABLE ${tableName2}(date DATETIMEV2(6), field INT, index index_date (date) using inverted,  index index_field (field) using inverted) DISTRIBUTED BY HASH(date) BUCKETS 1 PROPERTIES("replication_num" = "1");
        """

    // insert data
    sql """ INSERT INTO ${tableName1} (date, field) VALUES ("1900-01-01", 1); """
    sql """ INSERT INTO ${tableName1} (date, field) VALUES ("2022-01-01", 2); """
    sql """ INSERT INTO ${tableName1} (date, field) VALUES ("2022-01-01", 3); """
    sql """ INSERT INTO ${tableName1} (date, field) VALUES ("1500-01-01", 4); """

    sql """ INSERT INTO ${tableName2} (date, field) VALUES ("1900-01-01T12:12:12.123456Z", 1); """
    sql """ INSERT INTO ${tableName2} (date, field) VALUES ("2022-01-01T12:12:12.123456Z", 2); """
    sql """ INSERT INTO ${tableName2} (date, field) VALUES ("2022-01-03T12:12:12.123456Z", 3); """
    sql """ INSERT INTO ${tableName2} (date, field) VALUES ("1500-01-01T12:12:12.123456Z", 4); """
    sql """ INSERT INTO ${tableName2} (date, field) VALUES ("1500-01-05T12:12:12.123456Z", 5); """


    // test select
    qt_select """ SELECT * FROM ${tableName2} WHERE date >= '1000-01-01' AND date <= '2023-01-01' ORDER BY field, date; """
    qt_select """ SELECT * FROM ${tableName2} WHERE date >= '1500-01-01' AND date <= '1500-01-02' ORDER BY field, date; """
    qt_select """ SELECT * FROM ${tableName2} WHERE date >= '1500-01-01' AND date <= '2000-01-01' ORDER BY field, date; """
    qt_select """ SELECT * FROM ${tableName1} WHERE date like '%1500%' ORDER BY field LIMIT 10; """
}
