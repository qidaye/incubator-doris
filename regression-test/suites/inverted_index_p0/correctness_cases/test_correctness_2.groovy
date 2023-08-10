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

// File: /Users/luennng/develop/workspace/elasticsearch/rest-api-spec/src/yamlRestTest/resources/rest-api-spec/test/search/115_multiple_field_collapsing.yml
suite("test_correctness_2") {
    def tableName = "test_correctness_2"

    // create table
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName}(country VARCHAR(500),city VARCHAR(500),address VARCHAR(500),index index_country (country) using inverted properties("parser"="english"),index index_city (city) using inverted properties("parser"="english"),index index_address (address) using inverted properties("parser"="english")) DISTRIBUTED BY HASH(country) BUCKETS 1 PROPERTIES("replication_num" = "1")

        """

    // insert data
    sql """ INSERT INTO ${tableName} (country, city, address) VALUES ("Canada", "Saskatoon", "701 Victoria Avenue")
        """

    sql """ INSERT INTO ${tableName} (country, city, address) VALUES ("Canada", "Toronto", "74 Victoria Street, Suite, 74 Victoria Street, Suite 300")
        """

    sql """ INSERT INTO ${tableName} (country, city, address) VALUES ("Canada", "Toronto", "350 Victoria St")
        """

    sql """ INSERT INTO ${tableName} (country, city, address) VALUES ("Canada", "Toronto", "20 Victoria Street")
        """

    sql """ INSERT INTO ${tableName} (country, city, address) VALUES ("UK", "London", "58 Victoria Street")
        """

    sql """ INSERT INTO ${tableName} (country, city, address) VALUES ("UK", "London", "Victoria Street Victoria Palace Theatre")
        """

    sql """ INSERT INTO ${tableName} (country, city, address) VALUES ("UK", "Manchester", "75 Victoria street Westminster")
        """

    sql """ INSERT INTO ${tableName} (country, city, address) VALUES ("UK", "London", "Victoria Station Victoria Arcade")
        """

    // test select
//    qt_select1 """ select country,city,address from (select country,city,address,row_number() over(partition by country,city order by city desc) as rank  from ${tableName} where address match 'victoria') a where a.rank = 1 order by country,city,address; """
//    qt_select2 """ select country,city,address from (select country,city,address,row_number() over(partition by country,city order by city desc) as rank  from ${tableName} where address match 'victoria') a where a.rank = 1 order by country,city desc,address; """
    qt_select """ select country,city from ${tableName} where address match 'victoria' group by country,city order by country,city """
    qt_select """ select country,city from ${tableName} where address match 'victoria' group by country,city order by country,city desc """
}
