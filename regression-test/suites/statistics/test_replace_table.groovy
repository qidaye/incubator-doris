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

suite("test_replace_table") {

    sql """drop database if exists test_replace_table_statistics"""
    sql """create database test_replace_table_statistics"""
    sql """use test_replace_table_statistics"""
    sql """set global force_sample_analyze=false"""
    sql """set global enable_auto_analyze=false"""

    sql """CREATE TABLE t1 (
            t1key int NOT NULL,
            t1value varchar(25) NOT NULL
        )ENGINE=OLAP
        DUPLICATE KEY(`t1key`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`t1key`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """CREATE TABLE t2 (
            t2key int NOT NULL
        )ENGINE=OLAP
        DUPLICATE KEY(`t2key`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`t2key`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """insert into t1 values (1, "1"), (2, "2")"""
    sql """insert into t2 values (3)"""
    sql """analyze table t1 with sync"""
    sql """analyze table t2 with sync"""

    def result = sql """show column stats t1"""
    assertEquals(2, result.size())
    result = sql """show column cached stats t1"""
    assertEquals(2, result.size())
    result = sql """show column stats t2"""
    assertEquals(1, result.size())
    result = sql """show column cached stats t2"""
    assertEquals(1, result.size())

    sql """ALTER TABLE t1 REPLACE WITH TABLE t2;"""
    result = sql """show column stats t1"""
    assertEquals(1, result.size())
    result = sql """show column cached stats t1"""
    assertEquals(1, result.size())
    result = sql """show column stats t2"""
    assertEquals(2, result.size())
    result = sql """show column cached stats t2"""
    assertEquals(2, result.size())

    def id1 = get_table_id("internal", "test_replace_table_statistics", "t1")
    def id2 = get_table_id("internal", "test_replace_table_statistics", "t2")

    sql """ALTER TABLE t1 REPLACE WITH TABLE t2 PROPERTIES('swap' = 'false');"""
    result = sql """show column stats t1"""
    assertEquals(2, result.size())
    result = sql """show column cached stats t1"""
    assertEquals(2, result.size())

    result = sql """show table stats ${id1}"""
    assertEquals(0, result.size())
    result = sql """show table stats ${id2}"""
    assertEquals(1, result.size())

    sql """drop database if exists test_replace_table_statistics"""
}
