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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_first_delete_compaction", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }
    def tableName = "test_first_delete_compaction"
    def check_cumu_point = { cumu_point ->
        def tablets = sql_return_maparray """ show tablets from ${tableName}; """
        int cumuPoint = 0
        def tablet = tablets[0]
        String tablet_id = tablet.TabletId
        def (code, out, err) = curl("GET", tablet.CompactionStatus)
        logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def tabletJson = parseJson(out.trim())
        cumuPoint = tabletJson["cumulative point"]
        return cumuPoint > cumu_point
    }

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def set_be_config = { key, value ->
        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
    }
    set_be_config.call("disable_auto_compaction", "true")
    GetDebugPoint().clearDebugPointsForAllBEs()
    GetDebugPoint().enableDebugPointForAllBEs("CloudCumulativeCompaction.prepare_compact.sleep")

    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE ${tableName}
            (
            `k1` INT NULL,
            `v1` INT NULL
            )
            UNIQUE KEY(k1)
            PARTITION BY RANGE(k1)
            (
                PARTITION p1 VALUES LESS THAN (10),
                PARTITION p2 VALUES LESS THAN (20)
            )
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "enable_mow_light_delete" = "true")"""

        sql """ INSERT INTO ${tableName} VALUES (11,11)"""
        sql """ set delete_without_partition = true; """
        sql """ delete from ${tableName} where v1 > 0"""
        sql """ INSERT INTO ${tableName} VALUES (1,1)"""
        sql """ INSERT INTO ${tableName} VALUES (1,1)"""
        sql """ INSERT INTO ${tableName} VALUES (1,1)"""
        sql """ INSERT INTO ${tableName} VALUES (1,1)"""
        sql """ INSERT INTO ${tableName} VALUES (1,1)"""
        set_be_config.call("disable_auto_compaction", "false")

        def now = System.currentTimeMillis()

        while(true){
            sql """ INSERT INTO ${tableName} VALUES (1,1)"""
            sql """ INSERT INTO ${tableName} VALUES (1,1)"""
            sql """ INSERT INTO ${tableName} VALUES (1,1)"""
            sql """ INSERT INTO ${tableName} VALUES (1,1)"""
            sql """ INSERT INTO ${tableName} VALUES (1,1)"""
            if(check_cumu_point(3)){
                break;
            }
            Thread.sleep(3000)
            def diff = System.currentTimeMillis() - now
            if(diff > 300*1000){
                break
            }
        }
        def time_diff = System.currentTimeMillis() - now
        logger.info("time_diff:" + time_diff)
        assertTrue(time_diff<60*1000)

        qt_select1 """select * from ${tableName} order by k1, v1"""
    } catch (Exception e){
        logger.info(e.getMessage())
        assertFalse(true)
    } finally {
        set_be_config.call("disable_auto_compaction", "false")
        GetDebugPoint().disableDebugPointForAllBEs("CloudCumulativeCompaction.prepare_compact.sleep")
        try_sql("DROP TABLE IF EXISTS ${tableName} FORCE")
    }

}