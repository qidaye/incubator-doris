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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/LikePredicate.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class LikePredicate extends Predicate {

    public enum Operator {
        LIKE("LIKE"),
        REGEXP("REGEXP");

        private final String description;

        Operator(String description) {
            this.description = description;
        }

        @Override
        public String toString() {
            return description;
        }
    }

    public static void initBuiltins(FunctionSet functionSet) {
        functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltin(
                Operator.LIKE.name(), Type.BOOLEAN, Lists.<Type>newArrayList(Type.VARCHAR, Type.VARCHAR),
                false,
                "_ZN5doris13LikePredicate4likeEPN9doris_udf15FunctionContextERKNS1_9StringValES6_",
                "_ZN5doris13LikePredicate12like_prepareEPN9doris_udf15FunctionContextENS2_18FunctionStateScopeE",
                "_ZN5doris13LikePredicate10like_closeEPN9doris_udf15FunctionContextENS2_18FunctionStateScopeE", true));
        functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltin(
                Operator.REGEXP.name(), Type.BOOLEAN, Lists.<Type>newArrayList(Type.VARCHAR, Type.VARCHAR),
                false,
                "_ZN5doris13LikePredicate5regexEPN9doris_udf15FunctionContextERKNS1_9StringValES6_",
                "_ZN5doris13LikePredicate13regex_prepareEPN9doris_udf15FunctionContextENS2_18FunctionStateScopeE",
                "_ZN5doris13LikePredicate11regex_closeEPN9doris_udf15FunctionContextENS2_18FunctionStateScopeE", true));
    }

    @SerializedName("op")
    private Operator op;

    private LikePredicate() {
        // use for serde only
    }

    public LikePredicate(Operator op, Expr e1, Expr e2) {
        super();
        this.op = op;
        Preconditions.checkNotNull(e1);
        children.add(e1);
        Preconditions.checkNotNull(e2);
        children.add(e2);
        // TODO: improve with histograms?
        selectivity = 0.1;
    }

    protected LikePredicate(LikePredicate other) {
        super(other);
        op = other.op;
    }

    @Override
    public Expr clone() {
        return new LikePredicate(this);
    }

    public Operator getOp() {
        return this.op;
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        return ((LikePredicate) obj).op == op;
    }

    @Override
    public String toSqlImpl() {
        return getChild(0).toSql() + " " + op.toString() + " " + getChild(1).toSql();
    }

    @Override
    public String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf table) {
        return getChild(0).toSql(disableTableName, needExternalSql, tableType, table) + " " + op.toString() + " "
                + getChild(1).toSql(disableTableName, needExternalSql, tableType, table);
    }

    @Override
    public String toDigestImpl() {
        return getChild(0).toDigest() + " " + op.toString() + " " + getChild(1).toDigest();
    }


    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.FUNCTION_CALL;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(op);
    }
}
