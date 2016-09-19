/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.lang.common.visitor.base;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.clause.UpdateClause;
import org.apache.asterix.lang.common.expression.OrderedListTypeDefinition;
import org.apache.asterix.lang.common.expression.RecordTypeDefinition;
import org.apache.asterix.lang.common.expression.TypeReferenceExpression;
import org.apache.asterix.lang.common.expression.UnorderedListTypeDefinition;
import org.apache.asterix.lang.common.statement.*;

public abstract class AbstractQueryExpressionVisitor<R, T> implements ILangVisitor<R, T> {

    @Override
    public R visit(CreateIndexStatement cis, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(DataverseDecl dv, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(DeleteStatement del, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(DropStatement del, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(DatasetDecl dd, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(InsertStatement insert, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(LoadStatement stmtLoad, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(NodegroupDecl ngd, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(OrderedListTypeDefinition olte, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(RecordTypeDefinition tre, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(SetStatement ss, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(TypeDecl td, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(TypeReferenceExpression tre, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(UnorderedListTypeDefinition ulte, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(UpdateClause del, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(UpdateStatement update, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(WriteStatement ws, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(CreateDataverseStatement del, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(IndexDropStatement del, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(NodeGroupDropStatement del, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(DataverseDropStatement del, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(TypeDropStatement del, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(DisconnectFeedStatement del, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(CreateFunctionStatement cfs, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(FunctionDropStatement del, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(CreateFeedStatement del, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(ConnectFeedStatement del, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(StartFeedStatement ssfs, T arg) throws AsterixException{
        return null;
    }

    @Override
    public R visit(FeedDropStatement del, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(CompactStatement del, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(CreateFeedPolicyStatement cfps, T arg) throws AsterixException {
        return null;
    }

    @Override
    public R visit(FeedPolicyDropStatement dfs, T arg) throws AsterixException {
        return null;
    }
}
