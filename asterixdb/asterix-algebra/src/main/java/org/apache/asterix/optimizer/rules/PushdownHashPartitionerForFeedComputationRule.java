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
package org.apache.asterix.optimizer.rules;

import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.FeedDataSource;
import org.apache.asterix.metadata.entities.FeedConnection;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AssignPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.InsertDeleteUpsertPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.RandomPartitionExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.DefaultNodeGroupDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

import java.util.Arrays;

public class PushdownHashPartitionerForFeedComputationRule implements IAlgebraicRewriteRule {

    @Override public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op0 = opRef.getValue();
        if (!op0.getOperatorTag().equals(LogicalOperatorTag.INSERT_DELETE_UPSERT)) {
            return false;
        }

        ILogicalOperator op1 = op0.getInputs().get(0).getValue();
        if (!op1.getOperatorTag().equals(LogicalOperatorTag.EXCHANGE)) {
            return false;
        }

        ILogicalOperator op2 = op1.getInputs().get(0).getValue();
        if (!op2.getOperatorTag().equals(LogicalOperatorTag.ASSIGN)) {
            return false;
        }

        ILogicalOperator op3 = op2.getInputs().get(0).getValue();
        if (!op3.getOperatorTag().equals(LogicalOperatorTag.PROJECT)) {
            return false;
        }

        ILogicalOperator op4 = op3.getInputs().get(0).getValue();
        if (!op4.getOperatorTag().equals(LogicalOperatorTag.ASSIGN)) {
            return false;
        }

        ILogicalOperator op5 = op4.getInputs().get(0).getValue();
        if (!op5.getOperatorTag().equals(LogicalOperatorTag.DATASOURCESCAN)) {
            return false;
        }

        DataSourceScanOperator scanOp = (DataSourceScanOperator) op5;
        DataSource dataSource = (DataSource) scanOp.getDataSource();
        if (dataSource.getDatasourceType() != DataSource.Type.FEED) {
            return false;
        }

        FeedConnection feedConnection = ((FeedDataSource) dataSource).getFeedConnection();
        if (feedConnection.getAppliedFunctions() == null || feedConnection.getAppliedFunctions().size() == 0) {
            return false;
        }

        INodeDomain datasetDomain = ((InsertDeleteUpsertOperator) op0).getDataSource().getDomain();

        // if dataset partition is the same as compute partition, push hash partition down
        op0.getInputs().get(0).setValue(op3);
        op2.getInputs().get(0).setValue(op5);
        op4.getInputs().get(0).setValue(op1);

        // replace variable
        AssignOperator extractKeyAssignOp = (AssignOperator) op2;
        extractKeyAssignOp.recomputeSchema();
        extractKeyAssignOp.getExpressions().get(0).getValue().substituteVar(op3.getSchema().get(0), op5.getSchema().get(0));
        ((ProjectOperator) op3).getVariables().add(extractKeyAssignOp.getVariables().get(0));

        // assign location constraint for udf assignOp
        AssignOperator udfAssignOp = (AssignOperator) op4;
        ((AssignPOperator) udfAssignOp.getPhysicalOperator())
                .setLocationConstraint(((DefaultNodeGroupDomain) datasetDomain).getNodes());
        return true;
    }

    @Override public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

}
