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
package org.apache.asterix.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.asterix.app.translator.DefaultStatementExecutorFactory;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.compiler.provider.SqlppCompilationProvider;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.feed.watch.FeedActivityDetails;
import org.apache.asterix.external.operators.FeedCallableOperatorDescriptor;
import org.apache.asterix.external.operators.FeedCollectOperatorDescriptor;
import org.apache.asterix.external.operators.FeedIntakeOperatorDescriptor;
import org.apache.asterix.external.operators.FeedMetaOperatorDescriptor;
import org.apache.asterix.external.operators.FeedIntakeOperatorNodePushable;
import org.apache.asterix.external.operators.DeployedJobPartitionHolderDescriptor;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.FeedConstants;
import org.apache.asterix.external.util.FeedUtils;
import org.apache.asterix.external.util.FeedUtils.FeedRuntimeType;
import org.apache.asterix.file.StorageComponentProvider;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.IntegerLiteral;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.statement.InsertStatement;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.statement.UpsertStatement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.parser.SqlppParserFactory;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedConnection;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.SessionOutput;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.common.data.partition.RandomPartitionComputerFactory;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.FileRemoveOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;

import static org.apache.asterix.external.util.FeedConstants.FEED_INTAKE_PARTITION_HOLDER;

/**
 * Provides helper method(s) for creating JobSpec for operations on a feed.
 */
public class FeedOperations {

    public static final String FEED_DATAFLOW_INTERMEIDATE_VAL_PREFIX = "val";

    private FeedOperations() {
    }

    private static Pair<JobSpecification, IAdapterFactory> buildFeedIntakeJobSpec(Feed feed,
            MetadataProvider metadataProvider, FeedPolicyAccessor policyAccessor) throws Exception {
        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        spec.setFrameSize(metadataProvider.getApplicationContext().getCompilerProperties().getFrameSize());
        IAdapterFactory adapterFactory;
        IOperatorDescriptor intakeOperator;
        AlgebricksPartitionConstraint ingesterPc;
        Triple<IOperatorDescriptor, AlgebricksPartitionConstraint, IAdapterFactory> t =
                metadataProvider.buildFeedIntakeRuntime(spec, feed, policyAccessor);
        intakeOperator = t.first;
        ingesterPc = t.second;
        adapterFactory = t.third;
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, intakeOperator, ingesterPc);
        MToNPartitioningConnectorDescriptor randomPartitioner =
                new MToNPartitioningConnectorDescriptor(spec, new RandomPartitionComputerFactory());
        DeployedJobPartitionHolderDescriptor intakePartitionHolder =
                new DeployedJobPartitionHolderDescriptor(spec, 1, feed.getFeedId(), FEED_INTAKE_PARTITION_HOLDER,
                        Integer.valueOf(feed.getConfiguration().getOrDefault(FeedConstants.WORKER_NUM, "1")));
        spec.connect(randomPartitioner, intakeOperator, 0, intakePartitionHolder, 0);
        //TODO: the partition constraint in intake is set to cluster location now.
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, intakePartitionHolder,
                metadataProvider.getClusterLocations().getLocations());
        spec.addRoot(intakePartitionHolder);
        return Pair.of(spec, adapterFactory);
    }

    public static JobSpecification buildRemoveFeedStorageJob(MetadataProvider metadataProvider, Feed feed)
            throws AsterixException {
        ICcApplicationContext appCtx = metadataProvider.getApplicationContext();
        JobSpecification spec = RuntimeUtils.createJobSpecification(appCtx);
        IClusterStateManager csm = appCtx.getClusterStateManager();
        AlgebricksAbsolutePartitionConstraint allCluster = csm.getClusterLocations();
        Set<String> nodes = new TreeSet<>();
        for (String node : allCluster.getLocations()) {
            nodes.add(node);
        }
        AlgebricksAbsolutePartitionConstraint locations =
                new AlgebricksAbsolutePartitionConstraint(nodes.toArray(new String[nodes.size()]));
        FileSplit[] feedLogFileSplits =
                FeedUtils.splitsForAdapter(appCtx, feed.getDataverseName(), feed.getFeedName(), locations);
        org.apache.hyracks.algebricks.common.utils.Pair<IFileSplitProvider, AlgebricksPartitionConstraint> spC =
                StoragePathUtil.splitProviderAndPartitionConstraints(feedLogFileSplits);
        FileRemoveOperatorDescriptor frod = new FileRemoveOperatorDescriptor(spec, spC.first, true);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, frod, spC.second);
        spec.addRoot(frod);
        return spec;
    }

    private static List<Expression> addArgs(Object... args) {
        List<Expression> argExprs = new ArrayList<>();
        for (Object arg : args) {
            if (arg instanceof Integer) {
                argExprs.add(new LiteralExpr(new IntegerLiteral((Integer) arg)));
            } else if (arg instanceof String) {
                argExprs.add(new LiteralExpr(new StringLiteral((String) arg)));
            } else if (arg instanceof VariableExpr) {
                argExprs.add((VariableExpr) arg);
            }
        }
        return argExprs;
    }

    private static Query makeConnectionQuery(FeedConnection feedConnection) throws AlgebricksException {
        // Construct from clause
        VarIdentifier fromVarId = SqlppVariableUtil.toInternalVariableIdentifier(feedConnection.getFeedName());
        VariableExpr fromTermLeftExpr = new VariableExpr(fromVarId);
        // TODO: remove target feedid from args list (xikui)
        // TODO: Get rid of this INTAKE
        List<Expression> exprList = addArgs(feedConnection.getDataverseName(),
                feedConnection.getFeedId().getEntityName(), feedConnection.getFeedId().getEntityName(),
                FeedRuntimeType.INTAKE.toString(), feedConnection.getDatasetName(), feedConnection.getOutputType());
        CallExpr datasrouceCallFunction = new CallExpr(new FunctionSignature(BuiltinFunctions.FEED_COLLECT), exprList);
        FromTerm fromterm = new FromTerm(datasrouceCallFunction, fromTermLeftExpr, null, null);
        FromClause fromClause = new FromClause(Arrays.asList(fromterm));
        WhereClause whereClause = null;
        if (feedConnection.getWhereClauseBody().length() != 0) {
            String whereClauseExpr = feedConnection.getWhereClauseBody() + ";";
            IParserFactory sqlppParserFactory = new SqlppParserFactory();
            IParser sqlppParser = sqlppParserFactory.createParser(whereClauseExpr);
            List<Statement> stmts = sqlppParser.parse();
            if (stmts.size() != 1) {
                throw new CompilationException("Exceptions happened in processing where clause.");
            }
            Query whereClauseQuery = (Query) stmts.get(0);
            whereClause = new WhereClause(whereClauseQuery.getBody());
        }

        // TODO: This can be the place to add select predicate for ingestion
        // Attaching functions
        int varIdx = 1;
        VariableExpr previousVarExpr = fromTermLeftExpr;
        ArrayList<LetClause> letClauses = new ArrayList<>();
        for (FunctionSignature funcSig : feedConnection.getAppliedFunctions()) {
            VarIdentifier intermediateVar = SqlppVariableUtil
                    .toInternalVariableIdentifier(FEED_DATAFLOW_INTERMEIDATE_VAL_PREFIX + String.valueOf(varIdx));
            VariableExpr intermediateVarExpr = new VariableExpr(intermediateVar);
            CallExpr functionCallExpr = new CallExpr(funcSig, addArgs(previousVarExpr));
            previousVarExpr = intermediateVarExpr;
            LetClause letClause = new LetClause(intermediateVarExpr, functionCallExpr);
            letClauses.add(letClause);
            varIdx++;
        }
        // Constructing select clause
        SelectElement selectElement = new SelectElement(previousVarExpr);
        SelectClause selectClause = new SelectClause(selectElement, null, false);
        SelectBlock selectBlock = new SelectBlock(selectClause, fromClause, letClauses, whereClause, null, null, null);
        SelectSetOperation selectSetOperation = new SelectSetOperation(new SetOperationInput(selectBlock, null), null);
        SelectExpression body = new SelectExpression(null, selectSetOperation, null, null, true);
        Query query = new Query(false, true, body, 0);
        return query;
    }

    private static JobSpecification getConnectionJob(MetadataProvider metadataProvider, FeedConnection feedConn,
            IStatementExecutor statementExecutor, IHyracksClientConnection hcc, Boolean insertFeed)
            throws AlgebricksException, ACIDException {
        metadataProvider.getConfig().put(FeedActivityDetails.FEED_POLICY_NAME, feedConn.getPolicyName());
        metadataProvider.setWriteTransaction(true);
        Query feedConnQuery = makeConnectionQuery(feedConn);
        InsertStatement stmt;
        if (insertFeed) {
            stmt = new InsertStatement(new Identifier(feedConn.getDataverseName()),
                    new Identifier(feedConn.getDatasetName()), feedConnQuery, -1, null, null);
        } else {
            stmt = new UpsertStatement(new Identifier(feedConn.getDataverseName()),
                    new Identifier(feedConn.getDatasetName()), feedConnQuery, -1, null, null);
        }
        return statementExecutor.rewriteCompileInsertUpsert(hcc, metadataProvider, stmt, null, null);
    }

    //    private static JobSpecification simpleModifyConnJob(MetadataProvider metadataProvider, Feed feed,
    //            JobSpecification intakeJob, JobSpecification connJob, FeedConnection currentConnection) {
    //        FeedCollectOperatorDescriptor firstOp = (FeedCollectOperatorDescriptor) connJob.getOperatorMap()
    //                .get(new OperatorDescriptorId(0));
    //
    //        //        FeedCallableOperatorDescriptor callableOpDesc = new FeedCallableOperatorDescriptor(connJob, intakeLocations[0],
    //        //                new ActiveRuntimeId(feed.getFeedId(), FeedIntakeOperatorNodePushable.class.getSimpleName(), 0));
    //        FeedConnWorkerOperatorDescriptor callableOpDesc = new FeedConnWorkerOperatorDescriptor(connJob,
    //                intakeLocations[0],
    //                new ActiveRuntimeId(feed.getFeedId(), FeedIntakeOperatorNodePushable.class.getSimpleName(), 0));
    //
    //        PartitionConstraintHelper.addAbsoluteLocationConstraint(connJob, callableOpDesc, metadataProvider.getClusterLocations().getLocations());
    //        connJob.connect(new OneToOneConnectorDescriptor(connJob), callableOpDesc, 0, firstOp, 0);
    //        return connJob;
    //    }

    private static IStatementExecutor getSQLPPTranslator(MetadataProvider metadataProvider,
            SessionOutput sessionOutput) {
        List<Statement> stmts = new ArrayList<>();
        DefaultStatementExecutorFactory qtFactory = new DefaultStatementExecutorFactory();
        IStatementExecutor translator = qtFactory.create(metadataProvider.getApplicationContext(), stmts, sessionOutput,
                new SqlppCompilationProvider(), new StorageComponentProvider());
        return translator;
    }

    public static Pair<JobSpecification, AlgebricksAbsolutePartitionConstraint> buildStartFeedJob(
            MetadataProvider metadataProvider, Feed feed) throws Exception {
        FeedPolicyAccessor fpa = new FeedPolicyAccessor(new HashMap<>());
        Pair<JobSpecification, IAdapterFactory> intakeInfo = buildFeedIntakeJobSpec(feed, metadataProvider, fpa);
        // Construct the ingestion Job
        JobSpecification intakeJob = intakeInfo.getLeft();
        return Pair.of(intakeJob, intakeInfo.getRight().getPartitionConstraint());
    }

    public static JobSpecification buildFeedConnectionJob(MetadataProvider metadataProvider,
            List<FeedConnection> feedConnections, JobSpecification intakeJob, IHyracksClientConnection hcc,
            IStatementExecutor statementExecutor, Feed feed, AlgebricksAbsolutePartitionConstraint ingestionLocations)
            throws Exception {
        // Add metadata configs
        metadataProvider.getConfig().put(FunctionUtil.IMPORT_PRIVATE_FUNCTIONS, Boolean.TRUE.toString());
        metadataProvider.getConfig().put(FeedActivityDetails.COLLECT_LOCATIONS,
                StringUtils.join(metadataProvider.getClusterLocations().getLocations(), ','));
        // TODO: Once we deprecated AQL, this extra queryTranslator can be removed.
        IStatementExecutor translator =
                getSQLPPTranslator(metadataProvider, ((QueryTranslator) statementExecutor).getSessionOutput());
        // Add connection job
        Boolean insertFeed = ExternalDataUtils.isInsertFeed(feed.getConfiguration());
        JobSpecification connJob =
                getConnectionJob(metadataProvider, feedConnections.get(0), translator, hcc, insertFeed);
        return connJob;
    }
}