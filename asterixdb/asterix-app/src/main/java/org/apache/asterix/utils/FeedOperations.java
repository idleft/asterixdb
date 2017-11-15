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

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.message.ActiveManagerMessage;
import org.apache.asterix.active.message.ActiveManagerMessage.Kind;
import org.apache.asterix.app.translator.DefaultStatementExecutorFactory;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.dataflow.LSMTreeInsertDeleteOperatorDescriptor;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.compiler.provider.SqlppCompilationProvider;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.feed.watch.FeedActivityDetails;
import org.apache.asterix.external.operators.FeedCollectOperatorDescriptor;
import org.apache.asterix.external.operators.FeedIntakeOperatorDescriptor;
import org.apache.asterix.external.operators.FeedIntakeOperatorNodePushable;
import org.apache.asterix.external.operators.FeedMetaOperatorDescriptor;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.FeedConstants;
import org.apache.asterix.external.util.FeedUtils;
import org.apache.asterix.external.util.FeedUtils.FeedRuntimeType;
import org.apache.asterix.file.StorageComponentProvider;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.clause.LetClause;
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
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedConnection;
import org.apache.asterix.metadata.entities.FeedPolicyEntity;
import org.apache.asterix.metadata.feeds.FeedMetadataUtil;
import org.apache.asterix.metadata.feeds.LocationConstraint;
import org.apache.asterix.runtime.job.listener.JobEventListenerFactory;
import org.apache.asterix.runtime.job.listener.MultiTransactionJobletEventListenerFactory;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.asterix.translator.CompiledStatements;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.SessionOutput;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.constraints.Constraint;
import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.constraints.expressions.ConstantExpression;
import org.apache.hyracks.api.constraints.expressions.ConstraintExpression;
import org.apache.hyracks.api.constraints.expressions.LValueConstraintExpression;
import org.apache.hyracks.api.constraints.expressions.PartitionCountExpression;
import org.apache.hyracks.api.constraints.expressions.PartitionLocationExpression;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningWithMessageConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.FileRemoveOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.misc.NullSinkOperatorDescriptor;
import org.apache.hyracks.dataflow.std.misc.ReplicateOperatorDescriptor;

/**
 * Provides helper method(s) for creating JobSpec for operations on a feed.
 */
public class FeedOperations {

    public static final String FEED_DATAFLOW_INTERMEIDATE_VAL_PREFIX = "val";

    private FeedOperations() {
    }

    private static Pair<JobSpecification, IAdapterFactory> buildFeedIntakeJobSpec(Feed feed,
            MetadataProvider metadataProvider, FeedPolicyAccessor policyAccessor, int initConnNum) throws Exception {
        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        spec.setFrameSize(metadataProvider.getApplicationContext().getCompilerProperties().getFrameSize());
        IAdapterFactory adapterFactory;
        IOperatorDescriptor feedIngestor;
        AlgebricksPartitionConstraint ingesterPc;
        Triple<IOperatorDescriptor, AlgebricksPartitionConstraint, IAdapterFactory> t =
                metadataProvider.buildFeedIntakeRuntime(spec, feed, policyAccessor, initConnNum);
        feedIngestor = t.first;
        ingesterPc = t.second;
        adapterFactory = t.third;
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, feedIngestor, ingesterPc);
        NullSinkOperatorDescriptor nullSink = new NullSinkOperatorDescriptor(spec);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, nullSink, ingesterPc);
        spec.connect(new OneToOneConnectorDescriptor(spec), feedIngestor, 0, nullSink, 0);
        spec.addRoot(nullSink);
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

    private static Query makeConnectionQuery(FeedConnection feedConnection) {
        // Construct from clause
        VarIdentifier fromVarId = SqlppVariableUtil.toInternalVariableIdentifier(feedConnection.getFeedName());
        VariableExpr fromTermLeftExpr = new VariableExpr(fromVarId);
        // TODO: remove target feedid from args list (xikui)
        // TODO: Get rid of this INTAKE
        List<Expression> exprList =
                addArgs(feedConnection.getDataverseName(), feedConnection.getFeedId().getEntityName(),
                        feedConnection.getFeedId().getEntityName(), FeedRuntimeType.INTAKE.toString(),
                        feedConnection.getDatasetName(), feedConnection.getOutputType());
        CallExpr datasrouceCallFunction = new CallExpr(new FunctionSignature(FeedConstants.FEED_COLLECT_FUN), exprList);
        FromTerm fromterm = new FromTerm(datasrouceCallFunction, fromTermLeftExpr, null, null);
        FromClause fromClause = new FromClause(Arrays.asList(fromterm));
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
        SelectBlock selectBlock = new SelectBlock(selectClause, fromClause, letClauses, null, null, null, null);
        SelectSetOperation selectSetOperation = new SelectSetOperation(new SetOperationInput(selectBlock, null), null);
        SelectExpression body = new SelectExpression(null, selectSetOperation, null, null, true);
        Query query = new Query(false, true, body, 0);
        return query;
    }

    public static JobSpecification getConnectionJob(MetadataProvider metadataProvider, FeedConnection feedConn,
            IStatementExecutor statementExecutor, IHyracksClientConnection hcc, Boolean insertFeed)
            throws AlgebricksException, RemoteException, ACIDException {
        metadataProvider.getConfig().put(FeedActivityDetails.FEED_POLICY_NAME, feedConn.getPolicyName());
        Query feedConnQuery = makeConnectionQuery(feedConn);
        CompiledStatements.ICompiledDmlStatement clfrqs;
        if (insertFeed) {
            InsertStatement stmtUpsert = new InsertStatement(new Identifier(feedConn.getDataverseName()),
                    new Identifier(feedConn.getDatasetName()), feedConnQuery, -1, null, null);
            clfrqs = new CompiledStatements.CompiledInsertStatement(feedConn.getDataverseName(),
                    feedConn.getDatasetName(), feedConnQuery, stmtUpsert.getVarCounter(), null, null);
        } else {
            UpsertStatement stmtUpsert = new UpsertStatement(new Identifier(feedConn.getDataverseName()),
                    new Identifier(feedConn.getDatasetName()), feedConnQuery, -1, null, null);
            clfrqs = new CompiledStatements.CompiledUpsertStatement(feedConn.getDataverseName(),
                    feedConn.getDatasetName(), feedConnQuery, stmtUpsert.getVarCounter(), null, null);
        }
        return statementExecutor.rewriteCompileQuery(hcc, metadataProvider, feedConnQuery, clfrqs);
    }

    private static IStatementExecutor getSQLPPTranslator(MetadataProvider metadataProvider,
            SessionOutput sessionOutput) {
        List<Statement> stmts = new ArrayList<>();
        DefaultStatementExecutorFactory qtFactory = new DefaultStatementExecutorFactory();
        IStatementExecutor translator = qtFactory
                .create(metadataProvider.getApplicationContext(), stmts, sessionOutput, new SqlppCompilationProvider(),
                        new StorageComponentProvider());
        return translator;
    }

    public static Pair<JobSpecification, String[]> buildStartFeedJob(
            MetadataProvider metadataProvider, Feed feed, List<FeedConnection> feedConnections,
            IStatementExecutor statementExecutor, IHyracksClientConnection hcc) throws Exception {
        FeedPolicyAccessor fpa = new FeedPolicyAccessor(new HashMap<>());
        Pair<JobSpecification, IAdapterFactory> intakeInfo = buildFeedIntakeJobSpec(feed, metadataProvider, fpa, feedConnections.size());
        // TODO: Restore the insert and upsert feed
//        Boolean insertFeed = ExternalDataUtils.isInsertFeed(feed.getAdapterConfiguration());
        // Construct the ingestion Job
        return Pair.of(intakeInfo.getLeft(), intakeInfo.getRight().getPartitionConstraint().getLocations());
        // Add metadata configs
//        metadataProvider.getConfig().put(FunctionUtil.IMPORT_PRIVATE_FUNCTIONS, Boolean.TRUE.toString());
//        metadataProvider.getConfig()
//                .put(FeedActivityDetails.COLLECT_LOCATIONS, StringUtils.join(ingestionLocations, ','));
//        // TODO: Once we deprecated AQL, this extra queryTranslator can be removed.
//        IStatementExecutor translator =
//                getSQLPPTranslator(metadataProvider, ((QueryTranslator) statementExecutor).getSessionOutput());
//        // Add connection job
//        for (FeedConnection feedConnection : feedConnections) {
//            JobSpecification connectionJob = getConnectionJob(metadataProvider, feedConnection, translator, hcc,
//                    insertFeed);
//            jobsList.add(connectionJob);
//        }
//        return Pair.of(combineIntakeCollectJobs(metadataProvider, feed, intakeJob, jobsList, feedConnections,
//                ingestionLocations), intakeInfo.getRight().getPartitionConstraint());
    }

    public static void SendStopMessageToNode(ICcApplicationContext appCtx, EntityId feedId, String intakeNodeLocation,
            Integer partition) throws Exception {
        ActiveManagerMessage stopFeedMessage = new ActiveManagerMessage(Kind.STOP_ACTIVITY,
                new ActiveRuntimeId(feedId, FeedIntakeOperatorNodePushable.class.getSimpleName(), partition));
        SendActiveMessage(appCtx, stopFeedMessage, intakeNodeLocation);
    }

    private static void SendActiveMessage(ICcApplicationContext appCtx, ActiveManagerMessage activeManagerMessage,
            String nodeId) throws Exception {
        ICCMessageBroker messageBroker = (ICCMessageBroker) appCtx.getServiceContext().getMessageBroker();
        messageBroker.sendApplicationMessageToNC(activeManagerMessage, nodeId);
    }
}