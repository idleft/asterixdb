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

import org.apache.asterix.app.translator.DefaultStatementExecutorFactory;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.dataflow.LSMTreeInsertDeleteOperatorDescriptor;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.compiler.provider.SqlppCompilationProvider;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.feed.watch.FeedActivityDetails;
import org.apache.asterix.external.operators.FeedCallableOperatorDescriptor;
import org.apache.asterix.external.operators.FeedCollectOperatorDescriptor;
import org.apache.asterix.external.operators.FeedIntakeOperatorDescriptor;
import org.apache.asterix.external.operators.FeedMetaOperatorDescriptor;
import org.apache.asterix.external.util.ExternalDataUtils;
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
import org.apache.asterix.metadata.entities.FeedPolicyEntity;
import org.apache.asterix.metadata.feeds.FeedMetadataUtil;
import org.apache.asterix.metadata.feeds.LocationConstraint;
import org.apache.asterix.om.functions.BuiltinFunctions;
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
import org.apache.hyracks.algebricks.runtime.base.AlgebricksPipeline;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
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
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
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
            MetadataProvider metadataProvider, FeedPolicyAccessor policyAccessor, int connDs) throws Exception {
        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        spec.setFrameSize(metadataProvider.getApplicationContext().getCompilerProperties().getFrameSize());
        IAdapterFactory adapterFactory;
        IOperatorDescriptor feedIngestor;
        AlgebricksPartitionConstraint ingesterPc;
        Triple<IOperatorDescriptor, AlgebricksPartitionConstraint, IAdapterFactory> t =
                metadataProvider.buildFeedIntakeRuntime(spec, feed, policyAccessor, connDs);
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
        return statementExecutor.rewriteCompileQuery(hcc, metadataProvider, feedConnQuery, clfrqs, null, null);
    }

    private static JobSpecification simpleModifyConnJob(MetadataProvider metadataProvider, Feed feed,
            JobSpecification intakeJob, JobSpecification connJob, FeedConnection currentConnection,
            String[] intakeLocations) {
        FeedCollectOperatorDescriptor firstOp =
                (FeedCollectOperatorDescriptor) connJob.getOperatorMap().get(new OperatorDescriptorId(0));

        FeedCallableOperatorDescriptor callableOpDesc = new FeedCallableOperatorDescriptor(connJob, intakeLocations[0],
                new ActiveRuntimeId(feed.getFeedId(), FeedIntakeOperatorNodePushable.class.getSimpleName(), 0));

        // create replicator
        PartitionConstraintHelper.addAbsoluteLocationConstraint(connJob, callableOpDesc, intakeLocations);
        connJob.connect(new OneToOneConnectorDescriptor(connJob), callableOpDesc, 0, firstOp, 0);
        return connJob;
    }

    private static JobSpecification modifyConnectJob(MetadataProvider metadataProvider, Feed feed,
            JobSpecification intakeJob, JobSpecification connJob, FeedConnection currentConnection,
            String[] intakeLocations) throws AlgebricksException {
        JobSpecification jobSpec = new JobSpecification(intakeJob.getFrameSize());

        // create callable feed source
        // TODO: this only covers 1 adapter case, more adapters need to change.
        FeedCallableOperatorDescriptor callableOpDesc = new FeedCallableOperatorDescriptor(jobSpec, intakeLocations[0],
                new ActiveRuntimeId(feed.getFeedId(), FeedIntakeOperatorNodePushable.class.getSimpleName(), 0));

        // create replicator
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, callableOpDesc, intakeLocations);
        // Loop over the jobs to copy operators and connections
        Map<OperatorDescriptorId, OperatorDescriptorId> operatorIdMapping = new HashMap<>();
        Map<ConnectorDescriptorId, ConnectorDescriptorId> connectorIdMapping = new HashMap<>();
        Map<OperatorDescriptorId, List<LocationConstraint>> operatorLocations = new HashMap<>();
        Map<OperatorDescriptorId, Integer> operatorCounts = new HashMap<>();
        Map<OperatorDescriptorId, IOperatorDescriptor> operatorsMap = connJob.getOperatorMap();

        for (Map.Entry<OperatorDescriptorId, IOperatorDescriptor> entry : operatorsMap.entrySet()) {
            IOperatorDescriptor opDesc = entry.getValue();
            OperatorDescriptorId oldId = opDesc.getOperatorId();
            if (opDesc instanceof FeedCollectOperatorDescriptor) {
                continue;
            }
            OperatorDescriptorId opId = jobSpec.createOperatorDescriptorId(opDesc);
            operatorIdMapping.put(oldId, opId);
        }

        // copy connectors
        connectorIdMapping.clear();
        connJob.getConnectorMap().forEach((key, connDesc) -> {
            Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>> entry =
                    connJob.getConnectorOperatorMap().get(key);
            if (entry.getLeft().getLeft() instanceof FeedCollectOperatorDescriptor) {
                // do nothing
            } else {
                ConnectorDescriptorId newConnId = jobSpec.createConnectorDescriptor(connDesc);
                connectorIdMapping.put(key, newConnId);
            }
        });

        // make connections between operators
        for (Entry<ConnectorDescriptorId, Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>>> entry : connJob
                .getConnectorOperatorMap().entrySet()) {
            ConnectorDescriptorId newId = connectorIdMapping.get(entry.getKey());
            IConnectorDescriptor connDesc = jobSpec.getConnectorMap().get(newId);
            Pair<IOperatorDescriptor, Integer> leftOp = entry.getValue().getLeft();
            Pair<IOperatorDescriptor, Integer> rightOp = entry.getValue().getRight();
            IOperatorDescriptor leftOpDesc = jobSpec.getOperatorMap().get(leftOp.getLeft().getOperatorId());
            IOperatorDescriptor rightOpDesc = jobSpec.getOperatorMap().get(rightOp.getLeft().getOperatorId());
            if (leftOp.getLeft() instanceof FeedCollectOperatorDescriptor) {
                jobSpec.connect(new OneToOneConnectorDescriptor(jobSpec), callableOpDesc, 0, rightOpDesc,
                        rightOp.getRight());
            } else {
                jobSpec.connect(connDesc, leftOpDesc, leftOp.getRight(), rightOpDesc, rightOp.getRight());
            }
        }

        // prepare for setting partition constraints
        operatorLocations.clear();
        operatorCounts.clear();

        for (Constraint constraint : connJob.getUserConstraints()) {
            LValueConstraintExpression lexpr = constraint.getLValue();
            ConstraintExpression cexpr = constraint.getRValue();
            OperatorDescriptorId opId;
            switch (lexpr.getTag()) {
                case PARTITION_COUNT:
                    opId = ((PartitionCountExpression) lexpr).getOperatorDescriptorId();
                    if (opId.getId() == 0) {
                        continue;
                    }
                    operatorCounts.put(operatorIdMapping.get(opId), (int) ((ConstantExpression) cexpr).getValue());
                    break;
                case PARTITION_LOCATION:
                    opId = ((PartitionLocationExpression) lexpr).getOperatorDescriptorId();
                    if (opId.getId() == 0) {
                        continue;
                    }
                    IOperatorDescriptor opDesc = jobSpec.getOperatorMap().get(operatorIdMapping.get(opId));
                    List<LocationConstraint> locations = operatorLocations.get(opDesc.getOperatorId());
                    if (locations == null) {
                        locations = new ArrayList<>();
                        operatorLocations.put(opDesc.getOperatorId(), locations);
                    }
                    String location = (String) ((ConstantExpression) cexpr).getValue();
                    LocationConstraint lc =
                            new LocationConstraint(location, ((PartitionLocationExpression) lexpr).getPartition());
                    locations.add(lc);
                    break;
                default:
                    break;
            }
        }

        // set absolute location constraints
        for (Entry<OperatorDescriptorId, List<LocationConstraint>> entry : operatorLocations.entrySet()) {
            IOperatorDescriptor opDesc = jobSpec.getOperatorMap().get(entry.getKey());
            String[] locations = new String[entry.getValue().size()];
            for (int j = 0; j < locations.length; ++j) {
                locations[j] = entry.getValue().get(j).location;
            }
            PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, opDesc, locations);
        }

        // set count constraints
        operatorCounts.forEach((key, value) -> {
            IOperatorDescriptor opDesc = jobSpec.getOperatorMap().get(key);
            if (!operatorLocations.keySet().contains(key)) {
                PartitionConstraintHelper.addPartitionCountConstraint(jobSpec, opDesc, value);
            }
        });
        // roots
        for (OperatorDescriptorId root : connJob.getRoots()) {
            jobSpec.addRoot(jobSpec.getOperatorMap().get(operatorIdMapping.get(root)));
        }

        // jobEventListenerFactory
        jobSpec.setJobletEventListenerFactory(connJob.getJobletEventListenerFactory());
        // useConnectorSchedulingPolicy
        jobSpec.setUseConnectorPolicyForScheduling(connJob.isUseConnectorPolicyForScheduling());
        // connectorAssignmentPolicy
        jobSpec.setConnectorPolicyAssignmentPolicy(connJob.getConnectorPolicyAssignmentPolicy());
        return jobSpec;
    }

    private static IStatementExecutor getSQLPPTranslator(MetadataProvider metadataProvider,
            SessionOutput sessionOutput) {
        List<Statement> stmts = new ArrayList<>();
        DefaultStatementExecutorFactory qtFactory = new DefaultStatementExecutorFactory();
        IStatementExecutor translator = qtFactory.create(metadataProvider.getApplicationContext(), stmts, sessionOutput,
                new SqlppCompilationProvider(), new StorageComponentProvider());
        return translator;
    }

    public static Pair<JobSpecification, AlgebricksAbsolutePartitionConstraint> buildStartFeedJob(
            MetadataProvider metadataProvider, Feed feed, int connDs) throws Exception {
        FeedPolicyAccessor fpa = new FeedPolicyAccessor(new HashMap<>());
        Pair<JobSpecification, IAdapterFactory> intakeInfo =
                buildFeedIntakeJobSpec(feed, metadataProvider, fpa, connDs);
        // TODO: Figure out a better way to handle insert/upsert per conn instead of per feed
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
                StringUtils.join(ingestionLocations.getLocations(), ','));
        // TODO: Once we deprecated AQL, this extra queryTranslator can be removed.
        IStatementExecutor translator =
                getSQLPPTranslator(metadataProvider, ((QueryTranslator) statementExecutor).getSessionOutput());
        // Add connection job
        Boolean insertFeed = ExternalDataUtils.isInsertFeed(feed.getConfiguration());
        JobSpecification connJob =
                getConnectionJob(metadataProvider, feedConnections.get(0), translator, hcc, insertFeed);
        return simpleModifyConnJob(metadataProvider, feed, intakeJob, connJob, feedConnections.get(0),
                ingestionLocations.getLocations());
        //                ingestionLocations.getLocations())
        //        for (FeedConnection feedConnection : feedConnections) {
        //            JobSpecification connectionJob =
        //                    getConnectionJob(metadataProvider, feedConnection, translator, hcc, insertFeed);
        //            jobsList.add(connectionJob);
        //        }
        //        return combineIntakeCollectJobs(metadataProvider, feed, intakeJob, jobsList, feedConnections,
        //                ingestionLocations.getLocations());
    }
}