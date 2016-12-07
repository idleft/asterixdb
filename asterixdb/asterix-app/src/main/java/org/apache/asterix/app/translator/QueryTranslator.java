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
package org.apache.asterix.app.translator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.rmi.RemoteException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.ActiveJobNotificationHandler;
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveEntityEventsListener;
import org.apache.asterix.algebra.extension.IExtensionStatement;
import org.apache.asterix.api.common.APIFramework;
import org.apache.asterix.api.http.servlet.APIServlet;
import org.apache.asterix.app.cc.CompilerExtensionManager;
import org.apache.asterix.app.external.ExternalIndexingOperations;
import org.apache.asterix.app.external.FeedOperations;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.app.result.ResultUtil;
import org.apache.asterix.common.config.AsterixExternalProperties;
import org.apache.asterix.common.config.ClusterProperties;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.ExternalDatasetTransactionState;
import org.apache.asterix.common.config.DatasetConfig.ExternalFilePendingOp;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.compiler.provider.AqlCompilationProvider;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.management.FeedEventsListener;
import org.apache.asterix.external.feed.watch.FeedConnectJobInfo;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.file.DatasetOperations;
import org.apache.asterix.file.DataverseOperations;
import org.apache.asterix.file.IndexOperations;
import org.apache.asterix.formats.nontagged.AqlTypeTraitProvider;
import org.apache.asterix.lang.common.base.IRewriterFactory;
import org.apache.asterix.lang.common.base.IStatementRewriter;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.expression.TypeExpression;
import org.apache.asterix.lang.common.statement.CompactStatement;
import org.apache.asterix.lang.common.statement.ConnectFeedStatement;
import org.apache.asterix.lang.common.statement.CreateDataverseStatement;
import org.apache.asterix.lang.common.statement.CreateFeedPolicyStatement;
import org.apache.asterix.lang.common.statement.CreateFeedStatement;
import org.apache.asterix.lang.common.statement.CreateFunctionStatement;
import org.apache.asterix.lang.common.statement.CreateIndexStatement;
import org.apache.asterix.lang.common.statement.DatasetDecl;
import org.apache.asterix.lang.common.statement.DataverseDecl;
import org.apache.asterix.lang.common.statement.DataverseDropStatement;
import org.apache.asterix.lang.common.statement.DeleteStatement;
import org.apache.asterix.lang.common.statement.DisconnectFeedStatement;
import org.apache.asterix.lang.common.statement.DropDatasetStatement;
import org.apache.asterix.lang.common.statement.ExternalDetailsDecl;
import org.apache.asterix.lang.common.statement.FeedDropStatement;
import org.apache.asterix.lang.common.statement.FeedPolicyDropStatement;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.FunctionDropStatement;
import org.apache.asterix.lang.common.statement.IDatasetDetailsDecl;
import org.apache.asterix.lang.common.statement.IndexDropStatement;
import org.apache.asterix.lang.common.statement.InsertStatement;
import org.apache.asterix.lang.common.statement.InternalDetailsDecl;
import org.apache.asterix.lang.common.statement.LoadStatement;
import org.apache.asterix.lang.common.statement.NodeGroupDropStatement;
import org.apache.asterix.lang.common.statement.NodegroupDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.statement.RefreshExternalDatasetStatement;
import org.apache.asterix.lang.common.statement.RunStatement;
import org.apache.asterix.lang.common.statement.SetStatement;
import org.apache.asterix.lang.common.statement.StartFeedStatement;
import org.apache.asterix.lang.common.statement.StopFeedStatement;
import org.apache.asterix.lang.common.statement.TypeDecl;
import org.apache.asterix.lang.common.statement.TypeDropStatement;
import org.apache.asterix.lang.common.statement.WriteStatement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.metadata.IDatasetDetails;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.api.IMetadataEntity;
import org.apache.asterix.metadata.bootstrap.MetadataBuiltinEntities;
import org.apache.asterix.metadata.dataset.hints.DatasetHints;
import org.apache.asterix.metadata.dataset.hints.DatasetHints.DatasetNodegroupCardinalityHint;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.entities.BuiltinTypeMap;
import org.apache.asterix.metadata.entities.CompactionPolicy;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.ExternalDatasetDetails;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedConnection;
import org.apache.asterix.metadata.entities.FeedPolicyEntity;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.asterix.metadata.feeds.FeedMetadataUtil;
import org.apache.asterix.metadata.utils.DatasetUtils;
import org.apache.asterix.metadata.utils.ExternalDatasetsRegistry;
import org.apache.asterix.metadata.utils.KeyFieldTypeUtils;
import org.apache.asterix.metadata.utils.MetadataConstants;
import org.apache.asterix.metadata.utils.MetadataLockManager;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeSignature;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.util.AsterixAppContextInfo;
import org.apache.asterix.transaction.management.service.transaction.DatasetIdFactory;
import org.apache.asterix.translator.AbstractLangTranslator;
import org.apache.asterix.translator.CompiledStatements.CompiledCreateIndexStatement;
import org.apache.asterix.translator.CompiledStatements.CompiledDatasetDropStatement;
import org.apache.asterix.translator.CompiledStatements.CompiledDeleteStatement;
import org.apache.asterix.translator.CompiledStatements.CompiledIndexCompactStatement;
import org.apache.asterix.translator.CompiledStatements.CompiledIndexDropStatement;
import org.apache.asterix.translator.CompiledStatements.CompiledInsertStatement;
import org.apache.asterix.translator.CompiledStatements.CompiledLoadFromFileStatement;
import org.apache.asterix.translator.CompiledStatements.CompiledUpsertStatement;
import org.apache.asterix.translator.CompiledStatements.ICompiledDmlStatement;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.SessionConfig;
import org.apache.asterix.translator.TypeTranslator;
import org.apache.asterix.translator.util.ValidateUtil;
import org.apache.asterix.util.FlushDatasetUtils;
import org.apache.asterix.util.JobUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression.FunctionKind;
import org.apache.hyracks.algebricks.data.IAWriterFactory;
import org.apache.hyracks.algebricks.data.IResultSerializerFactoryProvider;
import org.apache.hyracks.algebricks.runtime.serializer.ResultSerializerFactoryProvider;
import org.apache.hyracks.algebricks.runtime.writers.PrinterBasedWriterFactory;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.file.FileSplit;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.collect.Lists;

/*
 * Provides functionality for executing a batch of Query statements (queries included)
 * sequentially.
 */
public class QueryTranslator extends AbstractLangTranslator implements IStatementExecutor {

    private static final Logger LOGGER = Logger.getLogger(QueryTranslator.class.getName());

    protected enum ProgressState {
        NO_PROGRESS,
        ADDED_PENDINGOP_RECORD_TO_METADATA
    }

    public static final boolean IS_DEBUG_MODE = false;// true
    protected final List<Statement> statements;
    protected final SessionConfig sessionConfig;
    protected Dataverse activeDefaultDataverse;
    protected final List<FunctionDecl> declaredFunctions;
    protected final APIFramework apiFramework;
    protected final IRewriterFactory rewriterFactory;

    public QueryTranslator(List<Statement> aqlStatements, SessionConfig conf,
            ILangCompilationProvider compliationProvider, CompilerExtensionManager ccExtensionManager) {
        this.statements = aqlStatements;
        this.sessionConfig = conf;
        this.declaredFunctions = getDeclaredFunctions(aqlStatements);
        this.apiFramework = new APIFramework(compliationProvider, ccExtensionManager);
        this.rewriterFactory = compliationProvider.getRewriterFactory();
        activeDefaultDataverse = MetadataBuiltinEntities.DEFAULT_DATAVERSE;
    }

    protected List<FunctionDecl> getDeclaredFunctions(List<Statement> statements) {
        List<FunctionDecl> functionDecls = new ArrayList<>();
        for (Statement st : statements) {
            if (st.getKind() == Statement.Kind.FUNCTION_DECL) {
                functionDecls.add((FunctionDecl) st);
            }
        }
        return functionDecls;
    }

    /**
     * Compiles and submits for execution a list of AQL statements.
     *
     * @param hcc
     *            A Hyracks client connection that is used to submit a jobspec to Hyracks.
     * @param hdc
     *            A Hyracks dataset client object that is used to read the results.
     * @param resultDelivery
     *            True if the results should be read asynchronously or false if we should wait for results to be read.
     * @return A List<QueryResult> containing a QueryResult instance corresponding to each submitted query.
     * @throws Exception
     */
    @Override
    public void compileAndExecute(IHyracksClientConnection hcc, IHyracksDataset hdc, ResultDelivery resultDelivery)
            throws Exception {
        compileAndExecute(hcc, hdc, resultDelivery, new Stats());
    }

    @Override
    public void compileAndExecute(IHyracksClientConnection hcc, IHyracksDataset hdc, ResultDelivery resultDelivery,
            Stats stats) throws Exception {
        int resultSetIdCounter = 0;
        FileSplit outputFile = null;
        IAWriterFactory writerFactory = PrinterBasedWriterFactory.INSTANCE;
        IResultSerializerFactoryProvider resultSerializerFactoryProvider = ResultSerializerFactoryProvider.INSTANCE;
        Map<String, String> config = new HashMap<>();
        /* Since the system runs a large number of threads, when HTTP requests don't return, it becomes difficult to
         * find the thread running the request to determine where it has stopped.
         * Setting the thread name helps make that easier
         */
        String threadName = Thread.currentThread().getName();
        Thread.currentThread().setName(QueryTranslator.class.getSimpleName());
        try {
            for (Statement stmt : statements) {
                if (sessionConfig.is(SessionConfig.FORMAT_HTML)) {
                    sessionConfig.out().println(APIServlet.HTML_STATEMENT_SEPARATOR);
                }
                validateOperation(activeDefaultDataverse, stmt);
                rewriteStatement(stmt); // Rewrite the statement's AST.
                AqlMetadataProvider metadataProvider = new AqlMetadataProvider(activeDefaultDataverse);
                metadataProvider.setWriterFactory(writerFactory);
                metadataProvider.setResultSerializerFactoryProvider(resultSerializerFactoryProvider);
                metadataProvider.setOutputFile(outputFile);
                metadataProvider.setConfig(config);
                switch (stmt.getKind()) {
                    case Statement.Kind.SET:
                        handleSetStatement(stmt, config);
                        break;
                    case Statement.Kind.DATAVERSE_DECL:
                        activeDefaultDataverse = handleUseDataverseStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.CREATE_DATAVERSE:
                        handleCreateDataverseStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.DATASET_DECL:
                        handleCreateDatasetStatement(metadataProvider, stmt, hcc);
                        break;
                    case Statement.Kind.CREATE_INDEX:
                        handleCreateIndexStatement(metadataProvider, stmt, hcc);
                        break;
                    case Statement.Kind.TYPE_DECL:
                        handleCreateTypeStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.NODEGROUP_DECL:
                        handleCreateNodeGroupStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.DATAVERSE_DROP:
                        handleDataverseDropStatement(metadataProvider, stmt, hcc);
                        break;
                    case Statement.Kind.DATASET_DROP:
                        handleDatasetDropStatement(metadataProvider, stmt, hcc);
                        break;
                    case Statement.Kind.INDEX_DROP:
                        handleIndexDropStatement(metadataProvider, stmt, hcc);
                        break;
                    case Statement.Kind.TYPE_DROP:
                        handleTypeDropStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.NODEGROUP_DROP:
                        handleNodegroupDropStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.CREATE_FUNCTION:
                        handleCreateFunctionStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.FUNCTION_DROP:
                        handleFunctionDropStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.LOAD:
                        handleLoadStatement(metadataProvider, stmt, hcc);
                        break;
                    case Statement.Kind.INSERT:
                    case Statement.Kind.UPSERT:
                        if (((InsertStatement) stmt).getReturnQuery() != null) {
                            metadataProvider.setResultSetId(new ResultSetId(resultSetIdCounter++));
                            metadataProvider.setResultAsyncMode(resultDelivery == ResultDelivery.ASYNC
                                    || resultDelivery == ResultDelivery.ASYNC_DEFERRED);
                        }
                        handleInsertUpsertStatement(metadataProvider, stmt, hcc, hdc, resultDelivery, stats, false);
                        break;
                    case Statement.Kind.DELETE:
                        handleDeleteStatement(metadataProvider, stmt, hcc);
                        break;
                    case Statement.Kind.CREATE_FEED:
                        handleCreateFeedStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.DROP_FEED:
                        handleDropFeedStatement(metadataProvider, stmt, hcc);
                        break;
                    case Statement.Kind.DROP_FEED_POLICY:
                        handleDropFeedPolicyStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.CONNECT_FEED:
                        handleConnectFeedStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.DISCONNECT_FEED:
                        handleDisconnectFeedStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.START_FEED:
                        handleStartFeedStatement(metadataProvider, stmt, hcc);
                        break;
                    case Statement.Kind.STOP_FEED:
                        handleStopFeedStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.CREATE_FEED_POLICY:
                        handleCreateFeedPolicyStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.QUERY:
                        metadataProvider.setResultSetId(new ResultSetId(resultSetIdCounter++));
                        metadataProvider.setResultAsyncMode(resultDelivery == ResultDelivery.ASYNC
                                || resultDelivery == ResultDelivery.ASYNC_DEFERRED);
                        handleQuery(metadataProvider, (Query) stmt, hcc, hdc, resultDelivery, stats);
                        break;
                    case Statement.Kind.COMPACT:
                        handleCompactStatement(metadataProvider, stmt, hcc);
                        break;
                    case Statement.Kind.EXTERNAL_DATASET_REFRESH:
                        handleExternalDatasetRefreshStatement(metadataProvider, stmt, hcc);
                        break;
                    case Statement.Kind.WRITE:
                        Pair<IAWriterFactory, FileSplit> result = handleWriteStatement(stmt);
                        writerFactory = (result.first != null) ? result.first : writerFactory;
                        outputFile = result.second;
                        break;
                    case Statement.Kind.RUN:
                        handleRunStatement(metadataProvider, stmt, hcc);
                        break;
                    case Statement.Kind.FUNCTION_DECL:
                        // No op
                        break;
                    case Statement.Kind.EXTENSION:
                        ((IExtensionStatement) stmt).handle(this, metadataProvider, hcc, hdc, resultDelivery, stats,
                                resultSetIdCounter);
                        break;
                    default:
                        throw new AsterixException("Unknown function");
                }
            }
        } finally {
            Thread.currentThread().setName(threadName);
        }
    }

    protected void handleSetStatement(Statement stmt, Map<String, String> config) {
        SetStatement ss = (SetStatement) stmt;
        String pname = ss.getPropName();
        String pvalue = ss.getPropValue();
        config.put(pname, pvalue);
    }

    protected Pair<IAWriterFactory, FileSplit> handleWriteStatement(Statement stmt)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        WriteStatement ws = (WriteStatement) stmt;
        File f = new File(ws.getFileName());
        FileSplit outputFile = new FileSplit(ws.getNcName().getValue(), new FileReference(f));
        IAWriterFactory writerFactory = null;
        if (ws.getWriterClassName() != null) {
            writerFactory = (IAWriterFactory) Class.forName(ws.getWriterClassName()).newInstance();
        }
        return new Pair<>(writerFactory, outputFile);
    }

    protected Dataverse handleUseDataverseStatement(AqlMetadataProvider metadataProvider, Statement stmt)
            throws Exception {
        DataverseDecl dvd = (DataverseDecl) stmt;
        String dvName = dvd.getDataverseName().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockManager.INSTANCE.acquireDataverseReadLock(dvName);
        try {
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(metadataProvider.getMetadataTxnContext(), dvName);
            if (dv == null) {
                throw new MetadataException("Unknown dataverse " + dvName);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            return dv;
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw new MetadataException(e);
        } finally {
            MetadataLockManager.INSTANCE.releaseDataverseReadLock(dvName);
        }
    }

    protected void handleCreateDataverseStatement(AqlMetadataProvider metadataProvider, Statement stmt)
            throws Exception {

        CreateDataverseStatement stmtCreateDataverse = (CreateDataverseStatement) stmt;
        String dvName = stmtCreateDataverse.getDataverseName().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        MetadataLockManager.INSTANCE.acquireDataverseReadLock(dvName);
        try {
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(metadataProvider.getMetadataTxnContext(), dvName);
            if (dv != null) {
                if (stmtCreateDataverse.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("A dataverse with this name " + dvName + " already exists.");
                }
            }
            MetadataManager.INSTANCE.addDataverse(metadataProvider.getMetadataTxnContext(),
                    new Dataverse(dvName, stmtCreateDataverse.getFormat(), IMetadataEntity.PENDING_NO_OP));
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.releaseDataverseReadLock(dvName);
        }
    }

    protected void validateCompactionPolicy(String compactionPolicy, Map<String, String> compactionPolicyProperties,
            MetadataTransactionContext mdTxnCtx, boolean isExternalDataset) throws AsterixException, Exception {
        CompactionPolicy compactionPolicyEntity = MetadataManager.INSTANCE.getCompactionPolicy(mdTxnCtx,
                MetadataConstants.METADATA_DATAVERSE_NAME, compactionPolicy);
        if (compactionPolicyEntity == null) {
            throw new AsterixException("Unknown compaction policy: " + compactionPolicy);
        }
        String compactionPolicyFactoryClassName = compactionPolicyEntity.getClassName();
        ILSMMergePolicyFactory mergePolicyFactory = (ILSMMergePolicyFactory) Class
                .forName(compactionPolicyFactoryClassName).newInstance();
        if (isExternalDataset && mergePolicyFactory.getName().compareTo("correlated-prefix") == 0) {
            throw new AsterixException("The correlated-prefix merge policy cannot be used with external dataset.");
        }
        if (compactionPolicyProperties == null) {
            if (mergePolicyFactory.getName().compareTo("no-merge") != 0) {
                throw new AsterixException("Compaction policy properties are missing.");
            }
        } else {
            for (Map.Entry<String, String> entry : compactionPolicyProperties.entrySet()) {
                if (!mergePolicyFactory.getPropertiesNames().contains(entry.getKey())) {
                    throw new AsterixException("Invalid compaction policy property: " + entry.getKey());
                }
            }
            for (String p : mergePolicyFactory.getPropertiesNames()) {
                if (!compactionPolicyProperties.containsKey(p)) {
                    throw new AsterixException("Missing compaction policy property: " + p);
                }
            }
        }
    }

    public void handleCreateDatasetStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws AsterixException, Exception {
        MutableObject<ProgressState> progress = new MutableObject<>(ProgressState.NO_PROGRESS);
        DatasetDecl dd = (DatasetDecl) stmt;
        String dataverseName = getActiveDataverse(dd.getDataverse());
        String datasetName = dd.getName().getValue();
        DatasetType dsType = dd.getDatasetType();
        String itemTypeDataverseName = getActiveDataverse(dd.getItemTypeDataverse());
        String itemTypeName = dd.getItemTypeName().getValue();
        String metaItemTypeDataverseName = getActiveDataverse(dd.getMetaItemTypeDataverse());
        String metaItemTypeName = dd.getMetaItemTypeName().getValue();
        Identifier ngNameId = dd.getNodegroupName();
        String nodegroupName = getNodeGroupName(ngNameId, dd, dataverseName);
        String compactionPolicy = dd.getCompactionPolicy();
        Map<String, String> compactionPolicyProperties = dd.getCompactionPolicyProperties();
        boolean defaultCompactionPolicy = compactionPolicy == null;
        boolean temp = dd.getDatasetDetailsDecl().isTemp();

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        MetadataLockManager.INSTANCE.createDatasetBegin(dataverseName, itemTypeDataverseName,
                itemTypeDataverseName + "." + itemTypeName, metaItemTypeDataverseName,
                metaItemTypeDataverseName + "." + metaItemTypeName, nodegroupName, compactionPolicy,
                dataverseName + "." + datasetName, defaultCompactionPolicy);
        Dataset dataset = null;
        try {

            IDatasetDetails datasetDetails = null;
            Dataset ds = MetadataManager.INSTANCE.getDataset(metadataProvider.getMetadataTxnContext(), dataverseName,
                    datasetName);
            if (ds != null) {
                if (dd.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("A dataset with this name " + datasetName + " already exists.");
                }
            }
            Datatype dt = MetadataManager.INSTANCE.getDatatype(metadataProvider.getMetadataTxnContext(),
                    itemTypeDataverseName, itemTypeName);
            if (dt == null) {
                throw new AlgebricksException(": type " + itemTypeName + " could not be found.");
            }
            String ngName = ngNameId != null ? ngNameId.getValue()
                    : configureNodegroupForDataset(dd, dataverseName, mdTxnCtx);

            if (compactionPolicy == null) {
                compactionPolicy = GlobalConfig.DEFAULT_COMPACTION_POLICY_NAME;
                compactionPolicyProperties = GlobalConfig.DEFAULT_COMPACTION_POLICY_PROPERTIES;
            } else {
                validateCompactionPolicy(compactionPolicy, compactionPolicyProperties, mdTxnCtx, false);
            }
            switch (dd.getDatasetType()) {
                case INTERNAL:
                    IAType itemType = dt.getDatatype();
                    if (itemType.getTypeTag() != ATypeTag.RECORD) {
                        throw new AlgebricksException("Dataset type has to be a record type.");
                    }

                    IAType metaItemType = null;
                    if (metaItemTypeDataverseName != null && metaItemTypeName != null) {
                        metaItemType = metadataProvider.findType(metaItemTypeDataverseName, metaItemTypeName);
                    }
                    if (metaItemType != null && metaItemType.getTypeTag() != ATypeTag.RECORD) {
                        throw new AlgebricksException("Dataset meta type has to be a record type.");
                    }
                    ARecordType metaRecType = (ARecordType) metaItemType;

                    List<List<String>> partitioningExprs = ((InternalDetailsDecl) dd.getDatasetDetailsDecl())
                            .getPartitioningExprs();
                    List<Integer> keySourceIndicators = ((InternalDetailsDecl) dd.getDatasetDetailsDecl())
                            .getKeySourceIndicators();
                    boolean autogenerated = ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).isAutogenerated();
                    ARecordType aRecordType = (ARecordType) itemType;
                    List<IAType> partitioningTypes = ValidateUtil.validatePartitioningExpressions(aRecordType,
                            metaRecType, partitioningExprs, keySourceIndicators, autogenerated);

                    List<String> filterField = ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).getFilterField();
                    if (filterField != null) {
                        ValidateUtil.validateFilterField(aRecordType, filterField);
                    }
                    if (compactionPolicy == null && filterField != null) {
                        // If the dataset has a filter and the user didn't specify a merge
                        // policy, then we will pick the
                        // correlated-prefix as the default merge policy.
                        compactionPolicy = GlobalConfig.DEFAULT_FILTERED_DATASET_COMPACTION_POLICY_NAME;
                        compactionPolicyProperties = GlobalConfig.DEFAULT_COMPACTION_POLICY_PROPERTIES;
                    }
                    datasetDetails = new InternalDatasetDetails(InternalDatasetDetails.FileStructure.BTREE,
                            InternalDatasetDetails.PartitioningStrategy.HASH, partitioningExprs, partitioningExprs,
                            keySourceIndicators, partitioningTypes, autogenerated, filterField, temp);
                    break;
                case EXTERNAL:
                    String adapter = ((ExternalDetailsDecl) dd.getDatasetDetailsDecl()).getAdapter();
                    Map<String, String> properties = ((ExternalDetailsDecl) dd.getDatasetDetailsDecl()).getProperties();

                    datasetDetails = new ExternalDatasetDetails(adapter, properties, new Date(),
                            ExternalDatasetTransactionState.COMMIT);
                    break;
                default:
                    throw new AsterixException("Unknown datatype " + dd.getDatasetType());
            }

            // #. initialize DatasetIdFactory if it is not initialized.
            if (!DatasetIdFactory.isInitialized()) {
                DatasetIdFactory.initialize(MetadataManager.INSTANCE.getMostRecentDatasetId());
            }

            // #. add a new dataset with PendingAddOp
            dataset = new Dataset(dataverseName, datasetName, itemTypeDataverseName, itemTypeName,
                    metaItemTypeDataverseName, metaItemTypeName, ngName, compactionPolicy, compactionPolicyProperties,
                    datasetDetails, dd.getHints(), dsType, DatasetIdFactory.generateDatasetId(),
                    IMetadataEntity.PENDING_ADD_OP);
            MetadataManager.INSTANCE.addDataset(metadataProvider.getMetadataTxnContext(), dataset);

            if (dd.getDatasetType() == DatasetType.INTERNAL) {
                Dataverse dataverse = MetadataManager.INSTANCE.getDataverse(metadataProvider.getMetadataTxnContext(),
                        dataverseName);
                JobSpecification jobSpec = DatasetOperations.createDatasetJobSpec(dataverse, datasetName,
                        metadataProvider);

                // #. make metadataTxn commit before calling runJob.
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                bActiveTxn = false;
                progress.setValue(ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA);

                // #. runJob
                JobUtils.runJob(hcc, jobSpec, true);

                // #. begin new metadataTxn
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
            }

            // #. add a new dataset with PendingNoOp after deleting the dataset with PendingAddOp
            MetadataManager.INSTANCE.dropDataset(metadataProvider.getMetadataTxnContext(), dataverseName, datasetName);
            dataset.setPendingOp(IMetadataEntity.PENDING_NO_OP);
            MetadataManager.INSTANCE.addDataset(metadataProvider.getMetadataTxnContext(), dataset);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }

            if (progress.getValue() == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {

                // #. execute compensation operations
                // remove the index in NC
                // [Notice]
                // As long as we updated(and committed) metadata, we should remove any effect of the job
                // because an exception occurs during runJob.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                CompiledDatasetDropStatement cds = new CompiledDatasetDropStatement(dataverseName, datasetName);
                try {
                    JobSpecification jobSpec = DatasetOperations.createDropDatasetJobSpec(cds, metadataProvider);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    bActiveTxn = false;
                    JobUtils.runJob(hcc, jobSpec, true);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    if (bActiveTxn) {
                        abort(e, e2, mdTxnCtx);
                    }
                }

                // remove the record from the metadata.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                try {
                    MetadataManager.INSTANCE.dropDataset(metadataProvider.getMetadataTxnContext(), dataverseName,
                            datasetName);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx);
                    throw new IllegalStateException("System is inconsistent state: pending dataset(" + dataverseName
                            + "." + datasetName + ") couldn't be removed from the metadata", e);
                }
            }

            throw e;
        } finally {
            MetadataLockManager.INSTANCE.createDatasetEnd(dataverseName, itemTypeDataverseName,
                    itemTypeDataverseName + "." + itemTypeName, metaItemTypeDataverseName,
                    metaItemTypeDataverseName + "." + metaItemTypeName, nodegroupName, compactionPolicy,
                    dataverseName + "." + datasetName, defaultCompactionPolicy);
        }
    }

    protected void validateIfResourceIsActiveInFeed(String dataverseName, String datasetName) throws AsterixException {
        StringBuilder builder = null;
        IActiveEntityEventsListener[] listeners = ActiveJobNotificationHandler.INSTANCE.getEventListeners();
        for (IActiveEntityEventsListener listener : listeners) {
            if (listener.isEntityUsingDataset(dataverseName, datasetName)) {
                if (builder == null) {
                    builder = new StringBuilder();
                }
                builder.append(listener.getEntityId() + "\n");
            }
        }
        if (builder != null) {
            throw new AsterixException("Dataset " + dataverseName + "." + datasetName + " is currently being "
                    + "fed into by the following active entities.\n" + builder.toString());
        }
    }

    protected String getNodeGroupName(Identifier ngNameId, DatasetDecl dd, String dataverse) {
        if (ngNameId != null) {
            return ngNameId.getValue();
        }
        String hintValue = dd.getHints().get(DatasetNodegroupCardinalityHint.NAME);
        if (hintValue == null) {
            return MetadataConstants.METADATA_DEFAULT_NODEGROUP_NAME;
        } else {
            return dataverse + ":" + dd.getName().getValue();
        }
    }

    protected String configureNodegroupForDataset(DatasetDecl dd, String dataverse, MetadataTransactionContext mdTxnCtx)
            throws AsterixException {
        int nodegroupCardinality;
        String nodegroupName;
        String hintValue = dd.getHints().get(DatasetNodegroupCardinalityHint.NAME);
        if (hintValue == null) {
            nodegroupName = MetadataConstants.METADATA_DEFAULT_NODEGROUP_NAME;
            return nodegroupName;
        } else {
            int numChosen = 0;
            boolean valid = DatasetHints.validate(DatasetNodegroupCardinalityHint.NAME,
                    dd.getHints().get(DatasetNodegroupCardinalityHint.NAME)).first;
            if (!valid) {
                throw new AsterixException("Incorrect use of hint:" + DatasetNodegroupCardinalityHint.NAME);
            } else {
                nodegroupCardinality = Integer.parseInt(dd.getHints().get(DatasetNodegroupCardinalityHint.NAME));
            }
            List<String> nodeNames = AsterixAppContextInfo.INSTANCE.getMetadataProperties().getNodeNames();
            List<String> nodeNamesClone = new ArrayList<>(nodeNames);
            String metadataNodeName = AsterixAppContextInfo.INSTANCE.getMetadataProperties().getMetadataNodeName();
            List<String> selectedNodes = new ArrayList<>();
            selectedNodes.add(metadataNodeName);
            numChosen++;
            nodeNamesClone.remove(metadataNodeName);

            if (numChosen < nodegroupCardinality) {
                Random random = new Random();
                String[] nodes = nodeNamesClone.toArray(new String[] {});
                int[] b = new int[nodeNamesClone.size()];
                for (int i = 0; i < b.length; i++) {
                    b[i] = i;
                }

                for (int i = 0; i < nodegroupCardinality - numChosen; i++) {
                    int selected = i + random.nextInt(nodeNamesClone.size() - i);
                    int selNodeIndex = b[selected];
                    selectedNodes.add(nodes[selNodeIndex]);
                    int temp = b[0];
                    b[0] = b[selected];
                    b[selected] = temp;
                }
            }
            nodegroupName = dataverse + ":" + dd.getName().getValue();
            MetadataManager.INSTANCE.addNodegroup(mdTxnCtx, new NodeGroup(nodegroupName, selectedNodes));
            return nodegroupName;
        }

    }

    protected void handleCreateIndexStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        ProgressState progress = ProgressState.NO_PROGRESS;
        CreateIndexStatement stmtCreateIndex = (CreateIndexStatement) stmt;
        String dataverseName = getActiveDataverse(stmtCreateIndex.getDataverseName());
        String datasetName = stmtCreateIndex.getDatasetName().getValue();
        List<Integer> keySourceIndicators = stmtCreateIndex.getFieldSourceIndicators();

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        MetadataLockManager.INSTANCE.createIndexBegin(dataverseName, dataverseName + "." + datasetName);

        String indexName = null;
        JobSpecification spec = null;
        Dataset ds = null;
        // For external datasets
        ArrayList<ExternalFile> externalFilesSnapshot = null;
        boolean firstExternalDatasetIndex = false;
        boolean filesIndexReplicated = false;
        Index filesIndex = null;
        boolean datasetLocked = false;
        try {
            ds = MetadataManager.INSTANCE.getDataset(metadataProvider.getMetadataTxnContext(), dataverseName,
                    datasetName);
            if (ds == null) {
                throw new AlgebricksException(
                        "There is no dataset with this name " + datasetName + " in dataverse " + dataverseName);
            }

            indexName = stmtCreateIndex.getIndexName().getValue();
            Index idx = MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(), dataverseName,
                    datasetName, indexName);
            Datatype dt = MetadataManager.INSTANCE.getDatatype(metadataProvider.getMetadataTxnContext(),
                    ds.getItemTypeDataverseName(), ds.getItemTypeName());
            ARecordType aRecordType = (ARecordType) dt.getDatatype();
            ARecordType metaRecordType = null;
            if (ds.hasMetaPart()) {
                Datatype metaDt = MetadataManager.INSTANCE.getDatatype(metadataProvider.getMetadataTxnContext(),
                        ds.getMetaItemTypeDataverseName(), ds.getMetaItemTypeName());
                metaRecordType = (ARecordType) metaDt.getDatatype();
            }

            List<List<String>> indexFields = new ArrayList<>();
            List<IAType> indexFieldTypes = new ArrayList<>();
            int keyIndex = 0;
            for (Pair<List<String>, TypeExpression> fieldExpr : stmtCreateIndex.getFieldExprs()) {
                IAType fieldType = null;
                ARecordType subType = KeyFieldTypeUtils.chooseSource(keySourceIndicators, keyIndex, aRecordType,
                        metaRecordType);
                boolean isOpen = subType.isOpen();
                int i = 0;
                if (fieldExpr.first.size() > 1 && !isOpen) {
                    while (i < fieldExpr.first.size() - 1 && !isOpen) {
                        subType = (ARecordType) subType.getFieldType(fieldExpr.first.get(i));
                        i++;
                        isOpen = subType.isOpen();
                    }
                }
                if (fieldExpr.second == null) {
                    fieldType = subType.getSubFieldType(fieldExpr.first.subList(i, fieldExpr.first.size()));
                } else {
                    if (!stmtCreateIndex.isEnforced()) {
                        throw new AlgebricksException("Cannot create typed index on \"" + fieldExpr.first
                                + "\" field without enforcing it's type");
                    }
                    if (!isOpen) {
                        throw new AlgebricksException("Typed index on \"" + fieldExpr.first
                                + "\" field could be created only for open datatype");
                    }
                    if (stmtCreateIndex.hasMetaField()) {
                        throw new AlgebricksException("Typed open index can only be created on the record part");
                    }
                    Map<TypeSignature, IAType> typeMap = TypeTranslator.computeTypes(mdTxnCtx, fieldExpr.second,
                            indexName, dataverseName);
                    TypeSignature typeSignature = new TypeSignature(dataverseName, indexName);
                    fieldType = typeMap.get(typeSignature);
                }
                if (fieldType == null) {
                    throw new AlgebricksException(
                            "Unknown type " + (fieldExpr.second == null ? fieldExpr.first : fieldExpr.second));
                }

                indexFields.add(fieldExpr.first);
                indexFieldTypes.add(fieldType);
                ++keyIndex;
            }

            ValidateUtil.validateKeyFields(aRecordType, metaRecordType, indexFields, keySourceIndicators,
                    indexFieldTypes, stmtCreateIndex.getIndexType());

            if (idx != null) {
                if (stmtCreateIndex.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("An index with this name " + indexName + " already exists.");
                }
            }

            // Checks whether a user is trying to create an inverted secondary index on a dataset
            // with a variable-length primary key.
            // Currently, we do not support this. Therefore, as a temporary solution, we print an
            // error message and stop.
            if (stmtCreateIndex.getIndexType() == IndexType.SINGLE_PARTITION_WORD_INVIX
                    || stmtCreateIndex.getIndexType() == IndexType.SINGLE_PARTITION_NGRAM_INVIX
                    || stmtCreateIndex.getIndexType() == IndexType.LENGTH_PARTITIONED_WORD_INVIX
                    || stmtCreateIndex.getIndexType() == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX) {
                List<List<String>> partitioningKeys = DatasetUtils.getPartitioningKeys(ds);
                for (List<String> partitioningKey : partitioningKeys) {
                    IAType keyType = aRecordType.getSubFieldType(partitioningKey);
                    ITypeTraits typeTrait = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);

                    // If it is not a fixed length
                    if (typeTrait.getFixedLength() < 0) {
                        throw new AlgebricksException("The keyword or ngram index -" + indexName
                                + " cannot be created on the dataset -" + datasetName
                                + " due to its variable-length primary key field - " + partitioningKey);
                    }

                }
            }

            if (ds.getDatasetType() == DatasetType.INTERNAL) {
                validateIfResourceIsActiveInFeed(dataverseName, datasetName);
            } else {
                // External dataset
                // Check if the dataset is indexible
                if (!ExternalIndexingOperations.isIndexible((ExternalDatasetDetails) ds.getDatasetDetails())) {
                    throw new AlgebricksException(
                            "dataset using " + ((ExternalDatasetDetails) ds.getDatasetDetails()).getAdapter()
                                    + " Adapter can't be indexed");
                }
                // Check if the name of the index is valid
                if (!ExternalIndexingOperations.isValidIndexName(datasetName, indexName)) {
                    throw new AlgebricksException("external dataset index name is invalid");
                }

                // Check if the files index exist
                filesIndex = MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(), dataverseName,
                        datasetName, ExternalIndexingOperations.getFilesIndexName(datasetName));
                firstExternalDatasetIndex = filesIndex == null;
                // Lock external dataset
                ExternalDatasetsRegistry.INSTANCE.buildIndexBegin(ds, firstExternalDatasetIndex);
                datasetLocked = true;
                if (firstExternalDatasetIndex) {
                    // Verify that no one has created an index before we acquire the lock
                    filesIndex = MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(),
                            dataverseName, datasetName, ExternalIndexingOperations.getFilesIndexName(datasetName));
                    if (filesIndex != null) {
                        ExternalDatasetsRegistry.INSTANCE.buildIndexEnd(ds, firstExternalDatasetIndex);
                        firstExternalDatasetIndex = false;
                        ExternalDatasetsRegistry.INSTANCE.buildIndexBegin(ds, firstExternalDatasetIndex);
                    }
                }
                if (firstExternalDatasetIndex) {
                    // Get snapshot from External File System
                    externalFilesSnapshot = ExternalIndexingOperations.getSnapshotFromExternalFileSystem(ds);
                    // Add an entry for the files index
                    filesIndex = new Index(dataverseName, datasetName,
                            ExternalIndexingOperations.getFilesIndexName(datasetName), IndexType.BTREE,
                            ExternalIndexingOperations.FILE_INDEX_FIELD_NAMES, null,
                            ExternalIndexingOperations.FILE_INDEX_FIELD_TYPES, false, false,
                            IMetadataEntity.PENDING_ADD_OP);
                    MetadataManager.INSTANCE.addIndex(metadataProvider.getMetadataTxnContext(), filesIndex);
                    // Add files to the external files index
                    for (ExternalFile file : externalFilesSnapshot) {
                        MetadataManager.INSTANCE.addExternalFile(mdTxnCtx, file);
                    }
                    // This is the first index for the external dataset, replicate the files index
                    spec = ExternalIndexingOperations.buildFilesIndexReplicationJobSpec(ds, externalFilesSnapshot,
                            metadataProvider, true);
                    if (spec == null) {
                        throw new AsterixException(
                                "Failed to create job spec for replicating Files Index For external dataset");
                    }
                    filesIndexReplicated = true;
                    JobUtils.runJob(hcc, spec, true);
                }
            }

            // check whether there exists another enforced index on the same field
            if (stmtCreateIndex.isEnforced()) {
                List<Index> indexes = MetadataManager.INSTANCE
                        .getDatasetIndexes(metadataProvider.getMetadataTxnContext(), dataverseName, datasetName);
                for (Index index : indexes) {
                    if (index.getKeyFieldNames().equals(indexFields)
                            && !index.getKeyFieldTypes().equals(indexFieldTypes) && index.isEnforcingKeyFileds()) {
                        throw new AsterixException("Cannot create index " + indexName + " , enforced index "
                                + index.getIndexName() + " on field \"" + StringUtils.join(indexFields, ',')
                                + "\" is already defined with type \"" + index.getKeyFieldTypes() + "\"");
                    }
                }
            }

            // #. add a new index with PendingAddOp
            Index index = new Index(dataverseName, datasetName, indexName, stmtCreateIndex.getIndexType(), indexFields,
                    keySourceIndicators, indexFieldTypes, stmtCreateIndex.getGramLength(), stmtCreateIndex.isEnforced(),
                    false, IMetadataEntity.PENDING_ADD_OP);
            MetadataManager.INSTANCE.addIndex(metadataProvider.getMetadataTxnContext(), index);

            ARecordType enforcedType = null;
            if (stmtCreateIndex.isEnforced()) {
                enforcedType = createEnforcedType(aRecordType, Lists.newArrayList(index));
            }

            // #. prepare to create the index artifact in NC.
            CompiledCreateIndexStatement cis = new CompiledCreateIndexStatement(index.getIndexName(), dataverseName,
                    index.getDatasetName(), index.getKeyFieldNames(), index.getKeyFieldTypes(),
                    index.isEnforcingKeyFileds(), index.getGramLength(), index.getIndexType());
            spec = IndexOperations.buildSecondaryIndexCreationJobSpec(cis, aRecordType, metaRecordType,
                    keySourceIndicators, enforcedType, metadataProvider);
            if (spec == null) {
                throw new AsterixException("Failed to create job spec for creating index '"
                        + stmtCreateIndex.getDatasetName() + "." + stmtCreateIndex.getIndexName() + "'");
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            progress = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;

            // #. create the index artifact in NC.
            JobUtils.runJob(hcc, spec, true);

            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            bActiveTxn = true;
            metadataProvider.setMetadataTxnContext(mdTxnCtx);

            // #. load data into the index in NC.
            cis = new CompiledCreateIndexStatement(index.getIndexName(), dataverseName, index.getDatasetName(),
                    index.getKeyFieldNames(), index.getKeyFieldTypes(), index.isEnforcingKeyFileds(),
                    index.getGramLength(), index.getIndexType());

            spec = IndexOperations.buildSecondaryIndexLoadingJobSpec(cis, aRecordType, metaRecordType,
                    keySourceIndicators, enforcedType, metadataProvider);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            JobUtils.runJob(hcc, spec, true);

            // #. begin new metadataTxn
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            bActiveTxn = true;
            metadataProvider.setMetadataTxnContext(mdTxnCtx);

            // #. add another new index with PendingNoOp after deleting the index with PendingAddOp
            MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(), dataverseName, datasetName,
                    indexName);
            index.setPendingOp(IMetadataEntity.PENDING_NO_OP);
            MetadataManager.INSTANCE.addIndex(metadataProvider.getMetadataTxnContext(), index);
            // add another new files index with PendingNoOp after deleting the index with
            // PendingAddOp
            if (firstExternalDatasetIndex) {
                MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(), dataverseName, datasetName,
                        filesIndex.getIndexName());
                filesIndex.setPendingOp(IMetadataEntity.PENDING_NO_OP);
                MetadataManager.INSTANCE.addIndex(metadataProvider.getMetadataTxnContext(), filesIndex);
                // update transaction timestamp
                ((ExternalDatasetDetails) ds.getDatasetDetails()).setRefreshTimestamp(new Date());
                MetadataManager.INSTANCE.updateDataset(mdTxnCtx, ds);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);

        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            // If files index was replicated for external dataset, it should be cleaned up on NC side
            if (filesIndexReplicated) {
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                CompiledIndexDropStatement cds = new CompiledIndexDropStatement(dataverseName, datasetName,
                        ExternalIndexingOperations.getFilesIndexName(datasetName));
                try {
                    JobSpecification jobSpec = ExternalIndexingOperations.buildDropFilesIndexJobSpec(cds,
                            metadataProvider, ds);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    bActiveTxn = false;
                    JobUtils.runJob(hcc, jobSpec, true);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    if (bActiveTxn) {
                        abort(e, e2, mdTxnCtx);
                    }
                }
            }

            if (progress == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {
                // #. execute compensation operations
                // remove the index in NC
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                CompiledIndexDropStatement cds = new CompiledIndexDropStatement(dataverseName, datasetName, indexName);
                try {
                    JobSpecification jobSpec = IndexOperations.buildDropSecondaryIndexJobSpec(cds, metadataProvider,
                            ds);

                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    bActiveTxn = false;
                    JobUtils.runJob(hcc, jobSpec, true);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    if (bActiveTxn) {
                        abort(e, e2, mdTxnCtx);
                    }
                }

                if (firstExternalDatasetIndex) {
                    mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                    metadataProvider.setMetadataTxnContext(mdTxnCtx);
                    try {
                        // Drop External Files from metadata
                        MetadataManager.INSTANCE.dropDatasetExternalFiles(mdTxnCtx, ds);
                        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    } catch (Exception e2) {
                        e.addSuppressed(e2);
                        abort(e, e2, mdTxnCtx);
                        throw new IllegalStateException("System is inconsistent state: pending files for("
                                + dataverseName + "." + datasetName + ") couldn't be removed from the metadata", e);
                    }
                    mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                    metadataProvider.setMetadataTxnContext(mdTxnCtx);
                    try {
                        // Drop the files index from metadata
                        MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(), dataverseName,
                                datasetName, ExternalIndexingOperations.getFilesIndexName(datasetName));
                        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    } catch (Exception e2) {
                        e.addSuppressed(e2);
                        abort(e, e2, mdTxnCtx);
                        throw new IllegalStateException("System is inconsistent state: pending index(" + dataverseName
                                + "." + datasetName + "." + ExternalIndexingOperations.getFilesIndexName(datasetName)
                                + ") couldn't be removed from the metadata", e);
                    }
                }
                // remove the record from the metadata.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                try {
                    MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(), dataverseName,
                            datasetName, indexName);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx);
                    throw new IllegalStateException("System is in inconsistent state: pending index(" + dataverseName
                            + "." + datasetName + "." + indexName + ") couldn't be removed from the metadata", e);
                }
            }
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.createIndexEnd(dataverseName, dataverseName + "." + datasetName);
            if (datasetLocked) {
                ExternalDatasetsRegistry.INSTANCE.buildIndexEnd(ds, firstExternalDatasetIndex);
            }
        }
    }

    protected void handleCreateTypeStatement(AqlMetadataProvider metadataProvider, Statement stmt) throws Exception {
        TypeDecl stmtCreateType = (TypeDecl) stmt;
        String dataverseName = getActiveDataverse(stmtCreateType.getDataverseName());
        String typeName = stmtCreateType.getIdent().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockManager.INSTANCE.createTypeBegin(dataverseName, dataverseName + "." + typeName);
        try {
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverseName);
            if (dv == null) {
                throw new AlgebricksException("Unknown dataverse " + dataverseName);
            }
            Datatype dt = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataverseName, typeName);
            if (dt != null) {
                if (!stmtCreateType.getIfNotExists()) {
                    throw new AlgebricksException("A datatype with this name " + typeName + " already exists.");
                }
            } else {
                if (BuiltinTypeMap.getBuiltinType(typeName) != null) {
                    throw new AlgebricksException("Cannot redefine builtin type " + typeName + ".");
                } else {
                    Map<TypeSignature, IAType> typeMap = TypeTranslator.computeTypes(mdTxnCtx,
                            stmtCreateType.getTypeDef(), stmtCreateType.getIdent().getValue(), dataverseName);
                    TypeSignature typeSignature = new TypeSignature(dataverseName, typeName);
                    IAType type = typeMap.get(typeSignature);
                    MetadataManager.INSTANCE.addDatatype(mdTxnCtx, new Datatype(dataverseName, typeName, type, false));
                }
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.createTypeEnd(dataverseName, dataverseName + "." + typeName);
        }
    }

    protected void handleDataverseDropStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        DataverseDropStatement stmtDelete = (DataverseDropStatement) stmt;
        String dataverseName = stmtDelete.getDataverseName().getValue();
        if (dataverseName.equals(MetadataBuiltinEntities.DEFAULT_DATAVERSE_NAME)) {
            throw new HyracksDataException(
                    MetadataBuiltinEntities.DEFAULT_DATAVERSE_NAME + " dataverse can't be dropped");
        }

        ProgressState progress = ProgressState.NO_PROGRESS;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockManager.INSTANCE.acquireDataverseWriteLock(dataverseName);
        List<JobSpecification> jobsToExecute = new ArrayList<>();
        try {
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverseName);
            if (dv == null) {
                if (stmtDelete.getIfExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("There is no dataverse with this name " + dataverseName + ".");
                }
            }
            // # disconnect all feeds from any datasets in the dataverse.
            IActiveEntityEventsListener[] activeListeners = ActiveJobNotificationHandler.INSTANCE.getEventListeners();
            Identifier dvId = new Identifier(dataverseName);
            for (IActiveEntityEventsListener listener : activeListeners) {
                EntityId activeEntityId = listener.getEntityId();
                if (activeEntityId.getExtensionName().equals(Feed.EXTENSION_NAME)
                        && activeEntityId.getDataverse().equals(dataverseName)) {
                    stopFeedBeforeDelete(new Pair<>(dvId, new Identifier(activeEntityId.getEntityName())),
                            metadataProvider, hcc);
                    // prepare job to remove feed log storage
                    jobsToExecute.add(FeedOperations.buildRemoveFeedStorageJob(
                            MetadataManager.INSTANCE.getFeed(mdTxnCtx, dataverseName, activeEntityId.getEntityName())));
                }
            }

            // #. prepare jobs which will drop corresponding datasets with indexes.
            List<Dataset> datasets = MetadataManager.INSTANCE.getDataverseDatasets(mdTxnCtx, dataverseName);
            for (int j = 0; j < datasets.size(); j++) {
                String datasetName = datasets.get(j).getDatasetName();
                DatasetType dsType = datasets.get(j).getDatasetType();
                if (dsType == DatasetType.INTERNAL) {
                    List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName,
                            datasetName);
                    for (int k = 0; k < indexes.size(); k++) {
                        if (indexes.get(k).isSecondaryIndex()) {
                            CompiledIndexDropStatement cds = new CompiledIndexDropStatement(dataverseName, datasetName,
                                    indexes.get(k).getIndexName());
                            jobsToExecute.add(IndexOperations.buildDropSecondaryIndexJobSpec(cds, metadataProvider,
                                    datasets.get(j)));
                        }
                    }

                    CompiledDatasetDropStatement cds = new CompiledDatasetDropStatement(dataverseName, datasetName);
                    jobsToExecute.add(DatasetOperations.createDropDatasetJobSpec(cds, metadataProvider));
                } else {
                    // External dataset
                    List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName,
                            datasetName);
                    for (int k = 0; k < indexes.size(); k++) {
                        if (ExternalIndexingOperations.isFileIndex(indexes.get(k))) {
                            CompiledIndexDropStatement cds = new CompiledIndexDropStatement(dataverseName, datasetName,
                                    indexes.get(k).getIndexName());
                            jobsToExecute.add(ExternalIndexingOperations.buildDropFilesIndexJobSpec(cds,
                                    metadataProvider, datasets.get(j)));
                        } else {
                            CompiledIndexDropStatement cds = new CompiledIndexDropStatement(dataverseName, datasetName,
                                    indexes.get(k).getIndexName());
                            jobsToExecute.add(IndexOperations.buildDropSecondaryIndexJobSpec(cds, metadataProvider,
                                    datasets.get(j)));
                        }
                    }
                    ExternalDatasetsRegistry.INSTANCE.removeDatasetInfo(datasets.get(j));
                }
            }
            jobsToExecute.add(DataverseOperations.createDropDataverseJobSpec(dv, metadataProvider));
            // #. mark PendingDropOp on the dataverse record by
            // first, deleting the dataverse record from the DATAVERSE_DATASET
            // second, inserting the dataverse record with the PendingDropOp value into the
            // DATAVERSE_DATASET
            MetadataManager.INSTANCE.dropDataverse(mdTxnCtx, dataverseName);
            MetadataManager.INSTANCE.addDataverse(mdTxnCtx,
                    new Dataverse(dataverseName, dv.getDataFormat(), IMetadataEntity.PENDING_DROP_OP));

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            progress = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;

            for (JobSpecification jobSpec : jobsToExecute) {
                JobUtils.runJob(hcc, jobSpec, true);
            }

            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            bActiveTxn = true;
            metadataProvider.setMetadataTxnContext(mdTxnCtx);

            // #. finally, delete the dataverse.
            MetadataManager.INSTANCE.dropDataverse(mdTxnCtx, dataverseName);
            if (activeDefaultDataverse != null && activeDefaultDataverse.getDataverseName() == dataverseName) {
                activeDefaultDataverse = null;
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }

            if (progress == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {
                if (activeDefaultDataverse != null && activeDefaultDataverse.getDataverseName() == dataverseName) {
                    activeDefaultDataverse = null;
                }

                // #. execute compensation operations
                // remove the all indexes in NC
                try {
                    for (JobSpecification jobSpec : jobsToExecute) {
                        JobUtils.runJob(hcc, jobSpec, true);
                    }
                } catch (Exception e2) {
                    // do no throw exception since still the metadata needs to be compensated.
                    e.addSuppressed(e2);
                }

                // remove the record from the metadata.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                try {
                    MetadataManager.INSTANCE.dropDataverse(mdTxnCtx, dataverseName);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx);
                    throw new IllegalStateException("System is inconsistent state: pending dataverse(" + dataverseName
                            + ") couldn't be removed from the metadata", e);
                }
            }

            throw e;
        } finally {
            MetadataLockManager.INSTANCE.releaseDataverseWriteLock(dataverseName);
        }
    }

    protected void stopFeedBeforeDelete(Pair<Identifier, Identifier> feedNameComp, AqlMetadataProvider metadataProvider,
            IHyracksClientConnection hcc) {
        StopFeedStatement disStmt = new StopFeedStatement(feedNameComp);
        try {
            handleStopFeedStatement(metadataProvider, disStmt);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Stopped feed " + feedNameComp.second.getValue());
            }
        } catch (Exception exception) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Unable to stop feed " + feedNameComp.second.getValue() + exception);
            }
        }
    }

    protected Dataset getDataset(MetadataTransactionContext mdTxnCtx, String dataverseName, String datasetName)
            throws MetadataException {
        return MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);
    }

    public void handleDatasetDropStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        DropDatasetStatement stmtDelete = (DropDatasetStatement) stmt;
        String dataverseName = getActiveDataverse(stmtDelete.getDataverseName());
        String datasetName = stmtDelete.getDatasetName().getValue();
        MutableObject<ProgressState> progress = new MutableObject<>(ProgressState.NO_PROGRESS);
        MutableObject<MetadataTransactionContext> mdTxnCtx = new MutableObject<>(
                MetadataManager.INSTANCE.beginTransaction());
        MutableBoolean bActiveTxn = new MutableBoolean(true);
        metadataProvider.setMetadataTxnContext(mdTxnCtx.getValue());
        MetadataLockManager.INSTANCE.dropDatasetBegin(dataverseName, dataverseName + "." + datasetName);
        List<JobSpecification> jobsToExecute = new ArrayList<>();
        try {
            Dataset ds = MetadataManager.INSTANCE.getDataset(mdTxnCtx.getValue(), dataverseName, datasetName);
            if (ds == null) {
                if (stmtDelete.getIfExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx.getValue());
                    return;
                } else {
                    throw new AlgebricksException("There is no dataset with this name " + datasetName + " in dataverse "
                            + dataverseName + ".");
                }
            }

            doDropDataset(ds, datasetName, metadataProvider, mdTxnCtx, jobsToExecute, dataverseName, bActiveTxn,
                    progress, hcc);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx.getValue());
        } catch (Exception e) {
            if (bActiveTxn.booleanValue()) {
                abort(e, e, mdTxnCtx.getValue());
            }

            if (progress.getValue() == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {
                // #. execute compensation operations
                // remove the all indexes in NC
                try {
                    for (JobSpecification jobSpec : jobsToExecute) {
                        JobUtils.runJob(hcc, jobSpec, true);
                    }
                } catch (Exception e2) {
                    // do no throw exception since still the metadata needs to be compensated.
                    e.addSuppressed(e2);
                }

                // remove the record from the metadata.
                mdTxnCtx.setValue(MetadataManager.INSTANCE.beginTransaction());
                metadataProvider.setMetadataTxnContext(mdTxnCtx.getValue());
                try {
                    MetadataManager.INSTANCE.dropDataset(metadataProvider.getMetadataTxnContext(), dataverseName,
                            datasetName);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx.getValue());
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx.getValue());
                    throw new IllegalStateException("System is inconsistent state: pending dataset(" + dataverseName
                            + "." + datasetName + ") couldn't be removed from the metadata", e);
                }
            }

            throw e;
        } finally {
            MetadataLockManager.INSTANCE.dropDatasetEnd(dataverseName, dataverseName + "." + datasetName);
        }
    }

    protected void doDropDataset(Dataset ds, String datasetName, AqlMetadataProvider metadataProvider,
            MutableObject<MetadataTransactionContext> mdTxnCtx, List<JobSpecification> jobsToExecute,
            String dataverseName, MutableBoolean bActiveTxn, MutableObject<ProgressState> progress,
            IHyracksClientConnection hcc) throws Exception {
        Map<FeedConnectionId, Pair<JobSpecification, Boolean>> disconnectJobList = new HashMap<>();
        if (ds.getDatasetType() == DatasetType.INTERNAL) {
            // prepare job spec(s) that would disconnect any active feeds involving the dataset.
            IActiveEntityEventsListener[] activeListeners = ActiveJobNotificationHandler.INSTANCE.getEventListeners();
            for (IActiveEntityEventsListener listener : activeListeners) {
                if (listener.isEntityUsingDataset(dataverseName, datasetName)) {
                    throw new AsterixException(
                            "Can't drop dataset since it is connected to active entity: " + listener.getEntityId());
                }
            }

            // #. prepare jobs to drop the datatset and the indexes in NC
            List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx.getValue(), dataverseName,
                    datasetName);
            for (int j = 0; j < indexes.size(); j++) {
                if (indexes.get(j).isSecondaryIndex()) {
                    CompiledIndexDropStatement cds = new CompiledIndexDropStatement(dataverseName, datasetName,
                            indexes.get(j).getIndexName());
                    jobsToExecute.add(IndexOperations.buildDropSecondaryIndexJobSpec(cds, metadataProvider, ds));
                }
            }
            CompiledDatasetDropStatement cds = new CompiledDatasetDropStatement(dataverseName, datasetName);
            jobsToExecute.add(DatasetOperations.createDropDatasetJobSpec(cds, metadataProvider));

            // #. mark the existing dataset as PendingDropOp
            MetadataManager.INSTANCE.dropDataset(mdTxnCtx.getValue(), dataverseName, datasetName);
            MetadataManager.INSTANCE.addDataset(mdTxnCtx.getValue(),
                    new Dataset(dataverseName, datasetName, ds.getItemTypeDataverseName(), ds.getItemTypeName(),
                            ds.getMetaItemTypeDataverseName(), ds.getMetaItemTypeName(), ds.getNodeGroupName(),
                            ds.getCompactionPolicy(), ds.getCompactionPolicyProperties(), ds.getDatasetDetails(),
                            ds.getHints(), ds.getDatasetType(), ds.getDatasetId(), IMetadataEntity.PENDING_DROP_OP));

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx.getValue());
            bActiveTxn.setValue(false);
            progress.setValue(ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA);

            // # disconnect the feeds
            for (Pair<JobSpecification, Boolean> p : disconnectJobList.values()) {
                JobUtils.runJob(hcc, p.first, true);
            }

            // #. run the jobs
            for (JobSpecification jobSpec : jobsToExecute) {
                JobUtils.runJob(hcc, jobSpec, true);
            }

            mdTxnCtx.setValue(MetadataManager.INSTANCE.beginTransaction());
            bActiveTxn.setValue(true);
            metadataProvider.setMetadataTxnContext(mdTxnCtx.getValue());
        } else {
            // External dataset
            ExternalDatasetsRegistry.INSTANCE.removeDatasetInfo(ds);
            // #. prepare jobs to drop the datatset and the indexes in NC
            List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx.getValue(), dataverseName,
                    datasetName);
            for (int j = 0; j < indexes.size(); j++) {
                if (ExternalIndexingOperations.isFileIndex(indexes.get(j))) {
                    CompiledIndexDropStatement cds = new CompiledIndexDropStatement(dataverseName, datasetName,
                            indexes.get(j).getIndexName());
                    jobsToExecute.add(IndexOperations.buildDropSecondaryIndexJobSpec(cds, metadataProvider, ds));
                } else {
                    CompiledIndexDropStatement cds = new CompiledIndexDropStatement(dataverseName, datasetName,
                            indexes.get(j).getIndexName());
                    jobsToExecute.add(ExternalIndexingOperations.buildDropFilesIndexJobSpec(cds, metadataProvider, ds));
                }
            }

            // #. mark the existing dataset as PendingDropOp
            MetadataManager.INSTANCE.dropDataset(mdTxnCtx.getValue(), dataverseName, datasetName);
            MetadataManager.INSTANCE.addDataset(mdTxnCtx.getValue(),
                    new Dataset(dataverseName, datasetName, ds.getItemTypeDataverseName(), ds.getItemTypeName(),
                            ds.getNodeGroupName(), ds.getCompactionPolicy(), ds.getCompactionPolicyProperties(),
                            ds.getDatasetDetails(), ds.getHints(), ds.getDatasetType(), ds.getDatasetId(),
                            IMetadataEntity.PENDING_DROP_OP));

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx.getValue());
            bActiveTxn.setValue(false);
            progress.setValue(ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA);

            // #. run the jobs
            for (JobSpecification jobSpec : jobsToExecute) {
                JobUtils.runJob(hcc, jobSpec, true);
            }
            if (!indexes.isEmpty()) {
                ExternalDatasetsRegistry.INSTANCE.removeDatasetInfo(ds);
            }
            mdTxnCtx.setValue(MetadataManager.INSTANCE.beginTransaction());
            bActiveTxn.setValue(true);
            metadataProvider.setMetadataTxnContext(mdTxnCtx.getValue());
        }

        // #. finally, delete the dataset.
        MetadataManager.INSTANCE.dropDataset(mdTxnCtx.getValue(), dataverseName, datasetName);
        // Drop the associated nodegroup
        String nodegroup = ds.getNodeGroupName();
        if (!nodegroup.equalsIgnoreCase(MetadataConstants.METADATA_DEFAULT_NODEGROUP_NAME)) {
            MetadataManager.INSTANCE.dropNodegroup(mdTxnCtx.getValue(), dataverseName + ":" + datasetName);
        }
    }

    protected void handleIndexDropStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {

        IndexDropStatement stmtIndexDrop = (IndexDropStatement) stmt;
        String datasetName = stmtIndexDrop.getDatasetName().getValue();
        String dataverseName = getActiveDataverse(stmtIndexDrop.getDataverseName());
        ProgressState progress = ProgressState.NO_PROGRESS;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        MetadataLockManager.INSTANCE.dropIndexBegin(dataverseName, dataverseName + "." + datasetName);

        String indexName = null;
        // For external index
        boolean dropFilesIndex = false;
        List<JobSpecification> jobsToExecute = new ArrayList<>();
        try {

            Dataset ds = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);
            if (ds == null) {
                throw new AlgebricksException(
                        "There is no dataset with this name " + datasetName + " in dataverse " + dataverseName);
            }
            IActiveEntityEventsListener[] listeners = ActiveJobNotificationHandler.INSTANCE.getEventListeners();
            StringBuilder builder = null;
            for (IActiveEntityEventsListener listener : listeners) {
                if (listener.isEntityUsingDataset(dataverseName, datasetName)) {
                    if (builder == null) {
                        builder = new StringBuilder();
                    }
                    builder.append(new FeedConnectionId(listener.getEntityId(), datasetName) + "\n");
                }
            }
            if (builder != null) {
                throw new AsterixException("Dataset" + datasetName
                        + " is currently being fed into by the following active entities: " + builder.toString());
            }

            if (ds.getDatasetType() == DatasetType.INTERNAL) {
                indexName = stmtIndexDrop.getIndexName().getValue();
                Index index = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataverseName, datasetName, indexName);
                if (index == null) {
                    if (stmtIndexDrop.getIfExists()) {
                        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                        return;
                    } else {
                        throw new AlgebricksException("There is no index with this name " + indexName + ".");
                    }
                }
                // #. prepare a job to drop the index in NC.
                CompiledIndexDropStatement cds = new CompiledIndexDropStatement(dataverseName, datasetName, indexName);
                jobsToExecute.add(IndexOperations.buildDropSecondaryIndexJobSpec(cds, metadataProvider, ds));

                // #. mark PendingDropOp on the existing index
                MetadataManager.INSTANCE.dropIndex(mdTxnCtx, dataverseName, datasetName, indexName);
                MetadataManager.INSTANCE.addIndex(mdTxnCtx,
                        new Index(dataverseName, datasetName, indexName, index.getIndexType(), index.getKeyFieldNames(),
                                index.getKeyFieldSourceIndicators(), index.getKeyFieldTypes(),
                                index.isEnforcingKeyFileds(), index.isPrimaryIndex(), IMetadataEntity.PENDING_DROP_OP));

                // #. commit the existing transaction before calling runJob.
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                bActiveTxn = false;
                progress = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;

                for (JobSpecification jobSpec : jobsToExecute) {
                    JobUtils.runJob(hcc, jobSpec, true);
                }

                // #. begin a new transaction
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                metadataProvider.setMetadataTxnContext(mdTxnCtx);

                // #. finally, delete the existing index
                MetadataManager.INSTANCE.dropIndex(mdTxnCtx, dataverseName, datasetName, indexName);
            } else {
                // External dataset
                indexName = stmtIndexDrop.getIndexName().getValue();
                Index index = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataverseName, datasetName, indexName);
                if (index == null) {
                    if (stmtIndexDrop.getIfExists()) {
                        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                        return;
                    } else {
                        throw new AlgebricksException("There is no index with this name " + indexName + ".");
                    }
                } else if (ExternalIndexingOperations.isFileIndex(index)) {
                    throw new AlgebricksException("Dropping a dataset's files index is not allowed.");
                }
                // #. prepare a job to drop the index in NC.
                CompiledIndexDropStatement cds = new CompiledIndexDropStatement(dataverseName, datasetName, indexName);
                jobsToExecute.add(IndexOperations.buildDropSecondaryIndexJobSpec(cds, metadataProvider, ds));
                List<Index> datasetIndexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName,
                        datasetName);
                if (datasetIndexes.size() == 2) {
                    dropFilesIndex = true;
                    // only one index + the files index, we need to delete both of the indexes
                    for (Index externalIndex : datasetIndexes) {
                        if (ExternalIndexingOperations.isFileIndex(externalIndex)) {
                            cds = new CompiledIndexDropStatement(dataverseName, datasetName,
                                    externalIndex.getIndexName());
                            jobsToExecute.add(
                                    ExternalIndexingOperations.buildDropFilesIndexJobSpec(cds, metadataProvider, ds));
                            // #. mark PendingDropOp on the existing files index
                            MetadataManager.INSTANCE.dropIndex(mdTxnCtx, dataverseName, datasetName,
                                    externalIndex.getIndexName());
                            MetadataManager.INSTANCE.addIndex(mdTxnCtx,
                                    new Index(dataverseName, datasetName, externalIndex.getIndexName(),
                                            externalIndex.getIndexType(), externalIndex.getKeyFieldNames(),
                                            externalIndex.getKeyFieldSourceIndicators(), index.getKeyFieldTypes(),
                                            index.isEnforcingKeyFileds(), externalIndex.isPrimaryIndex(),
                                            IMetadataEntity.PENDING_DROP_OP));
                        }
                    }
                }

                // #. mark PendingDropOp on the existing index
                MetadataManager.INSTANCE.dropIndex(mdTxnCtx, dataverseName, datasetName, indexName);
                MetadataManager.INSTANCE.addIndex(mdTxnCtx,
                        new Index(dataverseName, datasetName, indexName, index.getIndexType(), index.getKeyFieldNames(),
                                index.getKeyFieldSourceIndicators(), index.getKeyFieldTypes(),
                                index.isEnforcingKeyFileds(), index.isPrimaryIndex(), IMetadataEntity.PENDING_DROP_OP));

                // #. commit the existing transaction before calling runJob.
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                bActiveTxn = false;
                progress = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;

                for (JobSpecification jobSpec : jobsToExecute) {
                    JobUtils.runJob(hcc, jobSpec, true);
                }

                // #. begin a new transaction
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                metadataProvider.setMetadataTxnContext(mdTxnCtx);

                // #. finally, delete the existing index
                MetadataManager.INSTANCE.dropIndex(mdTxnCtx, dataverseName, datasetName, indexName);
                if (dropFilesIndex) {
                    // delete the files index too
                    MetadataManager.INSTANCE.dropIndex(mdTxnCtx, dataverseName, datasetName,
                            ExternalIndexingOperations.getFilesIndexName(datasetName));
                    MetadataManager.INSTANCE.dropDatasetExternalFiles(mdTxnCtx, ds);
                    ExternalDatasetsRegistry.INSTANCE.removeDatasetInfo(ds);
                }
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);

        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }

            if (progress == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {
                // #. execute compensation operations
                // remove the all indexes in NC
                try {
                    for (JobSpecification jobSpec : jobsToExecute) {
                        JobUtils.runJob(hcc, jobSpec, true);
                    }
                } catch (Exception e2) {
                    // do no throw exception since still the metadata needs to be compensated.
                    e.addSuppressed(e2);
                }

                // remove the record from the metadata.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                try {
                    MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(), dataverseName,
                            datasetName, indexName);
                    if (dropFilesIndex) {
                        MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(), dataverseName,
                                datasetName, ExternalIndexingOperations.getFilesIndexName(datasetName));
                    }
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx);
                    throw new IllegalStateException("System is inconsistent state: pending index(" + dataverseName + "."
                            + datasetName + "." + indexName + ") couldn't be removed from the metadata", e);
                }
            }

            throw e;

        } finally {
            MetadataLockManager.INSTANCE.dropIndexEnd(dataverseName, dataverseName + "." + datasetName);
        }
    }

    protected void handleTypeDropStatement(AqlMetadataProvider metadataProvider, Statement stmt) throws Exception {

        TypeDropStatement stmtTypeDrop = (TypeDropStatement) stmt;
        String dataverseName = getActiveDataverse(stmtTypeDrop.getDataverseName());
        String typeName = stmtTypeDrop.getTypeName().getValue();

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockManager.INSTANCE.dropTypeBegin(dataverseName, dataverseName + "." + typeName);

        try {
            Datatype dt = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataverseName, typeName);
            if (dt == null) {
                if (!stmtTypeDrop.getIfExists()) {
                    throw new AlgebricksException("There is no datatype with this name " + typeName + ".");
                }
            } else {
                MetadataManager.INSTANCE.dropDatatype(mdTxnCtx, dataverseName, typeName);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.dropTypeEnd(dataverseName, dataverseName + "." + typeName);
        }
    }

    protected void handleNodegroupDropStatement(AqlMetadataProvider metadataProvider, Statement stmt) throws Exception {
        NodeGroupDropStatement stmtDelete = (NodeGroupDropStatement) stmt;
        String nodegroupName = stmtDelete.getNodeGroupName().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockManager.INSTANCE.acquireNodeGroupWriteLock(nodegroupName);
        try {
            NodeGroup ng = MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, nodegroupName);
            if (ng == null) {
                if (!stmtDelete.getIfExists()) {
                    throw new AlgebricksException("There is no nodegroup with this name " + nodegroupName + ".");
                }
            } else {
                MetadataManager.INSTANCE.dropNodegroup(mdTxnCtx, nodegroupName);
            }

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.releaseNodeGroupWriteLock(nodegroupName);
        }
    }

    protected void handleCreateFunctionStatement(AqlMetadataProvider metadataProvider, Statement stmt)
            throws Exception {
        CreateFunctionStatement cfs = (CreateFunctionStatement) stmt;
        String dataverse = getActiveDataverseName(cfs.getSignature().getNamespace());
        cfs.getSignature().setNamespace(dataverse);
        String functionName = cfs.getaAterixFunction().getName();

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockManager.INSTANCE.functionStatementBegin(dataverse, dataverse + "." + functionName);
        try {

            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverse);
            if (dv == null) {
                throw new AlgebricksException("There is no dataverse with this name " + dataverse + ".");
            }
            Function function = new Function(dataverse, functionName, cfs.getaAterixFunction().getArity(),
                    cfs.getParamList(), Function.RETURNTYPE_VOID, cfs.getFunctionBody(), Function.LANGUAGE_AQL,
                    FunctionKind.SCALAR.toString());
            MetadataManager.INSTANCE.addFunction(mdTxnCtx, function);

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.functionStatementEnd(dataverse, dataverse + "." + functionName);
        }
    }

    protected void handleFunctionDropStatement(AqlMetadataProvider metadataProvider, Statement stmt) throws Exception {
        FunctionDropStatement stmtDropFunction = (FunctionDropStatement) stmt;
        FunctionSignature signature = stmtDropFunction.getFunctionSignature();
        signature.setNamespace(getActiveDataverseName(signature.getNamespace()));
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockManager.INSTANCE.functionStatementBegin(signature.getNamespace(),
                signature.getNamespace() + "." + signature.getName());
        try {
            Function function = MetadataManager.INSTANCE.getFunction(mdTxnCtx, signature);
            if (function == null) {
                if (!stmtDropFunction.getIfExists()) {
                    throw new AlgebricksException("Unknonw function " + signature);
                }
            } else {
                MetadataManager.INSTANCE.dropFunction(mdTxnCtx, signature);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.functionStatementEnd(signature.getNamespace(),
                    signature.getNamespace() + "." + signature.getName());
        }
    }

    protected void handleLoadStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        LoadStatement loadStmt = (LoadStatement) stmt;
        String dataverseName = getActiveDataverse(loadStmt.getDataverseName());
        String datasetName = loadStmt.getDatasetName().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockManager.INSTANCE.modifyDatasetBegin(dataverseName, dataverseName + "." + datasetName);
        try {
            CompiledLoadFromFileStatement cls = new CompiledLoadFromFileStatement(dataverseName,
                    loadStmt.getDatasetName().getValue(), loadStmt.getAdapter(), loadStmt.getProperties(),
                    loadStmt.dataIsAlreadySorted());
            JobSpecification spec = apiFramework.compileQuery(null, metadataProvider, null, 0, null, sessionConfig,
                    cls);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            if (spec != null) {
                JobUtils.runJob(hcc, spec, true);
            }
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.modifyDatasetEnd(dataverseName, dataverseName + "." + datasetName);
        }
    }

    public JobSpecification handleInsertUpsertStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IHyracksDataset hdc, ResultDelivery resultDelivery,
            IStatementExecutor.Stats stats, boolean compileOnly) throws Exception {

        InsertStatement stmtInsertUpsert = (InsertStatement) stmt;

        String dataverseName = getActiveDataverse(stmtInsertUpsert.getDataverseName());
        Query query = stmtInsertUpsert.getQuery();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        if (stmtInsertUpsert.getReturnQuery() != null) {
            if (!stmtInsertUpsert.getReturnQuery().getDatasets().isEmpty()) {
                throw new AsterixException("Cannot use datasets in an insert returning query");
            }
            // returnQuery Rewriting (happens under the same ongoing metadata transaction)
            Pair<Query, Integer> rewrittenReturnQuery = apiFramework.reWriteQuery(declaredFunctions, metadataProvider,
                    stmtInsertUpsert.getReturnQuery(), sessionConfig);

            stmtInsertUpsert.getQuery().setVarCounter(rewrittenReturnQuery.first.getVarCounter());
            stmtInsertUpsert.setRewrittenReturnQuery(rewrittenReturnQuery.first);
            stmtInsertUpsert.addToVarCounter(rewrittenReturnQuery.second);
        }

        MetadataLockManager.INSTANCE.insertDeleteUpsertBegin(dataverseName,
                dataverseName + "." + stmtInsertUpsert.getDatasetName(), query.getDataverses(), query.getDatasets());
        JobSpecification compiled = null;
        try {
            metadataProvider.setWriteTransaction(true);
            CompiledInsertStatement clfrqs = null;
            switch (stmtInsertUpsert.getKind()) {
                case Statement.Kind.INSERT:
                    clfrqs = new CompiledInsertStatement(dataverseName, stmtInsertUpsert.getDatasetName().getValue(),
                            query, stmtInsertUpsert.getVarCounter(), stmtInsertUpsert.getVar(),
                            stmtInsertUpsert.getReturnQuery());
                    break;
                case Statement.Kind.UPSERT:
                    clfrqs = new CompiledUpsertStatement(dataverseName, stmtInsertUpsert.getDatasetName().getValue(),
                            query, stmtInsertUpsert.getVarCounter(), stmtInsertUpsert.getVar(),
                            stmtInsertUpsert.getReturnQuery());
                    break;
                default:
                    throw new AlgebricksException("Unsupported statement type " + stmtInsertUpsert.getKind());
            }
            compiled = rewriteCompileQuery(metadataProvider, query, clfrqs);

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            if (compiled != null && !compileOnly) {
                if (stmtInsertUpsert.getReturnQuery() != null) {
                    handleQueryResult(metadataProvider, hcc, hdc, compiled, resultDelivery, stats);
                } else {
                    JobUtils.runJob(hcc, compiled, true);
                }
            }

        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.insertDeleteUpsertEnd(dataverseName,
                    dataverseName + "." + stmtInsertUpsert.getDatasetName(), query.getDataverses(),
                    query.getDatasets());
        }
        return compiled;
    }

    public void handleDeleteStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {

        DeleteStatement stmtDelete = (DeleteStatement) stmt;
        String dataverseName = getActiveDataverse(stmtDelete.getDataverseName());
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockManager.INSTANCE.insertDeleteUpsertBegin(dataverseName,
                dataverseName + "." + stmtDelete.getDatasetName(), stmtDelete.getDataverses(),
                stmtDelete.getDatasets());

        try {
            metadataProvider.setWriteTransaction(true);
            CompiledDeleteStatement clfrqs = new CompiledDeleteStatement(stmtDelete.getVariableExpr(), dataverseName,
                    stmtDelete.getDatasetName().getValue(), stmtDelete.getCondition(), stmtDelete.getVarCounter(),
                    stmtDelete.getQuery());
            JobSpecification compiled = rewriteCompileQuery(metadataProvider, clfrqs.getQuery(), clfrqs);

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            if (compiled != null) {
                JobUtils.runJob(hcc, compiled, true);
            }

        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.insertDeleteUpsertEnd(dataverseName,
                    dataverseName + "." + stmtDelete.getDatasetName(), stmtDelete.getDataverses(),
                    stmtDelete.getDatasets());
        }
    }

    @Override
    public JobSpecification rewriteCompileQuery(AqlMetadataProvider metadataProvider, Query query,
            ICompiledDmlStatement stmt)
            throws AsterixException, RemoteException, AlgebricksException, JSONException, ACIDException {

        // Query Rewriting (happens under the same ongoing metadata transaction)
        Pair<Query, Integer> reWrittenQuery = apiFramework.reWriteQuery(declaredFunctions, metadataProvider, query,
                sessionConfig);

        // Query Compilation (happens under the same ongoing metadata transaction)
        JobSpecification spec = apiFramework.compileQuery(declaredFunctions, metadataProvider, reWrittenQuery.first,
                reWrittenQuery.second, stmt == null ? null : stmt.getDatasetName(), sessionConfig, stmt);

        return spec;

    }

    protected void handleCreateFeedStatement(AqlMetadataProvider metadataProvider, Statement stmt) throws Exception {
        CreateFeedStatement cfs = (CreateFeedStatement) stmt;
        String dataverseName = getActiveDataverse(cfs.getDataverseName());
        String feedName = cfs.getFeedName().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockManager.INSTANCE.createFeedBegin(dataverseName, dataverseName + "." + feedName);
        Feed feed = null;
        try {
            feed = MetadataManager.INSTANCE.getFeed(metadataProvider.getMetadataTxnContext(), dataverseName, feedName);
            if (feed != null) {
                if (cfs.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("A feed with this name " + feedName + " already exists.");
                }
            }
            String adaptorName = cfs.getAdaptorName();
            feed = new Feed(dataverseName, feedName, adaptorName, cfs.getAdaptorConfiguration());
            FeedMetadataUtil.validateFeed(feed, mdTxnCtx, metadataProvider.getLibraryManager());
            MetadataManager.INSTANCE.addFeed(metadataProvider.getMetadataTxnContext(), feed);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.createFeedEnd(dataverseName, dataverseName + "." + feedName);
        }
    }

    protected void handleCreateFeedPolicyStatement(AqlMetadataProvider metadataProvider, Statement stmt)
            throws AlgebricksException, HyracksDataException {
        String dataverse;
        String policy;
        FeedPolicyEntity newPolicy = null;
        MetadataTransactionContext mdTxnCtx = null;
        CreateFeedPolicyStatement cfps = (CreateFeedPolicyStatement) stmt;
        dataverse = getActiveDataverse(null);
        policy = cfps.getPolicyName();
        MetadataLockManager.INSTANCE.createFeedPolicyBegin(dataverse, dataverse + "." + policy);
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            metadataProvider.setMetadataTxnContext(mdTxnCtx);
            FeedPolicyEntity feedPolicy = MetadataManager.INSTANCE
                    .getFeedPolicy(metadataProvider.getMetadataTxnContext(), dataverse, policy);
            if (feedPolicy != null) {
                if (cfps.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("A policy with this name " + policy + " already exists.");
                }
            }
            boolean extendingExisting = cfps.getSourcePolicyName() != null;
            String description = cfps.getDescription() == null ? "" : cfps.getDescription();
            if (extendingExisting) {
                FeedPolicyEntity sourceFeedPolicy = MetadataManager.INSTANCE
                        .getFeedPolicy(metadataProvider.getMetadataTxnContext(), dataverse, cfps.getSourcePolicyName());
                if (sourceFeedPolicy == null) {
                    sourceFeedPolicy = MetadataManager.INSTANCE.getFeedPolicy(metadataProvider.getMetadataTxnContext(),
                            MetadataConstants.METADATA_DATAVERSE_NAME, cfps.getSourcePolicyName());
                    if (sourceFeedPolicy == null) {
                        throw new AlgebricksException("Unknown policy " + cfps.getSourcePolicyName());
                    }
                }
                Map<String, String> policyProperties = sourceFeedPolicy.getProperties();
                policyProperties.putAll(cfps.getProperties());
                newPolicy = new FeedPolicyEntity(dataverse, policy, description, policyProperties);
            } else {
                Properties prop = new Properties();
                try {
                    InputStream stream = new FileInputStream(cfps.getSourcePolicyFile());
                    prop.load(stream);
                } catch (Exception e) {
                    throw new AlgebricksException("Unable to read policy file" + cfps.getSourcePolicyFile(), e);
                }
                Map<String, String> policyProperties = new HashMap<>();
                for (Entry<Object, Object> entry : prop.entrySet()) {
                    policyProperties.put((String) entry.getKey(), (String) entry.getValue());
                }
                newPolicy = new FeedPolicyEntity(dataverse, policy, description, policyProperties);
            }
            MetadataManager.INSTANCE.addFeedPolicy(mdTxnCtx, newPolicy);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (RemoteException | ACIDException e) {
            abort(e, e, mdTxnCtx);
            throw new HyracksDataException(e);
        } finally {
            MetadataLockManager.INSTANCE.createFeedPolicyEnd(dataverse, dataverse + "." + policy);
        }
    }

    protected void handleDropFeedStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        FeedDropStatement stmtFeedDrop = (FeedDropStatement) stmt;
        String dataverseName = getActiveDataverse(stmtFeedDrop.getDataverseName());
        String feedName = stmtFeedDrop.getFeedName().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockManager.INSTANCE.dropFeedBegin(dataverseName, dataverseName + "." + feedName);

        try {
            Feed feed = MetadataManager.INSTANCE.getFeed(mdTxnCtx, dataverseName, feedName);
            if (feed == null) {
                if (!stmtFeedDrop.getIfExists()) {
                    throw new AlgebricksException("There is no feed with this name " + feedName + ".");
                }
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                return;
            }

            EntityId feedId = new EntityId(Feed.EXTENSION_NAME, dataverseName, feedName);
            FeedEventsListener listener = (FeedEventsListener) ActiveJobNotificationHandler.INSTANCE
                    .getActiveEntityListener(feedId);
            if (listener != null) {
                throw new AlgebricksException("Feed " + feedId
                        + " is currently active and connected to the following dataset(s) \n" + listener.toString());
            } else {
                JobSpecification spec = FeedOperations.buildRemoveFeedStorageJob(
                        MetadataManager.INSTANCE.getFeed(mdTxnCtx, feedId.getDataverse(), feedId.getEntityName()));
                JobUtils.runJob(hcc, spec, true);
                MetadataManager.INSTANCE.dropFeed(mdTxnCtx, dataverseName, feedName);
            }

            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Removed feed " + feedId);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);

        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.dropFeedEnd(dataverseName, dataverseName + "." + feedName);
        }
    }

    protected void handleDropFeedPolicyStatement(AqlMetadataProvider metadataProvider, Statement stmt)
            throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        FeedPolicyDropStatement stmtFeedPolicyDrop = (FeedPolicyDropStatement) stmt;
        String dataverseName = getActiveDataverse(stmtFeedPolicyDrop.getDataverseName());
        String policyName = stmtFeedPolicyDrop.getPolicyName().getValue();
        MetadataLockManager.INSTANCE.dropFeedPolicyBegin(dataverseName, dataverseName + "." + policyName);

        try {
            FeedPolicyEntity feedPolicy = MetadataManager.INSTANCE.getFeedPolicy(mdTxnCtx, dataverseName, policyName);
            if (feedPolicy == null) {
                if (!stmtFeedPolicyDrop.getIfExists()) {
                    throw new AlgebricksException("Unknown policy " + policyName + " in dataverse " + dataverseName);
                }
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                return;
            }
            MetadataManager.INSTANCE.dropFeedPolicy(mdTxnCtx, dataverseName, policyName);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.dropFeedPolicyEnd(dataverseName, dataverseName + "." + policyName);
        }
    }

    private void handleStartFeedStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        StartFeedStatement sfs = (StartFeedStatement) stmt;
        String dataverseName = getActiveDataverse(sfs.getDataverseName());
        String feedName = sfs.getFeedName().getValue();
        // Transcation handler
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        // Runtime handler
        EntityId entityId = new EntityId(Feed.EXTENSION_NAME, dataverseName, feedName);
        // Feed & Feed Connections
        Feed feed = FeedMetadataUtil.validateIfFeedExists(dataverseName, feedName,
                metadataProvider.getMetadataTxnContext());
        List<FeedConnection> feedConnections = MetadataManager.INSTANCE
                .getFeedConections(metadataProvider.getMetadataTxnContext(), dataverseName, feedName);
        // not sure this local variable
        ILangCompilationProvider compilationProvider = new AqlCompilationProvider();
        DefaultStatementExecutorFactory qtFactory = new DefaultStatementExecutorFactory(null);
        // Start
        try {
            MetadataLockManager.INSTANCE.startFeedBegin(dataverseName, dataverseName + "." + feedName, feedConnections);
            // Prepare policy
            FeedEventsListener listener = (FeedEventsListener) ActiveJobNotificationHandler.INSTANCE
                    .getActiveEntityListener(entityId);
            if (listener != null) {
                throw new AlgebricksException("Feed " + feedName + " is started already.");
            }
            listener = new FeedEventsListener(entityId);
            ActiveJobNotificationHandler.INSTANCE.registerListener(listener);
            JobSpecification feedJob = FeedOperations.buildStartFeedJob(metadataProvider, feed, feedConnections,
                    compilationProvider, qtFactory);
            FeedConnectJobInfo cInfo = new FeedConnectJobInfo(entityId, null, ActivityState.CREATED, feedJob);
            listener.setFeedConnectJobInfo(cInfo);
            feedJob.setProperty(ActiveJobNotificationHandler.ACTIVE_ENTITY_PROPERTY_NAME, cInfo);
            JobUtils.runJob(hcc, feedJob,
                    Boolean.valueOf(metadataProvider.getConfig().get(StartFeedStatement.WAIT_FOR_COMPLETION)));
            LOGGER.log(Level.INFO,"Submitted");
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.startFeedEnd(dataverseName, dataverseName + "." + feedName, feedConnections);
        }
    }

    private void handleStopFeedStatement(AqlMetadataProvider metadataProvider, Statement stmt) throws Exception {
        List<String> intakeNodeLocations;
        StopFeedStatement sfst = (StopFeedStatement) stmt;
        String dataverseName = getActiveDataverse(sfst.getDataverseName());
        String feedName = sfst.getFeedName().getValue();
        EntityId feedId = new EntityId(Feed.EXTENSION_NAME, dataverseName, feedName);
        // Obtain runtime info from ActiveListener
        FeedEventsListener listener = (FeedEventsListener) ActiveJobNotificationHandler.INSTANCE
                .getActiveEntityListener(feedId);
        if (listener == null) {
            throw new AlgebricksException("Feed " + feedName + "is not started.");
        }
        intakeNodeLocations = listener.getIntakeLocations();
        // Transaction
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockManager.INSTANCE.StopFeedBegin(dataverseName, feedName);
        try {
            // validate
            // TODO: check feed is actually running.
            FeedMetadataUtil.validateIfFeedExists(dataverseName, feedName, mdTxnCtx);
            // Construct ActiveMessage
            for (String intakeLocation : intakeNodeLocations) {
                FeedOperations.SendStopMessageToNode(feedId, intakeLocation,
                        intakeNodeLocations.indexOf(intakeLocation));
            }
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.StopFeedEnd(dataverseName, feedName);
        }
    }

    private void handleConnectFeedStatement(AqlMetadataProvider metadataProvider, Statement stmt) throws Exception {
        FeedConnection fc;
        ConnectFeedStatement cfs = (ConnectFeedStatement) stmt;
        String dataverseName = getActiveDataverse(cfs.getDataverseName());
        String feedName = cfs.getFeedName();
        String datasetName = cfs.getDatasetName().getValue();
        String policyName = cfs.getPolicy();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        // validation
        Feed feed = FeedMetadataUtil.validateIfFeedExists(dataverseName, feedName,
                metadataProvider.getMetadataTxnContext());
        ARecordType outputType = FeedMetadataUtil.getOutputType(feed, feed.getAdapterConfiguration(),
                ExternalDataConstants.KEY_TYPE_NAME);
        List<FunctionSignature> appliedFunctions = cfs.getAppliedFunctions();
        // Transaction handling
        MetadataLockManager.INSTANCE.connectFeedBegin(dataverseName, dataverseName + "." + datasetName,
                dataverseName + "." + feedName);
        try {
            fc = MetadataManager.INSTANCE.getFeedConnection(metadataProvider.getMetadataTxnContext(), dataverseName,
                    feedName, datasetName);
            if (fc != null) {
                throw new AlgebricksException("Feed" + feedName + " is already connected dataset " + datasetName);
            }
            fc = new FeedConnection(dataverseName, feedName, datasetName, appliedFunctions, policyName,
                    outputType.toString());
            MetadataManager.INSTANCE.addFeedConnection(metadataProvider.getMetadataTxnContext(), fc);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.connectFeedEnd(dataverseName, dataverseName + "." + datasetName,
                    dataverseName + "." + feedName);
        }
    }

    protected void handleDisconnectFeedStatement(AqlMetadataProvider metadataProvider, Statement stmt)
            throws Exception {
        DisconnectFeedStatement cfs = (DisconnectFeedStatement) stmt;
        String dataverseName = getActiveDataverse(cfs.getDataverseName());
        String datasetName = cfs.getDatasetName().getValue();
        String feedName = cfs.getFeedName().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockManager.INSTANCE.disconnectFeedBegin(dataverseName, dataverseName + "." + datasetName,
                dataverseName + "." + cfs.getFeedName());
        try {
            FeedMetadataUtil.validateIfDatasetExists(dataverseName, cfs.getDatasetName().getValue(), mdTxnCtx);
            FeedMetadataUtil.validateIfFeedExists(dataverseName, cfs.getFeedName().getValue(), mdTxnCtx);
            FeedConnection fc = MetadataManager.INSTANCE.getFeedConnection(metadataProvider.getMetadataTxnContext(),
                    dataverseName, feedName, datasetName);
            if (fc == null) {
                throw new AsterixException("Feed " + feedName + " is currently not connected to "
                        + cfs.getDatasetName().getValue() + ". Invalid operation!");
            }
            MetadataManager.INSTANCE.dropFeedConnection(mdTxnCtx, dataverseName, feedName, datasetName);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.disconnectFeedEnd(dataverseName, dataverseName + "." + datasetName,
                    dataverseName + "." + cfs.getFeedName());
        }
    }

    protected void handleCompactStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        CompactStatement compactStatement = (CompactStatement) stmt;
        String dataverseName = getActiveDataverse(compactStatement.getDataverseName());
        String datasetName = compactStatement.getDatasetName().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockManager.INSTANCE.compactBegin(dataverseName, dataverseName + "." + datasetName);
        List<JobSpecification> jobsToExecute = new ArrayList<>();
        try {
            Dataset ds = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);
            if (ds == null) {
                throw new AlgebricksException(
                        "There is no dataset with this name " + datasetName + " in dataverse " + dataverseName + ".");
            }
            String itemTypeName = ds.getItemTypeName();
            Datatype dt = MetadataManager.INSTANCE.getDatatype(metadataProvider.getMetadataTxnContext(),
                    ds.getItemTypeDataverseName(), itemTypeName);
            ARecordType metaRecordType = null;
            if (ds.hasMetaPart()) {
                metaRecordType = (ARecordType) MetadataManager.INSTANCE
                        .getDatatype(metadataProvider.getMetadataTxnContext(), ds.getMetaItemTypeDataverseName(),
                                ds.getMetaItemTypeName())
                        .getDatatype();
            }
            // Prepare jobs to compact the datatset and its indexes
            List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName, datasetName);
            if (indexes.isEmpty()) {
                throw new AlgebricksException(
                        "Cannot compact the extrenal dataset " + datasetName + " because it has no indexes");
            }
            Dataverse dataverse = MetadataManager.INSTANCE.getDataverse(metadataProvider.getMetadataTxnContext(),
                    dataverseName);
            jobsToExecute.add(DatasetOperations.compactDatasetJobSpec(dataverse, datasetName, metadataProvider));
            ARecordType aRecordType = (ARecordType) dt.getDatatype();
            ARecordType enforcedType = createEnforcedType(aRecordType, indexes);
            if (ds.getDatasetType() == DatasetType.INTERNAL) {
                for (int j = 0; j < indexes.size(); j++) {
                    if (indexes.get(j).isSecondaryIndex()) {
                        CompiledIndexCompactStatement cics = new CompiledIndexCompactStatement(dataverseName,
                                datasetName, indexes.get(j).getIndexName(), indexes.get(j).getKeyFieldNames(),
                                indexes.get(j).getKeyFieldTypes(), indexes.get(j).isEnforcingKeyFileds(),
                                indexes.get(j).getGramLength(), indexes.get(j).getIndexType());
                        List<Integer> keySourceIndicators = indexes.get(j).getKeyFieldSourceIndicators();

                        jobsToExecute.add(IndexOperations.buildSecondaryIndexCompactJobSpec(cics, aRecordType,
                                metaRecordType, keySourceIndicators, enforcedType, metadataProvider));
                    }
                }
            } else {
                prepareCompactJobsForExternalDataset(indexes, dataverseName, datasetName, ds, jobsToExecute,
                        aRecordType, metaRecordType, metadataProvider, enforcedType);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            // #. run the jobs
            for (JobSpecification jobSpec : jobsToExecute) {
                JobUtils.runJob(hcc, jobSpec, true);
            }
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.compactEnd(dataverseName, dataverseName + "." + datasetName);
        }
    }

    protected void prepareCompactJobsForExternalDataset(List<Index> indexes, String dataverseName, String datasetName,
            Dataset ds, List<JobSpecification> jobsToExecute, ARecordType aRecordType, ARecordType metaRecordType,
            AqlMetadataProvider metadataProvider, ARecordType enforcedType) throws AlgebricksException {
        for (int j = 0; j < indexes.size(); j++) {
            if (!ExternalIndexingOperations.isFileIndex(indexes.get(j))) {
                CompiledIndexCompactStatement cics = new CompiledIndexCompactStatement(dataverseName, datasetName,
                        indexes.get(j).getIndexName(), indexes.get(j).getKeyFieldNames(),
                        indexes.get(j).getKeyFieldTypes(), indexes.get(j).isEnforcingKeyFileds(),
                        indexes.get(j).getGramLength(), indexes.get(j).getIndexType());
                List<Integer> keySourceIndicators = null;
                if (ds.hasMetaPart()) {
                    keySourceIndicators = indexes.get(j).getKeyFieldSourceIndicators();
                }
                jobsToExecute.add(IndexOperations.buildSecondaryIndexCompactJobSpec(cics, aRecordType, metaRecordType,
                        keySourceIndicators, enforcedType, metadataProvider));
            }

        }
        jobsToExecute.add(ExternalIndexingOperations.compactFilesIndexJobSpec(ds, metadataProvider));
    }

    protected JobSpecification handleQuery(AqlMetadataProvider metadataProvider, Query query,
            IHyracksClientConnection hcc, IHyracksDataset hdc, ResultDelivery resultDelivery, Stats stats)
            throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockManager.INSTANCE.queryBegin(activeDefaultDataverse, query.getDataverses(), query.getDatasets());
        JobSpecification compiled;
        try {
            compiled = rewriteCompileQuery(metadataProvider, query, null);

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            if (query.isExplain()) {
                sessionConfig.out().flush();
                return compiled;
            } else if (sessionConfig.isExecuteQuery() && compiled != null) {
                handleQueryResult(metadataProvider, hcc, hdc, compiled, resultDelivery, stats);
            }
        } catch (Exception e) {
            LOGGER.log(Level.INFO, e.getMessage(), e);
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.queryEnd(query.getDataverses(), query.getDatasets());
            // release external datasets' locks acquired during compilation of the query
            ExternalDatasetsRegistry.INSTANCE.releaseAcquiredLocks(metadataProvider);
        }
        return compiled;
    }

    private void handleQueryResult(AqlMetadataProvider metadataProvider, IHyracksClientConnection hcc,
            IHyracksDataset hdc, JobSpecification compiled, ResultDelivery resultDelivery, Stats stats)
            throws Exception {
        if (GlobalConfig.ASTERIX_LOGGER.isLoggable(Level.FINE)) {
            GlobalConfig.ASTERIX_LOGGER.fine(compiled.toJSON().toString(1));
        }
        JobId jobId = JobUtils.runJob(hcc, compiled, false);

        JSONObject response = new JSONObject();
        switch (resultDelivery) {
            case ASYNC:
                JSONArray handle = new JSONArray();
                handle.put(jobId.getId());
                handle.put(metadataProvider.getResultSetId().getId());
                response.put("handle", handle);
                sessionConfig.out().print(response);
                sessionConfig.out().flush();
                hcc.waitForCompletion(jobId);
                break;
            case SYNC:
                hcc.waitForCompletion(jobId);
                ResultReader resultReader = new ResultReader(hdc);
                resultReader.open(jobId, metadataProvider.getResultSetId());
                ResultUtil.displayResults(resultReader, sessionConfig, stats, metadataProvider.findOutputRecordType());
                break;
            case ASYNC_DEFERRED:
                handle = new JSONArray();
                handle.put(jobId.getId());
                handle.put(metadataProvider.getResultSetId().getId());
                response.put("handle", handle);
                hcc.waitForCompletion(jobId);
                sessionConfig.out().print(response);
                sessionConfig.out().flush();
                break;
            default:
                break;

        }
    }

    protected void handleCreateNodeGroupStatement(AqlMetadataProvider metadataProvider, Statement stmt)
            throws Exception {

        NodegroupDecl stmtCreateNodegroup = (NodegroupDecl) stmt;
        String ngName = stmtCreateNodegroup.getNodegroupName().getValue();

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockManager.INSTANCE.acquireNodeGroupWriteLock(ngName);

        try {
            NodeGroup ng = MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, ngName);
            if (ng != null) {
                if (!stmtCreateNodegroup.getIfNotExists()) {
                    throw new AlgebricksException("A nodegroup with this name " + ngName + " already exists.");
                }
            } else {
                List<Identifier> ncIdentifiers = stmtCreateNodegroup.getNodeControllerNames();
                List<String> ncNames = new ArrayList<>(ncIdentifiers.size());
                for (Identifier id : ncIdentifiers) {
                    ncNames.add(id.getValue());
                }
                MetadataManager.INSTANCE.addNodegroup(mdTxnCtx, new NodeGroup(ngName, ncNames));
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.releaseNodeGroupWriteLock(ngName);
        }
    }

    protected void handleExternalDatasetRefreshStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        RefreshExternalDatasetStatement stmtRefresh = (RefreshExternalDatasetStatement) stmt;
        String dataverseName = getActiveDataverse(stmtRefresh.getDataverseName());
        String datasetName = stmtRefresh.getDatasetName().getValue();
        ExternalDatasetTransactionState transactionState = ExternalDatasetTransactionState.COMMIT;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        MetadataLockManager.INSTANCE.refreshDatasetBegin(dataverseName, dataverseName + "." + datasetName);
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        JobSpecification spec = null;
        Dataset ds = null;
        List<ExternalFile> metadataFiles = null;
        List<ExternalFile> deletedFiles = null;
        List<ExternalFile> addedFiles = null;
        List<ExternalFile> appendedFiles = null;
        List<Index> indexes = null;
        Dataset transactionDataset = null;
        boolean lockAquired = false;
        boolean success = false;
        try {
            ds = MetadataManager.INSTANCE.getDataset(metadataProvider.getMetadataTxnContext(), dataverseName,
                    datasetName);

            // Dataset exists ?
            if (ds == null) {
                throw new AlgebricksException(
                        "There is no dataset with this name " + datasetName + " in dataverse " + dataverseName);
            }
            // Dataset external ?
            if (ds.getDatasetType() != DatasetType.EXTERNAL) {
                throw new AlgebricksException(
                        "dataset " + datasetName + " in dataverse " + dataverseName + " is not an external dataset");
            }
            // Dataset has indexes ?
            indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName, datasetName);
            if (indexes.isEmpty()) {
                throw new AlgebricksException("External dataset " + datasetName + " in dataverse " + dataverseName
                        + " doesn't have any index");
            }

            // Record transaction time
            Date txnTime = new Date();

            // refresh lock here
            ExternalDatasetsRegistry.INSTANCE.refreshBegin(ds);
            lockAquired = true;

            // Get internal files
            metadataFiles = MetadataManager.INSTANCE.getDatasetExternalFiles(mdTxnCtx, ds);
            deletedFiles = new ArrayList<>();
            addedFiles = new ArrayList<>();
            appendedFiles = new ArrayList<>();

            // Compute delta
            // Now we compare snapshot with external file system
            if (ExternalIndexingOperations.isDatasetUptodate(ds, metadataFiles, addedFiles, deletedFiles,
                    appendedFiles)) {
                ((ExternalDatasetDetails) ds.getDatasetDetails()).setRefreshTimestamp(txnTime);
                MetadataManager.INSTANCE.updateDataset(mdTxnCtx, ds);
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                // latch will be released in the finally clause
                return;
            }

            // At this point, we know data has changed in the external file system, record
            // transaction in metadata and start
            transactionDataset = ExternalIndexingOperations.createTransactionDataset(ds);
            /*
             * Remove old dataset record and replace it with a new one
             */
            MetadataManager.INSTANCE.updateDataset(mdTxnCtx, transactionDataset);

            // Add delta files to the metadata
            for (ExternalFile file : addedFiles) {
                MetadataManager.INSTANCE.addExternalFile(mdTxnCtx, file);
            }
            for (ExternalFile file : appendedFiles) {
                MetadataManager.INSTANCE.addExternalFile(mdTxnCtx, file);
            }
            for (ExternalFile file : deletedFiles) {
                MetadataManager.INSTANCE.addExternalFile(mdTxnCtx, file);
            }

            // Create the files index update job
            spec = ExternalIndexingOperations.buildFilesIndexUpdateOp(ds, metadataFiles, deletedFiles, addedFiles,
                    appendedFiles, metadataProvider);

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            transactionState = ExternalDatasetTransactionState.BEGIN;

            // run the files update job
            JobUtils.runJob(hcc, spec, true);

            for (Index index : indexes) {
                if (!ExternalIndexingOperations.isFileIndex(index)) {
                    spec = ExternalIndexingOperations.buildIndexUpdateOp(ds, index, metadataFiles, deletedFiles,
                            addedFiles, appendedFiles, metadataProvider);
                    // run the files update job
                    JobUtils.runJob(hcc, spec, true);
                }
            }

            // all index updates has completed successfully, record transaction state
            spec = ExternalIndexingOperations.buildCommitJob(ds, indexes, metadataProvider);

            // Aquire write latch again -> start a transaction and record the decision to commit
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            metadataProvider.setMetadataTxnContext(mdTxnCtx);
            bActiveTxn = true;
            ((ExternalDatasetDetails) transactionDataset.getDatasetDetails())
                    .setState(ExternalDatasetTransactionState.READY_TO_COMMIT);
            ((ExternalDatasetDetails) transactionDataset.getDatasetDetails()).setRefreshTimestamp(txnTime);
            MetadataManager.INSTANCE.updateDataset(mdTxnCtx, transactionDataset);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            transactionState = ExternalDatasetTransactionState.READY_TO_COMMIT;
            // We don't release the latch since this job is expected to be quick
            JobUtils.runJob(hcc, spec, true);
            // Start a new metadata transaction to record the final state of the transaction
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            metadataProvider.setMetadataTxnContext(mdTxnCtx);
            bActiveTxn = true;

            for (ExternalFile file : metadataFiles) {
                if (file.getPendingOp() == ExternalFilePendingOp.PENDING_DROP_OP) {
                    MetadataManager.INSTANCE.dropExternalFile(mdTxnCtx, file);
                } else if (file.getPendingOp() == ExternalFilePendingOp.PENDING_NO_OP) {
                    Iterator<ExternalFile> iterator = appendedFiles.iterator();
                    while (iterator.hasNext()) {
                        ExternalFile appendedFile = iterator.next();
                        if (file.getFileName().equals(appendedFile.getFileName())) {
                            // delete existing file
                            MetadataManager.INSTANCE.dropExternalFile(mdTxnCtx, file);
                            // delete existing appended file
                            MetadataManager.INSTANCE.dropExternalFile(mdTxnCtx, appendedFile);
                            // add the original file with appended information
                            appendedFile.setFileNumber(file.getFileNumber());
                            appendedFile.setPendingOp(ExternalFilePendingOp.PENDING_NO_OP);
                            MetadataManager.INSTANCE.addExternalFile(mdTxnCtx, appendedFile);
                            iterator.remove();
                        }
                    }
                }
            }

            // remove the deleted files delta
            for (ExternalFile file : deletedFiles) {
                MetadataManager.INSTANCE.dropExternalFile(mdTxnCtx, file);
            }

            // insert new files
            for (ExternalFile file : addedFiles) {
                MetadataManager.INSTANCE.dropExternalFile(mdTxnCtx, file);
                file.setPendingOp(ExternalFilePendingOp.PENDING_NO_OP);
                MetadataManager.INSTANCE.addExternalFile(mdTxnCtx, file);
            }

            // mark the transaction as complete
            ((ExternalDatasetDetails) transactionDataset.getDatasetDetails())
                    .setState(ExternalDatasetTransactionState.COMMIT);
            MetadataManager.INSTANCE.updateDataset(mdTxnCtx, transactionDataset);

            // commit metadata transaction
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            success = true;
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            if (transactionState == ExternalDatasetTransactionState.READY_TO_COMMIT) {
                throw new IllegalStateException("System is inconsistent state: commit of (" + dataverseName + "."
                        + datasetName + ") refresh couldn't carry out the commit phase", e);
            }
            if (transactionState == ExternalDatasetTransactionState.COMMIT) {
                // Nothing to do , everything should be clean
                throw e;
            }
            if (transactionState == ExternalDatasetTransactionState.BEGIN) {
                // transaction failed, need to do the following
                // clean NCs removing transaction components
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                spec = ExternalIndexingOperations.buildAbortOp(ds, indexes, metadataProvider);
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                bActiveTxn = false;
                try {
                    JobUtils.runJob(hcc, spec, true);
                } catch (Exception e2) {
                    // This should never happen -- fix throw illegal
                    e.addSuppressed(e2);
                    throw new IllegalStateException("System is in inconsistent state. Failed to abort refresh", e);
                }
                // remove the delta of files
                // return the state of the dataset to committed
                try {
                    mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                    for (ExternalFile file : deletedFiles) {
                        MetadataManager.INSTANCE.dropExternalFile(mdTxnCtx, file);
                    }
                    for (ExternalFile file : addedFiles) {
                        MetadataManager.INSTANCE.dropExternalFile(mdTxnCtx, file);
                    }
                    for (ExternalFile file : appendedFiles) {
                        MetadataManager.INSTANCE.dropExternalFile(mdTxnCtx, file);
                    }
                    MetadataManager.INSTANCE.updateDataset(mdTxnCtx, ds);
                    // commit metadata transaction
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    abort(e, e2, mdTxnCtx);
                    e.addSuppressed(e2);
                    throw new IllegalStateException("System is in inconsistent state. Failed to drop delta files", e);
                }
            }
        } finally {
            if (lockAquired) {
                ExternalDatasetsRegistry.INSTANCE.refreshEnd(ds, success);
            }
            MetadataLockManager.INSTANCE.refreshDatasetEnd(dataverseName, dataverseName + "." + datasetName);
        }
    }

    protected void handleRunStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws AsterixException, Exception {
        RunStatement runStmt = (RunStatement) stmt;
        switch (runStmt.getSystem()) {
            case "pregel":
            case "pregelix":
                handlePregelixStatement(metadataProvider, runStmt, hcc);
                break;
            default:
                throw new AlgebricksException(
                        "The system \"" + runStmt.getSystem() + "\" specified in your run statement is not supported.");
        }

    }

    protected void handlePregelixStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        RunStatement pregelixStmt = (RunStatement) stmt;
        boolean bActiveTxn = true;
        String dataverseNameFrom = getActiveDataverse(pregelixStmt.getDataverseNameFrom());
        String dataverseNameTo = getActiveDataverse(pregelixStmt.getDataverseNameTo());
        String datasetNameFrom = pregelixStmt.getDatasetNameFrom().getValue();
        String datasetNameTo = pregelixStmt.getDatasetNameTo().getValue();

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        List<String> readDataverses = new ArrayList<>();
        readDataverses.add(dataverseNameFrom);
        List<String> readDatasets = new ArrayList<>();
        readDatasets.add(datasetNameFrom);
        MetadataLockManager.INSTANCE.insertDeleteUpsertBegin(dataverseNameTo, datasetNameTo, readDataverses,
                readDatasets);
        try {
            prepareRunExternalRuntime(metadataProvider, hcc, pregelixStmt, dataverseNameFrom, dataverseNameTo,
                    datasetNameFrom, datasetNameTo, mdTxnCtx);

            String pregelixHomeKey = "PREGELIX_HOME";
            // Finds PREGELIX_HOME in system environment variables.
            String pregelixHome = System.getenv(pregelixHomeKey);
            // Finds PREGELIX_HOME in Java properties.
            if (pregelixHome == null) {
                pregelixHome = System.getProperty(pregelixHomeKey);
            }
            // Finds PREGELIX_HOME in AsterixDB configuration.
            if (pregelixHome == null) {
                // Since there is a default value for PREGELIX_HOME in AsterixCompilerProperties,
                // pregelixHome can never be null.
                pregelixHome = AsterixAppContextInfo.INSTANCE.getCompilerProperties().getPregelixHome();
            }

            // Constructs the pregelix command line.
            List<String> cmd = constructPregelixCommand(pregelixStmt, dataverseNameFrom, datasetNameFrom,
                    dataverseNameTo, datasetNameTo);
            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.directory(new File(pregelixHome));
            pb.redirectErrorStream(true);

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            // Executes the Pregelix command.
            int resultState = executeExternalShellProgram(pb);
            // Checks the return state of the external Pregelix command.
            if (resultState != 0) {
                throw new AlgebricksException(
                        "Something went wrong executing your Pregelix Job. Perhaps the Pregelix cluster "
                                + "needs to be restarted. "
                                + "Check the following things: Are the datatypes of Asterix and Pregelix matching? "
                                + "Is the server configuration correct (node names, buffer sizes, framesize)? "
                                + "Check the logfiles for more details.");
            }
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.insertDeleteUpsertEnd(dataverseNameTo, datasetNameTo, readDataverses,
                    readDatasets);
        }
    }

    // Prepares to run a program on external runtime.
    protected void prepareRunExternalRuntime(AqlMetadataProvider metadataProvider, IHyracksClientConnection hcc,
            RunStatement pregelixStmt, String dataverseNameFrom, String dataverseNameTo, String datasetNameFrom,
            String datasetNameTo, MetadataTransactionContext mdTxnCtx) throws Exception {
        // Validates the source/sink dataverses and datasets.
        Dataset fromDataset = metadataProvider.findDataset(dataverseNameFrom, datasetNameFrom);
        if (fromDataset == null) {
            throw new AsterixException("The source dataset " + datasetNameFrom + " in dataverse " + dataverseNameFrom
                    + " could not be found for the Run command");
        }
        Dataset toDataset = metadataProvider.findDataset(dataverseNameTo, datasetNameTo);
        if (toDataset == null) {
            throw new AsterixException("The sink dataset " + datasetNameTo + " in dataverse " + dataverseNameTo
                    + " could not be found for the Run command");
        }

        try {
            // Find the primary index of the sink dataset.
            Index toIndex = null;
            List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseNameTo,
                    pregelixStmt.getDatasetNameTo().getValue());
            for (Index index : indexes) {
                if (index.isPrimaryIndex()) {
                    toIndex = index;
                    break;
                }
            }
            if (toIndex == null) {
                throw new AlgebricksException("Tried to access non-existing dataset: " + datasetNameTo);
            }
            // Cleans up the sink dataset -- Drop and then Create.
            DropDatasetStatement dropStmt = new DropDatasetStatement(new Identifier(dataverseNameTo),
                    pregelixStmt.getDatasetNameTo(), true);
            this.handleDatasetDropStatement(metadataProvider, dropStmt, hcc);
            IDatasetDetailsDecl idd = new InternalDetailsDecl(toIndex.getKeyFieldNames(),
                    toIndex.getKeyFieldSourceIndicators(), false, null, toDataset.getDatasetDetails().isTemp());
            DatasetDecl createToDataset = new DatasetDecl(new Identifier(dataverseNameTo),
                    pregelixStmt.getDatasetNameTo(), new Identifier(toDataset.getItemTypeDataverseName()),
                    new Identifier(toDataset.getItemTypeName()),
                    new Identifier(toDataset.getMetaItemTypeDataverseName()),
                    new Identifier(toDataset.getMetaItemTypeName()), new Identifier(toDataset.getNodeGroupName()),
                    toDataset.getCompactionPolicy(), toDataset.getCompactionPolicyProperties(), toDataset.getHints(),
                    toDataset.getDatasetType(), idd, false);
            this.handleCreateDatasetStatement(metadataProvider, createToDataset, hcc);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, e.getMessage(), e);
            throw new AlgebricksException("Error cleaning the result dataset. This should not happen.");
        }

        // Flushes source dataset.
        FlushDatasetUtils.flushDataset(hcc, metadataProvider, mdTxnCtx, dataverseNameFrom, datasetNameFrom,
                datasetNameFrom);
    }

    // Executes external shell commands.
    protected int executeExternalShellProgram(ProcessBuilder pb)
            throws IOException, AlgebricksException, InterruptedException {
        Process process = pb.start();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = in.readLine()) != null) {
                LOGGER.info(line);
                if (line.contains("Exception") || line.contains("Error")) {
                    LOGGER.severe(line);
                    if (line.contains("Connection refused")) {
                        throw new AlgebricksException(
                                "The connection to your Pregelix cluster was refused. Is it running? "
                                        + "Is the port in the query correct?");
                    }
                    if (line.contains("Could not find or load main class")) {
                        throw new AlgebricksException("The main class of your Pregelix query was not found. "
                                + "Is the path to your .jar file correct?");
                    }
                    if (line.contains("ClassNotFoundException")) {
                        throw new AlgebricksException("The vertex class of your Pregelix query was not found. "
                                + "Does it exist? Is the spelling correct?");
                    }
                }
            }
            process.waitFor();
        }
        // Gets the exit value of the program.
        return process.exitValue();
    }

    // Constructs a Pregelix command line.
    protected List<String> constructPregelixCommand(RunStatement pregelixStmt, String fromDataverseName,
            String fromDatasetName, String toDataverseName, String toDatasetName) {
        // Constructs AsterixDB parameters, e.g., URL, source dataset and sink dataset.
        AsterixExternalProperties externalProperties = AsterixAppContextInfo.INSTANCE.getExternalProperties();
        String clientIP = ClusterProperties.INSTANCE.getCluster().getMasterNode().getClientIp();
        StringBuilder asterixdbParameterBuilder = new StringBuilder();
        asterixdbParameterBuilder.append(
                "pregelix.asterixdb.url=" + "http://" + clientIP + ":" + externalProperties.getAPIServerPort() + ",");
        asterixdbParameterBuilder.append("pregelix.asterixdb.source=true,");
        asterixdbParameterBuilder.append("pregelix.asterixdb.sink=true,");
        asterixdbParameterBuilder.append("pregelix.asterixdb.input.dataverse=" + fromDataverseName + ",");
        asterixdbParameterBuilder.append("pregelix.asterixdb.input.dataset=" + fromDatasetName + ",");
        asterixdbParameterBuilder.append("pregelix.asterixdb.output.dataverse=" + toDataverseName + ",");
        asterixdbParameterBuilder.append("pregelix.asterixdb.output.dataset=" + toDatasetName + ",");
        asterixdbParameterBuilder.append("pregelix.asterixdb.output.cleanup=false,");

        // construct command
        List<String> cmds = new ArrayList<>();
        cmds.add("bin/pregelix");
        cmds.add(pregelixStmt.getParameters().get(0)); // jar
        cmds.add(pregelixStmt.getParameters().get(1)); // class

        String customizedPregelixProperty = "-cust-prop";
        String inputConverterClassKey = "pregelix.asterixdb.input.converterclass";
        String inputConverterClassValue = "=org.apache.pregelix.example.converter.VLongIdInputVertexConverter,";
        String outputConverterClassKey = "pregelix.asterixdb.output.converterclass";
        String outputConverterClassValue = "=org.apache.pregelix.example.converter.VLongIdOutputVertexConverter,";
        boolean custPropAdded = false;
        boolean meetCustProp = false;
        // User parameters.
        for (String s : pregelixStmt.getParameters().get(2).split(" ")) {
            if (meetCustProp) {
                if (!s.contains(inputConverterClassKey)) {
                    asterixdbParameterBuilder.append(inputConverterClassKey + inputConverterClassValue);
                }
                if (!s.contains(outputConverterClassKey)) {
                    asterixdbParameterBuilder.append(outputConverterClassKey + outputConverterClassValue);
                }
                cmds.add(asterixdbParameterBuilder.toString() + s);
                meetCustProp = false;
                custPropAdded = true;
                continue;
            }
            cmds.add(s);
            if (s.equals(customizedPregelixProperty)) {
                meetCustProp = true;
            }
        }

        if (!custPropAdded) {
            cmds.add(customizedPregelixProperty);
            // Appends default converter classes to asterixdbParameterBuilder.
            asterixdbParameterBuilder.append(inputConverterClassKey + inputConverterClassValue);
            asterixdbParameterBuilder.append(outputConverterClassKey + outputConverterClassValue);
            // Remove the last comma.
            asterixdbParameterBuilder.delete(asterixdbParameterBuilder.length() - 1,
                    asterixdbParameterBuilder.length());
            cmds.add(asterixdbParameterBuilder.toString());
        }
        return cmds;
    }

    @Override
    public String getActiveDataverseName(String dataverse) {
        return (dataverse != null) ? dataverse : activeDefaultDataverse.getDataverseName();
    }

    protected String getActiveDataverse(Identifier dataverse) {
        return getActiveDataverseName(dataverse != null ? dataverse.getValue() : null);
    }

    /**
     * Abort the ongoing metadata transaction logging the error cause
     *
     * @param rootE
     * @param parentE
     * @param mdTxnCtx
     */
    public static void abort(Exception rootE, Exception parentE, MetadataTransactionContext mdTxnCtx) {
        try {
            if (IS_DEBUG_MODE) {
                LOGGER.log(Level.SEVERE, rootE.getMessage(), rootE);
            }
            if (mdTxnCtx != null) {
                MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            }
        } catch (Exception e2) {
            parentE.addSuppressed(e2);
            throw new IllegalStateException(rootE);
        }
    }

    protected void rewriteStatement(Statement stmt) throws AsterixException {
        IStatementRewriter rewriter = rewriterFactory.createStatementRewriter();
        rewriter.rewrite(stmt);
    }

    /*
     * Merges typed index fields with specified recordType, allowing indexed fields to be optional.
     * I.e. the type { "personId":int32, "name": string, "address" : { "street": string } } with typed indexes
     * on age:int32, address.state:string will be merged into type { "personId":int32, "name": string,
     * "age": int32? "address" : { "street": string, "state": string? } } Used by open indexes to enforce
     * the type of an indexed record
     */
    private static ARecordType createEnforcedType(ARecordType initialType, List<Index> indexes)
            throws AlgebricksException {
        ARecordType enforcedType = initialType;
        for (Index index : indexes) {
            if (!index.isSecondaryIndex() || !index.isEnforcingKeyFileds()) {
                continue;
            }
            if (index.hasMetaFields()) {
                throw new AlgebricksException("Indexing an open field is only supported on the record part");
            }
            for (int i = 0; i < index.getKeyFieldNames().size(); i++) {
                Deque<Pair<ARecordType, String>> nestedTypeStack = new ArrayDeque<>();
                List<String> splits = index.getKeyFieldNames().get(i);
                ARecordType nestedFieldType = enforcedType;
                boolean openRecords = false;
                String bridgeName = nestedFieldType.getTypeName();
                int j;
                // Build the stack for the enforced type
                for (j = 1; j < splits.size(); j++) {
                    nestedTypeStack.push(new Pair<ARecordType, String>(nestedFieldType, splits.get(j - 1)));
                    bridgeName = nestedFieldType.getTypeName();
                    nestedFieldType = (ARecordType) enforcedType.getSubFieldType(splits.subList(0, j));
                    if (nestedFieldType == null) {
                        openRecords = true;
                        break;
                    }
                }
                if (openRecords) {
                    // create the smallest record
                    enforcedType = new ARecordType(splits.get(splits.size() - 2),
                            new String[] { splits.get(splits.size() - 1) },
                            new IAType[] { AUnionType.createUnknownableType(index.getKeyFieldTypes().get(i)) }, true);
                    // create the open part of the nested field
                    for (int k = splits.size() - 3; k > (j - 2); k--) {
                        enforcedType = new ARecordType(splits.get(k), new String[] { splits.get(k + 1) },
                                new IAType[] { AUnionType.createUnknownableType(enforcedType) }, true);
                    }
                    // Bridge the gap
                    Pair<ARecordType, String> gapPair = nestedTypeStack.pop();
                    ARecordType parent = gapPair.first;

                    IAType[] parentFieldTypes = ArrayUtils.addAll(parent.getFieldTypes().clone(),
                            new IAType[] { AUnionType.createUnknownableType(enforcedType) });
                    enforcedType = new ARecordType(bridgeName,
                            ArrayUtils.addAll(parent.getFieldNames(), enforcedType.getTypeName()), parentFieldTypes,
                            true);
                } else {
                    //Schema is closed all the way to the field
                    //enforced fields are either null or strongly typed
                    LinkedHashMap<String, IAType> recordNameTypesMap = createRecordNameTypeMap(nestedFieldType);
                    // if a an enforced field already exists and the type is correct
                    IAType enforcedFieldType = recordNameTypesMap.get(splits.get(splits.size() - 1));
                    if (enforcedFieldType != null && enforcedFieldType.getTypeTag() == ATypeTag.UNION
                            && ((AUnionType) enforcedFieldType).isUnknownableType()) {
                        enforcedFieldType = ((AUnionType) enforcedFieldType).getActualType();
                    }
                    if (enforcedFieldType != null && !ATypeHierarchy.canPromote(enforcedFieldType.getTypeTag(),
                            index.getKeyFieldTypes().get(i).getTypeTag())) {
                        throw new AlgebricksException("Cannot enforce field " + index.getKeyFieldNames().get(i)
                                + " to have type " + index.getKeyFieldTypes().get(i));
                    }
                    if (enforcedFieldType == null) {
                        recordNameTypesMap.put(splits.get(splits.size() - 1),
                                AUnionType.createUnknownableType(index.getKeyFieldTypes().get(i)));
                    }
                    enforcedType = new ARecordType(nestedFieldType.getTypeName(),
                            recordNameTypesMap.keySet().toArray(new String[recordNameTypesMap.size()]),
                            recordNameTypesMap.values().toArray(new IAType[recordNameTypesMap.size()]),
                            nestedFieldType.isOpen());
                }

                // Create the enforced type for the nested fields in the schema, from the ground up
                if (!nestedTypeStack.isEmpty()) {
                    while (!nestedTypeStack.isEmpty()) {
                        Pair<ARecordType, String> nestedTypePair = nestedTypeStack.pop();
                        ARecordType nestedRecType = nestedTypePair.first;
                        IAType[] nestedRecTypeFieldTypes = nestedRecType.getFieldTypes().clone();
                        nestedRecTypeFieldTypes[nestedRecType.getFieldIndex(nestedTypePair.second)] = enforcedType;
                        enforcedType = new ARecordType(nestedRecType.getTypeName() + "_enforced",
                                nestedRecType.getFieldNames(), nestedRecTypeFieldTypes, nestedRecType.isOpen());
                    }
                }
            }
        }
        return enforcedType;
    }

    private static LinkedHashMap<String, IAType> createRecordNameTypeMap(ARecordType nestedFieldType) {
        LinkedHashMap<String, IAType> recordNameTypesMap = new LinkedHashMap<>();
        for (int j = 0; j < nestedFieldType.getFieldNames().length; j++) {
            recordNameTypesMap.put(nestedFieldType.getFieldNames()[j], nestedFieldType.getFieldTypes()[j]);
        }
        return recordNameTypesMap;
    }
}