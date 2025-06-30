/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.timeseries;

import static java.util.Collections.unmodifiableList;
import static org.opensearch.ad.constant.ADCommonName.ANOMALY_RESULT_INDEX_ALIAS;
import static org.opensearch.ad.constant.ADCommonName.CHECKPOINT_INDEX_NAME;
import static org.opensearch.ad.constant.ADCommonName.DETECTION_STATE_INDEX;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_COOLDOWN_MINUTES;
import static org.opensearch.forecast.constant.ForecastCommonName.FORECAST_CHECKPOINT_INDEX_NAME;
import static org.opensearch.forecast.constant.ForecastCommonName.FORECAST_STATE_INDEX;
import static org.opensearch.timeseries.constant.CommonName.JOB_INDEX;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.ServiceLoader;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.SpecialPermission;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.ADJobProcessor;
import org.opensearch.ad.ADTaskProfileRunner;
import org.opensearch.ad.AnomalyDetectorRunner;
import org.opensearch.ad.ExecuteADResultResponseRecorder;
import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.client.ADHttpNodeCommunicator;
import org.opensearch.ad.client.ADNodeCommunicator;
import org.opensearch.ad.client.ADTransportNodeCommunicator;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.executor.ADCoordinatorContributor;
import org.opensearch.ad.executor.ADModelContributor;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.ml.ADCheckpointDao;
import org.opensearch.ad.ml.ADCheckpointStore;
import org.opensearch.ad.ml.ADColdStart;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.ad.ml.ADRealTimeInferencer;
import org.opensearch.ad.ml.ADS3CheckpointDao;
import org.opensearch.ad.ml.DelegatingADCheckpointStore;
import org.opensearch.ad.ml.HybridThresholdingModel;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.DetectorInternalState;
import org.opensearch.ad.ratelimit.ADCheckpointMaintainWorker;
import org.opensearch.ad.ratelimit.ADCheckpointReadWorker;
import org.opensearch.ad.ratelimit.ADCheckpointWriteWorker;
import org.opensearch.ad.ratelimit.ADColdEntityWorker;
import org.opensearch.ad.ratelimit.ADColdStartWorker;
import org.opensearch.ad.ratelimit.ADResultWriteWorker;
import org.opensearch.ad.ratelimit.ADSaveResultStrategy;
import org.opensearch.ad.rest.RestADSingleStreamResultAction;
import org.opensearch.ad.rest.RestADStatsNodesAction;
import org.opensearch.ad.rest.RestAnomalyDetectorEntityProfileAction;
import org.opensearch.ad.rest.RestAnomalyDetectorJobAction;
import org.opensearch.ad.rest.RestAnomalyDetectorNodeProfileAction;
import org.opensearch.ad.rest.RestAnomalyDetectorSuggestAction;
import org.opensearch.ad.rest.RestDeleteADModelAction;
import org.opensearch.ad.rest.RestDeleteAnomalyDetectorAction;
import org.opensearch.ad.rest.RestDeleteAnomalyResultsAction;
import org.opensearch.ad.rest.RestEntityADResultAction;
import org.opensearch.ad.rest.RestExecuteAnomalyDetectorAction;
import org.opensearch.ad.rest.RestGetAnomalyDetectorAction;
import org.opensearch.ad.rest.RestIndexAnomalyDetectorAction;
import org.opensearch.ad.rest.RestPreviewAnomalyDetectorAction;
import org.opensearch.ad.rest.RestSearchADTasksAction;
import org.opensearch.ad.rest.RestSearchAnomalyDetectorAction;
import org.opensearch.ad.rest.RestSearchAnomalyDetectorInfoAction;
import org.opensearch.ad.rest.RestSearchAnomalyResultAction;
import org.opensearch.ad.rest.RestSearchTopAnomalyResultAction;
import org.opensearch.ad.rest.RestStatsAnomalyDetectorAction;
import org.opensearch.ad.rest.RestValidateAnomalyDetectorAction;
import org.opensearch.ad.rest.handler.ADEventBridgeHandler;
import org.opensearch.ad.rest.handler.ADIndexJobActionHandler;
import org.opensearch.ad.rest.handler.store.ADDelegatingDataManagement;
import org.opensearch.ad.rest.handler.store.ADSdkDataManagement;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.ad.settings.ADNumericSetting;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.settings.LegacyOpenDistroAnomalyDetectorSettings;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.stats.suppliers.ADModelsOnNodeCountSupplier;
import org.opensearch.ad.stats.suppliers.ADModelsOnNodeSupplier;
import org.opensearch.ad.task.ADBatchTaskRunner;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.ADBatchAnomalyResultAction;
import org.opensearch.ad.transport.ADBatchAnomalyResultTransportAction;
import org.opensearch.ad.transport.ADBatchTaskRemoteExecutionAction;
import org.opensearch.ad.transport.ADBatchTaskRemoteExecutionTransportAction;
import org.opensearch.ad.transport.ADCancelTaskAction;
import org.opensearch.ad.transport.ADCancelTaskTransportAction;
import org.opensearch.ad.transport.ADEntityProfileAction;
import org.opensearch.ad.transport.ADEntityProfileTransportAction;
import org.opensearch.ad.transport.ADHCImputeAction;
import org.opensearch.ad.transport.ADHCImputeTransportAction;
import org.opensearch.ad.transport.ADProfileAction;
import org.opensearch.ad.transport.ADProfileTransportAction;
import org.opensearch.ad.transport.ADResultBulkAction;
import org.opensearch.ad.transport.ADResultBulkTransportAction;
import org.opensearch.ad.transport.ADSingleStreamResultAction;
import org.opensearch.ad.transport.ADSingleStreamResultTransportAction;
import org.opensearch.ad.transport.ADStatsNodesAction;
import org.opensearch.ad.transport.ADStatsNodesTransportAction;
import org.opensearch.ad.transport.ADTaskProfileAction;
import org.opensearch.ad.transport.ADTaskProfileTransportAction;
import org.opensearch.ad.transport.AnomalyDetectorJobAction;
import org.opensearch.ad.transport.AnomalyDetectorJobTransportAction;
import org.opensearch.ad.transport.AnomalyResultAction;
import org.opensearch.ad.transport.AnomalyResultTransportAction;
import org.opensearch.ad.transport.DeleteADModelAction;
import org.opensearch.ad.transport.DeleteADModelTransportAction;
import org.opensearch.ad.transport.DeleteAnomalyDetectorAction;
import org.opensearch.ad.transport.DeleteAnomalyDetectorMutliTenantTransportAction;
import org.opensearch.ad.transport.DeleteAnomalyDetectorTransportAction;
import org.opensearch.ad.transport.DeleteAnomalyResultsAction;
import org.opensearch.ad.transport.DeleteAnomalyResultsTransportAction;
import org.opensearch.ad.transport.EntityADResultAction;
import org.opensearch.ad.transport.EntityADResultTransportAction;
import org.opensearch.ad.transport.ForwardADTaskAction;
import org.opensearch.ad.transport.ForwardADTaskTransportAction;
import org.opensearch.ad.transport.GetAnomalyDetectorAction;
import org.opensearch.ad.transport.GetAnomalyDetectorTransportAction;
import org.opensearch.ad.transport.IndexAnomalyDetectorAction;
import org.opensearch.ad.transport.IndexAnomalyDetectorTransportAction;
import org.opensearch.ad.transport.PreviewAnomalyDetectorAction;
import org.opensearch.ad.transport.PreviewAnomalyDetectorTransportAction;
import org.opensearch.ad.transport.RCFPollingAction;
import org.opensearch.ad.transport.RCFPollingTransportAction;
import org.opensearch.ad.transport.RCFResultAction;
import org.opensearch.ad.transport.RCFResultTransportAction;
import org.opensearch.ad.transport.SearchADTasksAction;
import org.opensearch.ad.transport.SearchADTasksTransportAction;
import org.opensearch.ad.transport.SearchAnomalyDetectorAction;
import org.opensearch.ad.transport.SearchAnomalyDetectorInfoAction;
import org.opensearch.ad.transport.SearchAnomalyDetectorInfoTransportAction;
import org.opensearch.ad.transport.SearchAnomalyDetectorTransportAction;
import org.opensearch.ad.transport.SearchAnomalyResultAction;
import org.opensearch.ad.transport.SearchAnomalyResultTransportAction;
import org.opensearch.ad.transport.SearchTopAnomalyResultAction;
import org.opensearch.ad.transport.SearchTopAnomalyResultTransportAction;
import org.opensearch.ad.transport.StatsAnomalyDetectorAction;
import org.opensearch.ad.transport.StatsAnomalyDetectorTransportAction;
import org.opensearch.ad.transport.StopDetectorAction;
import org.opensearch.ad.transport.StopDetectorTransportAction;
import org.opensearch.ad.transport.SuggestAnomalyDetectorParamAction;
import org.opensearch.ad.transport.SuggestAnomalyDetectorParamTransportAction;
import org.opensearch.ad.transport.ThresholdResultAction;
import org.opensearch.ad.transport.ThresholdResultTransportAction;
import org.opensearch.ad.transport.ValidateAnomalyDetectorAction;
import org.opensearch.ad.transport.ValidateAnomalyDetectorTransportAction;
import org.opensearch.ad.transport.handler.ADIndexMemoryPressureAwareResultHandler;
import org.opensearch.ad.transport.handler.ADSearchHandler;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.AbstractModule;
import org.opensearch.common.inject.Module;
import org.opensearch.common.inject.Provider;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.forecast.ExecuteForecastResultResponseRecorder;
import org.opensearch.forecast.ForecastJobProcessor;
import org.opensearch.forecast.ForecastTaskProfileRunner;
import org.opensearch.forecast.caching.ForecastCacheProvider;
import org.opensearch.forecast.caching.ForecastPriorityCache;
import org.opensearch.forecast.client.ForecastNodeCommunicator;
import org.opensearch.forecast.client.ForecastTransportNodeCommunicator;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.executor.ForecastModelContributor;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.ml.ForecastCheckpointDao;
import org.opensearch.forecast.ml.ForecastColdStart;
import org.opensearch.forecast.ml.ForecastModelManager;
import org.opensearch.forecast.ml.ForecastRealTimeInferencer;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.ratelimit.ForecastCheckpointMaintainWorker;
import org.opensearch.forecast.ratelimit.ForecastCheckpointReadWorker;
import org.opensearch.forecast.ratelimit.ForecastCheckpointWriteWorker;
import org.opensearch.forecast.ratelimit.ForecastColdEntityWorker;
import org.opensearch.forecast.ratelimit.ForecastColdStartWorker;
import org.opensearch.forecast.ratelimit.ForecastResultWriteWorker;
import org.opensearch.forecast.ratelimit.ForecastSaveResultStrategy;
import org.opensearch.forecast.rest.RestDeleteForecasterAction;
import org.opensearch.forecast.rest.RestForecasterJobAction;
import org.opensearch.forecast.rest.RestForecasterSuggestAction;
import org.opensearch.forecast.rest.RestGetForecasterAction;
import org.opensearch.forecast.rest.RestIndexForecasterAction;
import org.opensearch.forecast.rest.RestRunOnceForecasterAction;
import org.opensearch.forecast.rest.RestSearchForecastTasksAction;
import org.opensearch.forecast.rest.RestSearchForecasterAction;
import org.opensearch.forecast.rest.RestSearchForecasterInfoAction;
import org.opensearch.forecast.rest.RestSearchTopForecastResultAction;
import org.opensearch.forecast.rest.RestStatsForecasterAction;
import org.opensearch.forecast.rest.RestValidateForecasterAction;
import org.opensearch.forecast.rest.handler.ForecastIndexJobActionHandler;
import org.opensearch.forecast.rest.handler.store.ForecastDelegatingDataManagement;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.forecast.settings.ForecastNumericSetting;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.forecast.stats.ForecastModelsOnNodeSupplier;
import org.opensearch.forecast.stats.ForecastStats;
import org.opensearch.forecast.stats.suppliers.ForecastModelsOnNodeCountSupplier;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.forecast.transport.DeleteForecastModelAction;
import org.opensearch.forecast.transport.DeleteForecastModelTransportAction;
import org.opensearch.forecast.transport.DeleteForecasterAction;
import org.opensearch.forecast.transport.DeleteForecasterTransportAction;
import org.opensearch.forecast.transport.EntityForecastResultAction;
import org.opensearch.forecast.transport.EntityForecastResultTransportAction;
import org.opensearch.forecast.transport.ForecastEntityProfileAction;
import org.opensearch.forecast.transport.ForecastEntityProfileTransportAction;
import org.opensearch.forecast.transport.ForecastProfileAction;
import org.opensearch.forecast.transport.ForecastProfileTransportAction;
import org.opensearch.forecast.transport.ForecastResultAction;
import org.opensearch.forecast.transport.ForecastResultBulkAction;
import org.opensearch.forecast.transport.ForecastResultBulkTransportAction;
import org.opensearch.forecast.transport.ForecastResultTransportAction;
import org.opensearch.forecast.transport.ForecastRunOnceAction;
import org.opensearch.forecast.transport.ForecastRunOnceProfileAction;
import org.opensearch.forecast.transport.ForecastRunOnceProfileTransportAction;
import org.opensearch.forecast.transport.ForecastRunOnceTransportAction;
import org.opensearch.forecast.transport.ForecastSingleStreamResultAction;
import org.opensearch.forecast.transport.ForecastSingleStreamResultTransportAction;
import org.opensearch.forecast.transport.ForecastStatsNodesAction;
import org.opensearch.forecast.transport.ForecastStatsNodesTransportAction;
import org.opensearch.forecast.transport.ForecasterJobAction;
import org.opensearch.forecast.transport.ForecasterJobTransportAction;
import org.opensearch.forecast.transport.GetForecasterAction;
import org.opensearch.forecast.transport.GetForecasterTransportAction;
import org.opensearch.forecast.transport.IndexForecasterAction;
import org.opensearch.forecast.transport.IndexForecasterTransportAction;
import org.opensearch.forecast.transport.SearchForecastTasksAction;
import org.opensearch.forecast.transport.SearchForecastTasksTransportAction;
import org.opensearch.forecast.transport.SearchForecasterAction;
import org.opensearch.forecast.transport.SearchForecasterInfoAction;
import org.opensearch.forecast.transport.SearchForecasterInfoTransportAction;
import org.opensearch.forecast.transport.SearchForecasterTransportAction;
import org.opensearch.forecast.transport.SearchTopForecastResultAction;
import org.opensearch.forecast.transport.SearchTopForecastResultTransportAction;
import org.opensearch.forecast.transport.StatsForecasterAction;
import org.opensearch.forecast.transport.StatsForecasterTransportAction;
import org.opensearch.forecast.transport.StopForecasterAction;
import org.opensearch.forecast.transport.StopForecasterTransportAction;
import org.opensearch.forecast.transport.SuggestForecasterParamAction;
import org.opensearch.forecast.transport.SuggestForecasterParamTransportAction;
import org.opensearch.forecast.transport.ValidateForecasterAction;
import org.opensearch.forecast.transport.ValidateForecasterTransportAction;
import org.opensearch.forecast.transport.handler.ForecastIndexMemoryPressureAwareResultHandler;
import org.opensearch.forecast.transport.handler.ForecastSearchHandler;
import org.opensearch.identity.PluginSubject;
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.jobscheduler.spi.JobSchedulerExtension;
import org.opensearch.jobscheduler.spi.ScheduledJobParser;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.IdentityAwarePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.ScriptPlugin;
import org.opensearch.plugins.SystemIndexPlugin;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.annotation.SuppressForbidden;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.client.ConfigDocumentStore;
import org.opensearch.timeseries.client.ConfigDocumentStoreFactory;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.DefaultDataAccess;
import org.opensearch.timeseries.client.RemoteMetadataConfigDocumentStoreFactory;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.client.SdkDataAccess;
import org.opensearch.timeseries.client.SdkRunContext;
import org.opensearch.timeseries.client.ThreadRunContext;
import org.opensearch.timeseries.cluster.ADDataMigrator;
import org.opensearch.timeseries.cluster.ClusterEventListener;
import org.opensearch.timeseries.cluster.ClusterManagerTaskRegistry;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.dataprocessor.Imputer;
import org.opensearch.timeseries.dataprocessor.LinearUniformImputer;
import org.opensearch.timeseries.executor.CloudMapWatcherContributor;
import org.opensearch.timeseries.executor.ExecutorBuilderContributor;
import org.opensearch.timeseries.executor.SQSConsumerContributor;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.function.ThrowingSupplierWrapper;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.ratelimit.CheckPointMaintainRequestAdapter;
import org.opensearch.timeseries.rest.handler.store.SDKDataManagement;
import org.opensearch.timeseries.rest.handler.store.spi.DefaultTenantEndpointResolver;
import org.opensearch.timeseries.rest.handler.store.spi.TenantEndpointResolver;
import org.opensearch.timeseries.settings.TimeSeriesEnabledSetting;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.stats.TimeSeriesStat;
import org.opensearch.timeseries.stats.suppliers.CounterSupplier;
import org.opensearch.timeseries.stats.suppliers.IndexStatusSupplier;
import org.opensearch.timeseries.stats.suppliers.SettableSupplier;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.transport.CronAction;
import org.opensearch.timeseries.transport.CronTransportAction;
import org.opensearch.timeseries.transport.DeleteConfigRequest;
import org.opensearch.timeseries.transport.handler.ResultBulkIndexingHandler;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;
import org.opensearch.timeseries.util.IndexOperations;
import org.opensearch.timeseries.util.IndexUtils;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.timeseries.util.PluginClient;
import org.opensearch.timeseries.util.SDKIndexOperations;
import org.opensearch.timeseries.util.SDKNodeFilter;
import org.opensearch.timeseries.util.SdkClientProvider;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import com.amazon.randomcutforest.parkservices.RCFCaster;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;
import com.amazon.randomcutforest.parkservices.state.RCFCasterMapper;
import com.amazon.randomcutforest.parkservices.state.RCFCasterState;
import com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestMapper;
import com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestState;
import com.amazon.randomcutforest.serialize.json.v1.V1JsonToV3StateConverter;
import com.amazon.randomcutforest.state.RandomCutForestMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import io.protostuff.LinkedBuffer;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * Entry point of time series analytics plugin.
 */
@SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: Pass parameter for single-tenant-only components.")
public class TimeSeriesAnalyticsPlugin extends Plugin
    implements
        ActionPlugin,
        ScriptPlugin,
        SystemIndexPlugin,
        JobSchedulerExtension,
        IdentityAwarePlugin {

    private static final Logger LOG = LogManager.getLogger(TimeSeriesAnalyticsPlugin.class);

    // AD constants
    public static final String LEGACY_AD_BASE = "/_opendistro/_anomaly_detection";
    public static final String LEGACY_OPENDISTRO_AD_BASE_URI = LEGACY_AD_BASE + "/detectors";
    public static final String AD_BASE_URI = "/_plugins/_anomaly_detection";
    public static final String AD_BASE_DETECTORS_URI = AD_BASE_URI + "/detectors";
    // forecasting constants
    public static final String FORECAST_BASE_URI = "/_plugins/_forecast";
    // common timeseries constants
    public static final String TIMESERIES_BASE_URI = "/_plugins/_timeseries";
    public static final String FORECAST_FORECASTERS_URI = FORECAST_BASE_URI + "/forecasters";
    public static final String TIME_SERIES_JOB_TYPE = "opensearch_time_series_analytics";

    private static Gson gson;
    private ADIndexManagement anomalyDetectionIndices;
    private ADDelegatingDataManagement adDataManagement;
    private ForecastIndexManagement forecastIndices;
    private ForecastDelegatingDataManagement forecastDataManagement;
    private AnomalyDetectorRunner anomalyDetectorRunner;
    private Client client;
    private Settings pluginSettings = Settings.EMPTY;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private ADStats adStats;
    private ForecastStats forecastStats;
    private ClientUtil clientUtil;
    private SecurityClientUtil securityClientUtil;
    private DiscoveryNodeSelector nodeFilter;
    private IndexUtils indexUtils;
    private IndexOperations indexOperations;
    private ADTaskManager adTaskManager;
    private ForecastTaskManager forecastTaskManager;
    private ADBatchTaskRunner adBatchTaskRunner;
    // package private for testing
    GenericObjectPool<LinkedBuffer> serializeRCFBufferPool;
    private StateManager stateManager;
    private DataAccess dataAccess;
    private RunContext runContext;
    private ADNodeCommunicator adNodeCommunicator;
    private ForecastNodeCommunicator forecastNodeCommunicator;
    private ExecuteADResultResponseRecorder adResultResponseRecorder;
    private ExecuteForecastResultResponseRecorder forecastResultResponseRecorder;
    private ADIndexJobActionHandler adIndexJobActionHandler;
    private ForecastIndexJobActionHandler forecastIndexJobActionHandler;

    private PluginClient pluginClient;
    private ADEventBridgeHandler eventBridgeHandler;

    static {
        SpecialPermission.check();
        // gson intialization requires "java.lang.RuntimePermission" "accessDeclaredMembers" to
        // initialize ConstructorConstructor
        AccessController.doPrivileged((PrivilegedAction<Void>) TimeSeriesAnalyticsPlugin::initGson);
    }

    public TimeSeriesAnalyticsPlugin() {}

    @Override
    public Collection<Module> createGuiceModules() {
        // Transport actions get constructed during Guice injector creation (see Node#createInjector),
        // which happens *before* OpenSearch calls plugin#createComponents. Objects returned by
        // createComponents are only auto-bound to themselves, so interfaces such as StateManager
        // remain unbound and injected constructors (e.g. SecurityClientUtil) fail. Providing an
        // explicit module here lets us bind StateManager via a Provider that looks up the instance
        // initialized later in createComponents, satisfying Guice without duplicating construction logic.
        return Collections.singletonList(new AbstractModule() {
            @Override
            protected void configure() {
                final Provider<Settings> settingsProvider = getProvider(Settings.class);
                final Provider<ClusterService> clusterServiceProvider = getProvider(ClusterService.class);
                final Provider<IndexNameExpressionResolver> indexNameExpressionResolverProvider = getProvider(
                    IndexNameExpressionResolver.class
                );

                // DiscoveryNodeSelector is needed by several transport action constructors during
                // injector creation. That happens before createComponents(), so unlike
                // StateManager/DataAccess/RunContext we cannot wait for createComponents()
                // to initialize it.
                bind(DiscoveryNodeSelector.class).toProvider(new Provider<DiscoveryNodeSelector>() {
                    @Override
                    public DiscoveryNodeSelector get() {
                        Settings settings = settingsProvider.get();
                        boolean multiTenancyEnabled = AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.get(settings)
                            || ForecastSettings.FORECAST_MULTI_TENANCY_ENABLED.get(settings);
                        return getOrCreateDiscoveryNodeSelector(
                            multiTenancyEnabled,
                            clusterServiceProvider.get(),
                            indexNameExpressionResolverProvider.get()
                        );
                    }
                });
                bind(StateManager.class).toProvider(new Provider<StateManager>() {
                    @Override
                    public StateManager get() {
                        StateManager manager = stateManager;
                        if (manager == null) {
                            throw new IllegalStateException("StateManager has not been initialized yet");
                        }
                        return manager;
                    }
                });
                bind(DataAccess.class).toProvider(new Provider<DataAccess>() {
                    @Override
                    public DataAccess get() {
                        DataAccess access = dataAccess;
                        if (access == null) {
                            throw new IllegalStateException("DataAccess has not been initialized yet");
                        }
                        return access;
                    }
                });
                bind(RunContext.class).toProvider(new Provider<RunContext>() {
                    @Override
                    public RunContext get() {
                        RunContext context = runContext;
                        if (context == null) {
                            throw new IllegalStateException("RunContext has not been initialized yet");
                        }
                        return context;
                    }
                });
                bind(ADNodeCommunicator.class).toProvider(new Provider<ADNodeCommunicator>() {
                    @Override
                    public ADNodeCommunicator get() {
                        ADNodeCommunicator communicator = adNodeCommunicator;
                        if (communicator == null) {
                            throw new IllegalStateException("ADNodeCommunicator has not been initialized yet");
                        }
                        return communicator;
                    }
                });
                bind(ForecastNodeCommunicator.class).toProvider(new Provider<ForecastNodeCommunicator>() {
                    @Override
                    public ForecastNodeCommunicator get() {
                        ForecastNodeCommunicator communicator = forecastNodeCommunicator;
                        if (communicator == null) {
                            throw new IllegalStateException("ForecastNodeCommunicator has not been initialized yet");
                        }
                        return communicator;
                    }
                });
            }
        });
    }

    /**
     * Returns the singleton DiscoveryNodeSelector used by both Guice-time injections
     * and createComponents() wiring.
     */
    private synchronized DiscoveryNodeSelector getOrCreateDiscoveryNodeSelector(
        boolean multiTenancyEnabled,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        if (nodeFilter == null) {
            nodeFilter = multiTenancyEnabled ? new SDKNodeFilter() : new DiscoveryNodeFilterer(clusterService, indexNameExpressionResolver);
        }
        return nodeFilter;
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        boolean adMultiTenancyEnabled = AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.get(settings);

        // AD
        ADJobProcessor adJobRunner = ADJobProcessor.getInstance();
        adJobRunner.setClient(client);
        adJobRunner.setThreadPool(threadPool);
        adJobRunner.registerSettings(settings);
        adJobRunner.setIndexManagement(adDataManagement);
        adJobRunner.setTaskManager(adTaskManager);
        adJobRunner.setNodeStateManager(stateManager);
        adJobRunner.setExecuteResultResponseRecorder(adResultResponseRecorder);
        adJobRunner.setIndexJobActionHandler(adIndexJobActionHandler);
        adJobRunner.setClock(getClock());

        RestGetAnomalyDetectorAction restGetAnomalyDetectorAction = new RestGetAnomalyDetectorAction(settings);
        RestAnomalyDetectorEntityProfileAction restAnomalyDetectorEntityProfileAction = new RestAnomalyDetectorEntityProfileAction(
            settings
        );
        RestIndexAnomalyDetectorAction restIndexAnomalyDetectorAction = new RestIndexAnomalyDetectorAction(settings, clusterService);
        RestSearchAnomalyDetectorAction searchAnomalyDetectorAction = new RestSearchAnomalyDetectorAction(settings);
        RestSearchAnomalyResultAction searchAnomalyResultAction = new RestSearchAnomalyResultAction(settings);
        RestSearchADTasksAction searchADTasksAction = new RestSearchADTasksAction(settings);
        RestDeleteAnomalyDetectorAction deleteAnomalyDetectorAction = new RestDeleteAnomalyDetectorAction(settings);
        RestExecuteAnomalyDetectorAction executeAnomalyDetectorAction = new RestExecuteAnomalyDetectorAction(settings, clusterService);
        RestStatsAnomalyDetectorAction statsAnomalyDetectorAction = new RestStatsAnomalyDetectorAction(adStats, this.nodeFilter, settings);
        RestAnomalyDetectorJobAction anomalyDetectorJobAction = new RestAnomalyDetectorJobAction(settings, clusterService);
        RestSearchAnomalyDetectorInfoAction searchAnomalyDetectorInfoAction = new RestSearchAnomalyDetectorInfoAction(settings);
        RestPreviewAnomalyDetectorAction previewAnomalyDetectorAction = new RestPreviewAnomalyDetectorAction(settings);
        RestDeleteAnomalyResultsAction deleteAnomalyResultsAction = new RestDeleteAnomalyResultsAction(settings);
        RestSearchTopAnomalyResultAction searchTopAnomalyResultAction = new RestSearchTopAnomalyResultAction(settings);
        RestValidateAnomalyDetectorAction validateAnomalyDetectorAction = new RestValidateAnomalyDetectorAction(settings, clusterService);
        RestAnomalyDetectorSuggestAction suggestAnomalyDetectorAction = new RestAnomalyDetectorSuggestAction(settings, clusterService);
        RestADStatsNodesAction restADStatsNodesAction = new RestADStatsNodesAction(adStats, this.nodeFilter, settings);

        // Forecast
        RestIndexForecasterAction restIndexForecasterAction = new RestIndexForecasterAction(settings, clusterService);
        RestForecasterJobAction restForecasterJobAction = new RestForecasterJobAction();
        RestGetForecasterAction restGetForecasterAction = new RestGetForecasterAction(settings);
        RestDeleteForecasterAction deleteForecasterAction = new RestDeleteForecasterAction(settings);
        RestSearchForecasterAction searchForecasterAction = new RestSearchForecasterAction(settings);
        RestSearchForecasterInfoAction searchForecasterInfoAction = new RestSearchForecasterInfoAction(settings);
        RestSearchTopForecastResultAction searchTopForecastResultAction = new RestSearchTopForecastResultAction(settings);
        RestSearchForecastTasksAction searchForecastTasksAction = new RestSearchForecastTasksAction(settings);
        RestStatsForecasterAction statsForecasterAction = new RestStatsForecasterAction(forecastStats, this.nodeFilter);
        RestRunOnceForecasterAction runOnceForecasterAction = new RestRunOnceForecasterAction(settings);
        RestValidateForecasterAction validateForecasterAction = new RestValidateForecasterAction(settings, clusterService);
        RestForecasterSuggestAction suggestForecasterParamAction = new RestForecasterSuggestAction(settings, clusterService);

        // Common timeseries handlers
        org.opensearch.timeseries.rest.RestCronAction restCronAction = new org.opensearch.timeseries.rest.RestCronAction();

        ForecastJobProcessor forecastJobRunner = ForecastJobProcessor.getInstance();
        forecastJobRunner.setClient(client);
        forecastJobRunner.setThreadPool(threadPool);
        forecastJobRunner.registerSettings(settings);
        forecastJobRunner.setIndexManagement(forecastDataManagement);
        forecastJobRunner.setTaskManager(forecastTaskManager);
        forecastJobRunner.setNodeStateManager(stateManager);
        forecastJobRunner.setExecuteResultResponseRecorder(forecastResultResponseRecorder);
        forecastJobRunner.setIndexJobActionHandler(forecastIndexJobActionHandler);
        forecastJobRunner.setClock(getClock());

        ImmutableList.Builder<RestHandler> handlers = ImmutableList.builder();

        handlers
            .add(
                // AD
                restGetAnomalyDetectorAction,
                restAnomalyDetectorEntityProfileAction,
                restIndexAnomalyDetectorAction,
                searchAnomalyDetectorAction,
                searchAnomalyResultAction,
                searchADTasksAction,
                deleteAnomalyDetectorAction,
                executeAnomalyDetectorAction,
                anomalyDetectorJobAction,
                statsAnomalyDetectorAction,
                restADStatsNodesAction,
                searchAnomalyDetectorInfoAction,
                previewAnomalyDetectorAction,
                deleteAnomalyResultsAction,
                searchTopAnomalyResultAction,
                validateAnomalyDetectorAction,
                suggestAnomalyDetectorAction
            );

        if (adMultiTenancyEnabled) {
            handlers
                .add(
                    new RestAnomalyDetectorNodeProfileAction(settings, clusterService),
                    new RestEntityADResultAction(settings),
                    new RestADSingleStreamResultAction(settings),
                    new RestDeleteADModelAction(settings, clusterService),
                    new org.opensearch.ad.rest.RestADHCImputeAction(settings)
                );
        }

        handlers
            .add(
                // Forecast
                restIndexForecasterAction,
                restForecasterJobAction,
                restGetForecasterAction,
                deleteForecasterAction,
                searchForecasterAction,
                searchForecasterInfoAction,
                searchTopForecastResultAction,
                searchForecastTasksAction,
                statsForecasterAction,
                runOnceForecasterAction,
                validateForecasterAction,
                suggestForecasterParamAction,
                // Common timeseries
                restCronAction
            );

        return handlers.build();
    }

    private static Void initGson() {
        gson = new GsonBuilder().serializeSpecialFloatingPointValues().create();
        return null;
    }

    GenericObjectPool<LinkedBuffer> createSerializeRCFBufferPool() {
        GenericObjectPool<LinkedBuffer> pool = AccessController.doPrivileged(new PrivilegedAction<GenericObjectPool<LinkedBuffer>>() {
            @Override
            public GenericObjectPool<LinkedBuffer> run() {
                return new GenericObjectPool<>(new BasePooledObjectFactory<LinkedBuffer>() {
                    @Override
                    public LinkedBuffer create() throws Exception {
                        return LinkedBuffer.allocate(TimeSeriesSettings.SERIALIZATION_BUFFER_BYTES);
                    }

                    @Override
                    public PooledObject<LinkedBuffer> wrap(LinkedBuffer obj) {
                        return new DefaultPooledObject<>(obj);
                    }
                });
            }
        });
        pool.setMaxTotal(TimeSeriesSettings.MAX_TOTAL_RCF_SERIALIZATION_BUFFERS);
        pool.setMaxIdle(TimeSeriesSettings.MAX_TOTAL_RCF_SERIALIZATION_BUFFERS);
        pool.setMinIdle(0);
        pool.setBlockWhenExhausted(false);
        pool.setTimeBetweenEvictionRuns(TimeSeriesSettings.HOURLY_MAINTENANCE);
        return pool;
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        // =====================
        // Common components
        // =====================
        this.client = client;
        this.pluginClient = new PluginClient(client);
        this.threadPool = threadPool;
        Settings settings = environment.settings();
        this.pluginSettings = settings;
        this.clientUtil = new ClientUtil(client);
        this.indexUtils = new IndexUtils(clusterService);
        boolean adMultiTenancyEnabled = AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.get(settings);
        boolean multiTenancyEnabled = adMultiTenancyEnabled || ForecastSettings.FORECAST_MULTI_TENANCY_ENABLED.get(settings);
        String internalApiSharedSecret = TimeSeriesSettings.INTERNAL_API_SHARED_SECRET.get(settings);
        if (adMultiTenancyEnabled && (internalApiSharedSecret == null || internalApiSharedSecret.isBlank())) {
            throw new IllegalStateException(
                TimeSeriesSettings.INTERNAL_API_SHARED_SECRET.getKey() + " must be configured when AD multi-tenancy is enabled."
            );
        }
        if (multiTenancyEnabled && ParseUtils.shouldUseResourceAuthz()) {
            // TODO: we don't support resource authz for multi-tenant as it requires security plugin
            // to centralize the authN logic (e.g., verify access to resource). In a multi-tenant AD,
            // there is no security plugin. To support that, we need to either to move some of the
            // authN logic to AD plugin and make security plugin to support external
            // access resource sharing calls from outside the cluster.
            throw new IllegalStateException("Resource authz is not supported when multi-tenancy is enabled.");
        }
        this.runContext = multiTenancyEnabled ? new SdkRunContext() : new ThreadRunContext(threadPool.getThreadContext());
        this.nodeFilter = getOrCreateDiscoveryNodeSelector(multiTenancyEnabled, clusterService, indexNameExpressionResolver);
        this.indexOperations = multiTenancyEnabled ? new SDKIndexOperations(settings) : indexUtils;
        this.clusterService = clusterService;
        Imputer imputer = new LinearUniformImputer(true);

        JvmService jvmService = new JvmService(environment.settings());
        RandomCutForestMapper rcfMapper = new RandomCutForestMapper();
        rcfMapper.setSaveExecutorContextEnabled(true);
        rcfMapper.setSaveTreeStateEnabled(true);
        rcfMapper.setPartialTreeStateEnabled(true);
        V1JsonToV3StateConverter converter = new V1JsonToV3StateConverter();

        CircuitBreakerService circuitBreakerService = new CircuitBreakerService(jvmService).init();

        long heapSizeBytes = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();

        serializeRCFBufferPool = createSerializeRCFBufferPool();

        java.util.List<String> nodeRoles = org.opensearch.timeseries.settings.TimeSeriesSettings.NODE_ROLE.get(settings);
        boolean localMetadataStoreEnabled = TimeSeriesSettings.LOCAL_METADATA_STORE_ENABLED.get(settings);
        eventBridgeHandler = null;
        if (nodeRoles.contains(org.opensearch.timeseries.settings.TimeSeriesSettings.COORDINATOR_ROLE)
            && localMetadataStoreEnabled == false) {
            eventBridgeHandler = new ADEventBridgeHandler(settings, getClock());
        }

        SdkStateManager resourceStateManager = null;
        if (multiTenancyEnabled) {
            resourceStateManager = new SdkStateManager(
                client,
                xContentRegistry,
                settings,
                getClock(),
                TimeSeriesSettings.HOURLY_MAINTENANCE,
                TimeSeriesSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE,
                TimeSeriesSettings.BACKOFF_MINUTES,
                eventBridgeHandler
            );
            stateManager = resourceStateManager;
        } else {
            stateManager = new NodeStateManager(
                client,
                xContentRegistry,
                settings,
                clientUtil,
                getClock(),
                TimeSeriesSettings.HOURLY_MAINTENANCE,
                clusterService,
                TimeSeriesSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE,
                TimeSeriesSettings.BACKOFF_MINUTES,
                eventBridgeHandler
            );
        }
        securityClientUtil = new SecurityClientUtil(stateManager, settings);

        dataAccess = createDataAccess(multiTenancyEnabled, xContentRegistry, resourceStateManager, indexNameExpressionResolver);

        SearchFeatureDao searchFeatureDao = new SearchFeatureDao(
            xContentRegistry,
            dataAccess,
            clusterService,
            TimeSeriesSettings.NUM_SAMPLES_PER_TREE,
            getClock(),
            AnomalyDetectorSettings.MAX_ENTITIES_FOR_PREVIEW.get(settings),
            AnomalyDetectorSettings.AD_PAGE_SIZE.get(settings),
            AnomalyDetectorSettings.PREVIEW_TIMEOUT_IN_MILLIS
        );

        FeatureManager featureManager = new FeatureManager(
            searchFeatureDao,
            imputer,
            TimeSeriesSettings.TRAIN_SAMPLE_TIME_RANGE_IN_HOURS,
            TimeSeriesSettings.MIN_TRAIN_SAMPLES,
            AnomalyDetectorSettings.MAX_SHINGLE_PROPORTION_MISSING,
            AnomalyDetectorSettings.MAX_IMPUTATION_NEIGHBOR_DISTANCE,
            AnomalyDetectorSettings.PREVIEW_SAMPLE_RATE,
            AnomalyDetectorSettings.MAX_PREVIEW_SAMPLES,
            threadPool
        );

        Random random = new Random(42);

        // =====================
        // AD components
        // =====================
        ADEnabledSetting.getInstance().init(clusterService);
        ADNumericSetting.getInstance().init(clusterService);
        // convert from checked IOException to unchecked RuntimeException
        this.anomalyDetectionIndices = ThrowingSupplierWrapper
            .throwingSupplierWrapper(
                () -> new ADIndexManagement(
                    client,
                    clusterService,
                    threadPool,
                    settings,
                    nodeFilter,
                    TimeSeriesSettings.MAX_UPDATE_RETRY_TIMES,
                    xContentRegistry,
                    dataAccess
                )
            )
            .get();

        SDKDataManagement<ADIndex, AnomalyResult> adSdkConfigStore = new ADSdkDataManagement(
            client,
            xContentRegistry,
            settings,
            clusterService
        );
        this.adDataManagement = new ADDelegatingDataManagement(anomalyDetectionIndices, adSdkConfigStore, clusterService);

        double adModelMaxSizePercent = AnomalyDetectorSettings.AD_MODEL_MAX_SIZE_PERCENTAGE.get(settings);

        MemoryTracker adMemoryTracker = new MemoryTracker(jvmService, adModelMaxSizePercent, clusterService, circuitBreakerService);

        ThresholdedRandomCutForestMapper trcfMapper = new ThresholdedRandomCutForestMapper();
        Schema<ThresholdedRandomCutForestState> trcfSchema = AccessController
            .doPrivileged(
                (PrivilegedAction<Schema<ThresholdedRandomCutForestState>>) () -> RuntimeSchema
                    .getSchema(ThresholdedRandomCutForestState.class)
            );

        double anomalyRate = 1 - TimeSeriesSettings.THRESHOLD_MIN_PVALUE;

        ADCheckpointDao indexCheckpointStore = new ADCheckpointDao(
            client,
            clientUtil,
            gson,
            rcfMapper,
            converter,
            trcfMapper,
            trcfSchema,
            HybridThresholdingModel.class,
            adDataManagement,
            TimeSeriesSettings.MAX_CHECKPOINT_BYTES,
            serializeRCFBufferPool,
            TimeSeriesSettings.SERIALIZATION_BUFFER_BYTES,
            anomalyRate,
            getClock()
        );

        ADS3CheckpointDao s3CheckpointStore = null;
        if (AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.get(settings) && localMetadataStoreEnabled == false) {
            try {
                s3CheckpointStore = new ADS3CheckpointDao(
                    settings,
                    TimeSeriesSettings.MAX_CHECKPOINT_BYTES,
                    trcfSchema,
                    trcfMapper,
                    converter,
                    gson,
                    rcfMapper,
                    HybridThresholdingModel.class,
                    anomalyRate,
                    getClock(),
                    serializeRCFBufferPool,
                    TimeSeriesSettings.SERIALIZATION_BUFFER_BYTES,
                    adDataManagement,
                    indexOperations
                );
            } catch (Exception e) {
                LOG.warn("Failed to initialise S3-backed checkpoint store; falling back to index-backed checkpoints.", e);
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("S3 checkpoint store not configured for this node; using index-backed checkpoints");
        }

        ADCheckpointStore adCheckpoint = new DelegatingADCheckpointStore(indexCheckpointStore, s3CheckpointStore, clusterService);

        ADCacheProvider adCacheProvider = new ADCacheProvider();

        CheckPointMaintainRequestAdapter<ThresholdedRandomCutForest, ADIndex, ADDelegatingDataManagement, ADCheckpointStore, ADPriorityCache> adAdapter =
            new CheckPointMaintainRequestAdapter<>(
                adCheckpoint,
                ADCommonName.CHECKPOINT_INDEX_NAME,
                AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ,
                getClock(),
                clusterService,
                settings,
                adCacheProvider
            );

        ADCheckpointWriteWorker adCheckpointWriteQueue = new ADCheckpointWriteWorker(
            heapSizeBytes,
            TimeSeriesSettings.CHECKPOINT_WRITE_QUEUE_SIZE_IN_BYTES,
            AnomalyDetectorSettings.AD_CHECKPOINT_WRITE_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            circuitBreakerService,
            threadPool,
            settings,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            TimeSeriesSettings.QUEUE_MAINTENANCE,
            adCheckpoint,
            ADCommonName.CHECKPOINT_INDEX_NAME,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            stateManager,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            indexOperations
        );

        ADCheckpointMaintainWorker adCheckpointMaintainQueue = new ADCheckpointMaintainWorker(
            heapSizeBytes,
            TimeSeriesSettings.CHECKPOINT_MAINTAIN_REQUEST_SIZE_IN_BYTES,
            AnomalyDetectorSettings.AD_CHECKPOINT_MAINTAIN_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            circuitBreakerService,
            threadPool,
            settings,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            adCheckpointWriteQueue,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            stateManager,
            adAdapter::convert
        );

        ADPriorityCache adPriorityCache = new ADPriorityCache(
            adCheckpoint,
            AnomalyDetectorSettings.AD_DEDICATED_CACHE_SIZE.get(settings),
            AnomalyDetectorSettings.AD_CHECKPOINT_TTL,
            AnomalyDetectorSettings.MAX_INACTIVE_ENTITIES,
            adMemoryTracker,
            TimeSeriesSettings.NUM_TREES,
            getClock(),
            clusterService,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            threadPool,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            settings,
            AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ,
            adCheckpointWriteQueue,
            adCheckpointMaintainQueue,
            stateManager
        );

        // cache provider allows us to break circular dependency among PriorityCache, CacheBuffer,
        // CheckPointMaintainRequestAdapter, and CheckpointMaintainWorker
        adCacheProvider.set(adPriorityCache);

        int adResultMappingVersion = adDataManagement.getSchemaVersion(ADIndex.RESULT);

        ADColdStart adEntityColdStarter = new ADColdStart(
            getClock(),
            threadPool,
            stateManager,
            TimeSeriesSettings.NUM_SAMPLES_PER_TREE,
            TimeSeriesSettings.NUM_TREES,
            TimeSeriesSettings.NUM_MIN_SAMPLES,
            AnomalyDetectorSettings.MAX_SAMPLE_STRIDE,
            AnomalyDetectorSettings.MAX_TRAIN_SAMPLE,
            searchFeatureDao,
            TimeSeriesSettings.THRESHOLD_MIN_PVALUE,
            featureManager,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            TimeSeriesSettings.MAX_COLD_START_ROUNDS,
            (int) (AD_COOLDOWN_MINUTES.get(settings).getMinutes()),
            adResultMappingVersion
        );

        ADModelManager adModelManager = new ADModelManager(
            adCheckpoint,
            getClock(),
            TimeSeriesSettings.NUM_TREES,
            TimeSeriesSettings.NUM_SAMPLES_PER_TREE,
            TimeSeriesSettings.NUM_MIN_SAMPLES,
            TimeSeriesSettings.THRESHOLD_MIN_PVALUE,
            AnomalyDetectorSettings.MIN_PREVIEW_SIZE,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ,
            adEntityColdStarter,
            featureManager,
            adMemoryTracker,
            settings,
            clusterService,
            stateManager
        );

        ADIndexMemoryPressureAwareResultHandler adIndexMemoryPressureAwareResultHandler = new ADIndexMemoryPressureAwareResultHandler(
            client,
            adDataManagement,
            nodeFilter
        );

        ADResultWriteWorker adResultWriteQueue = new ADResultWriteWorker(
            heapSizeBytes,
            TimeSeriesSettings.RESULT_WRITE_QUEUE_SIZE_IN_BYTES,
            AnomalyDetectorSettings.AD_RESULT_WRITE_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            circuitBreakerService,
            threadPool,
            settings,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            TimeSeriesSettings.QUEUE_MAINTENANCE,
            adIndexMemoryPressureAwareResultHandler,
            xContentRegistry,
            stateManager,
            TimeSeriesSettings.HOURLY_MAINTENANCE
        );

        ADSaveResultStrategy adSaveResultStrategy = new ADSaveResultStrategy(adResultMappingVersion, adResultWriteQueue);

        ADDataMigrator adDataMigrator = new ADDataMigrator(
            client,
            clusterService,
            xContentRegistry,
            anomalyDetectionIndices,
            adDataManagement,
            stateManager
        );
        HashRing hashRing = new HashRing(nodeFilter, getClock(), settings, dataAccess, clusterService, adDataMigrator, adCacheProvider);
        ADTaskProfileRunner adTaskProfileRunner = new ADTaskProfileRunner(hashRing, client);
        ADTaskCacheManager adTaskCacheManager = new ADTaskCacheManager(settings, clusterService, adMemoryTracker);
        adTaskManager = new ADTaskManager(
            settings,
            clusterService,
            client,
            xContentRegistry,
            nodeFilter,
            hashRing,
            adTaskCacheManager,
            threadPool,
            stateManager,
            dataAccess,
            adDataManagement,
            adTaskProfileRunner
        );

        ADColdStartWorker adColdstartQueue = new ADColdStartWorker(
            heapSizeBytes,
            TimeSeriesSettings.FEATURE_REQUEST_SIZE_IN_BYTES,
            AnomalyDetectorSettings.AD_ENTITY_COLD_START_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            circuitBreakerService,
            threadPool,
            settings,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            TimeSeriesSettings.QUEUE_MAINTENANCE,
            adEntityColdStarter,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            stateManager,
            adPriorityCache,
            adModelManager,
            adSaveResultStrategy,
            adTaskManager,
            adCheckpointWriteQueue
        );

        Map<String, TimeSeriesStat<?>> adStatsMap = ImmutableMap
            .<String, TimeSeriesStat<?>>builder()
            // ad stats
            .put(StatNames.AD_EXECUTE_REQUEST_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_EXECUTE_FAIL_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_HC_EXECUTE_REQUEST_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_HC_EXECUTE_FAIL_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(
                StatNames.ANOMALY_RESULTS_INDEX_STATUS.getName(),
                new TimeSeriesStat<>(true, new IndexStatusSupplier(indexOperations, ADCommonName.ANOMALY_RESULT_INDEX_ALIAS))
            )
            .put(
                StatNames.AD_MODELS_CHECKPOINT_INDEX_STATUS.getName(),
                new TimeSeriesStat<>(true, new IndexStatusSupplier(indexOperations, ADCommonName.CHECKPOINT_INDEX_NAME))
            )
            .put(
                StatNames.ANOMALY_DETECTION_STATE_STATUS.getName(),
                new TimeSeriesStat<>(true, new IndexStatusSupplier(indexOperations, ADCommonName.DETECTION_STATE_INDEX))
            )
            .put(StatNames.DETECTOR_COUNT.getName(), new TimeSeriesStat<>(true, new SettableSupplier()))
            .put(StatNames.SINGLE_STREAM_DETECTOR_COUNT.getName(), new TimeSeriesStat<>(true, new SettableSupplier()))
            .put(StatNames.HC_DETECTOR_COUNT.getName(), new TimeSeriesStat<>(true, new SettableSupplier()))
            .put(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_CANCELED_BATCH_TASK_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_TOTAL_BATCH_TASK_EXECUTION_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_BATCH_TASK_FAILURE_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_MODEL_CORRUTPION_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(
                StatNames.MODEL_INFORMATION.getName(),
                new TimeSeriesStat<>(false, new ADModelsOnNodeSupplier(adCacheProvider, settings, clusterService))
            )
            .put(
                StatNames.AD_CONFIG_INDEX_STATUS.getName(),
                new TimeSeriesStat<>(true, new IndexStatusSupplier(indexOperations, ADCommonName.CONFIG_INDEX))
            )
            .put(
                StatNames.FORECAST_CONFIG_INDEX_STATUS.getName(),
                new TimeSeriesStat<>(true, new IndexStatusSupplier(indexOperations, ForecastCommonName.CONFIG_INDEX))
            )
            .put(
                StatNames.JOB_INDEX_STATUS.getName(),
                new TimeSeriesStat<>(true, new IndexStatusSupplier(indexOperations, CommonName.JOB_INDEX))
            )
            .put(StatNames.MODEL_COUNT.getName(), new TimeSeriesStat<>(false, new ADModelsOnNodeCountSupplier(adCacheProvider)))
            .build();

        adStats = new ADStats(adStatsMap);

        ADRealTimeInferencer adInferencer = new ADRealTimeInferencer(
            adModelManager,
            adStats,
            adCheckpoint,
            adColdstartQueue,
            adSaveResultStrategy,
            adCacheProvider,
            threadPool,
            getClock(),
            searchFeatureDao
        );

        ADCheckpointReadWorker adCheckpointReadQueue = new ADCheckpointReadWorker(
            heapSizeBytes,
            TimeSeriesSettings.FEATURE_REQUEST_SIZE_IN_BYTES,
            AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            circuitBreakerService,
            threadPool,
            settings,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            TimeSeriesSettings.QUEUE_MAINTENANCE,
            adModelManager,
            adCheckpoint,
            adColdstartQueue,
            stateManager,
            adCacheProvider,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            adCheckpointWriteQueue,
            adInferencer,
            indexOperations
        );

        ADColdEntityWorker adColdEntityQueue = new ADColdEntityWorker(
            heapSizeBytes,
            TimeSeriesSettings.FEATURE_REQUEST_SIZE_IN_BYTES,
            AnomalyDetectorSettings.AD_COLD_ENTITY_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            circuitBreakerService,
            threadPool,
            settings,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            adCheckpointReadQueue,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            stateManager
        );

        anomalyDetectorRunner = new AnomalyDetectorRunner(adModelManager, featureManager, AnomalyDetectorSettings.MAX_PREVIEW_RESULTS);

        ResultBulkIndexingHandler<AnomalyResult, ADIndex, ADDelegatingDataManagement> anomalyResultBulkIndexHandler =
            new ResultBulkIndexingHandler<>(
                dataAccess,
                settings,
                threadPool,
                ANOMALY_RESULT_INDEX_ALIAS,
                adDataManagement,
                nodeFilter,
                AnomalyDetectorSettings.AD_BACKOFF_INITIAL_DELAY,
                AnomalyDetectorSettings.AD_MAX_RETRY_FOR_BACKOFF
            );

        ADSearchHandler adSearchHandler = new ADSearchHandler(settings, clusterService, pluginClient, dataAccess, runContext);

        ResultBulkIndexingHandler<AnomalyResult, ADIndex, ADDelegatingDataManagement> anomalyResultHandler =
            new ResultBulkIndexingHandler<>(
                dataAccess,
                settings,
                threadPool,
                ANOMALY_RESULT_INDEX_ALIAS,
                adDataManagement,
                nodeFilter,
                AnomalyDetectorSettings.AD_BACKOFF_INITIAL_DELAY,
                AnomalyDetectorSettings.AD_MAX_RETRY_FOR_BACKOFF
            );

        // =====================
        // common components, need AD/forecasting components to initialize
        // =====================

        adBatchTaskRunner = new ADBatchTaskRunner(
            settings,
            threadPool,
            clusterService,
            client,
            circuitBreakerService,
            featureManager,
            adTaskManager,
            adStats,
            anomalyResultBulkIndexHandler,
            adTaskCacheManager,
            searchFeatureDao,
            hashRing,
            adModelManager,
            adResultMappingVersion,
            dataAccess
        );

        adNodeCommunicator = adMultiTenancyEnabled
            ? new ADHttpNodeCommunicator(hashRing, internalApiSharedSecret)
            : new ADTransportNodeCommunicator(client, nodeFilter);

        adResultResponseRecorder = new ExecuteADResultResponseRecorder(
            anomalyResultHandler,
            adTaskManager,
            nodeFilter,
            threadPool,
            adNodeCommunicator,
            dataAccess,
            stateManager,
            getClock(),
            adResultMappingVersion
        );

        // Use EventBridge for starting jobs on coordinator nodes; otherwise default indexing-based starter

        if (nodeRoles.contains(org.opensearch.timeseries.settings.TimeSeriesSettings.COORDINATOR_ROLE) && eventBridgeHandler != null) {
            adIndexJobActionHandler = new ADIndexJobActionHandler(
                client,
                adDataManagement,
                xContentRegistry,
                adTaskManager,
                adResultResponseRecorder,
                stateManager,
                settings,
                eventBridgeHandler::startJob,
                eventBridgeHandler::stopJob,
                runContext
            );
        } else {
            adIndexJobActionHandler = new ADIndexJobActionHandler(
                client,
                adDataManagement,
                xContentRegistry,
                adTaskManager,
                adResultResponseRecorder,
                stateManager,
                settings,
                runContext
            );
        }

        // =====================
        // forecast components
        // =====================
        ForecastEnabledSetting.getInstance().init(clusterService);
        ForecastNumericSetting.getInstance().init(clusterService);

        forecastIndices = ThrowingSupplierWrapper
            .throwingSupplierWrapper(
                () -> new ForecastIndexManagement(
                    client,
                    clusterService,
                    threadPool,
                    settings,
                    nodeFilter,
                    ForecastSettings.FORECAST_MAX_UPDATE_RETRY_TIMES,
                    xContentRegistry,
                    dataAccess
                )
            )
            .get();

        this.forecastDataManagement = new ForecastDelegatingDataManagement(forecastIndices, null, clusterService);

        int forecastResultMappingVersion = forecastDataManagement.getSchemaVersion(ForecastIndex.RESULT);

        double forecastModelMaxSizePercent = ForecastSettings.FORECAST_MODEL_MAX_SIZE_PERCENTAGE.get(settings);

        MemoryTracker forecastMemoryTracker = new MemoryTracker(
            jvmService,
            forecastModelMaxSizePercent,
            clusterService,
            circuitBreakerService
        );

        ForecastCheckpointDao forecastCheckpoint = new ForecastCheckpointDao(
            client,
            clientUtil,
            gson,
            TimeSeriesSettings.MAX_CHECKPOINT_BYTES,
            serializeRCFBufferPool,
            TimeSeriesSettings.SERIALIZATION_BUFFER_BYTES,
            forecastDataManagement,
            new RCFCasterMapper(),
            AccessController.doPrivileged((PrivilegedAction<Schema<RCFCasterState>>) () -> RuntimeSchema.getSchema(RCFCasterState.class)),
            getClock()
        );

        ForecastCacheProvider forecastCacheProvider = new ForecastCacheProvider();

        CheckPointMaintainRequestAdapter<RCFCaster, ForecastIndex, ForecastDelegatingDataManagement, ForecastCheckpointDao, ForecastPriorityCache> forecastAdapter =
            new CheckPointMaintainRequestAdapter<RCFCaster, ForecastIndex, ForecastDelegatingDataManagement, ForecastCheckpointDao, ForecastPriorityCache>(
                forecastCheckpoint,
                ForecastIndex.CHECKPOINT.getIndexName(),
                ForecastSettings.FORECAST_CHECKPOINT_SAVING_FREQ,
                getClock(),
                clusterService,
                settings,
                forecastCacheProvider
            );

        ForecastCheckpointWriteWorker forecastCheckpointWriteQueue = new ForecastCheckpointWriteWorker(
            heapSizeBytes,
            TimeSeriesSettings.CHECKPOINT_WRITE_QUEUE_SIZE_IN_BYTES,
            ForecastSettings.FORECAST_CHECKPOINT_WRITE_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            circuitBreakerService,
            threadPool,
            settings,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            TimeSeriesSettings.QUEUE_MAINTENANCE,
            forecastCheckpoint,
            ForecastIndex.CHECKPOINT.getIndexName(),
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            stateManager,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            indexOperations
        );

        ForecastCheckpointMaintainWorker forecastCheckpointMaintainQueue = new ForecastCheckpointMaintainWorker(
            heapSizeBytes,
            TimeSeriesSettings.CHECKPOINT_MAINTAIN_REQUEST_SIZE_IN_BYTES,
            ForecastSettings.FORECAST_CHECKPOINT_MAINTAIN_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            circuitBreakerService,
            threadPool,
            settings,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            forecastCheckpointWriteQueue,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            stateManager,
            forecastAdapter::convert
        );

        ForecastPriorityCache forecastPriorityCache = new ForecastPriorityCache(
            forecastCheckpoint,
            ForecastSettings.FORECAST_DEDICATED_CACHE_SIZE.get(settings),
            AnomalyDetectorSettings.AD_CHECKPOINT_TTL,
            AnomalyDetectorSettings.MAX_INACTIVE_ENTITIES,
            adMemoryTracker,
            TimeSeriesSettings.NUM_TREES,
            getClock(),
            clusterService,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            threadPool,
            ForecastCommonName.FORECAST_THREAD_POOL_NAME,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            settings,
            ForecastSettings.FORECAST_CHECKPOINT_SAVING_FREQ,
            forecastCheckpointWriteQueue,
            forecastCheckpointMaintainQueue,
            stateManager
        );

        // cache provider allows us to break circular dependency among PriorityCache, CacheBuffer,
        // CheckPointMaintainRequestAdapter, and CheckpointMaintainWorker
        forecastCacheProvider.set(forecastPriorityCache);

        ForecastColdStart forecastColdStarter = new ForecastColdStart(
            getClock(),
            threadPool,
            stateManager,
            TimeSeriesSettings.NUM_SAMPLES_PER_TREE,
            TimeSeriesSettings.NUM_TREES,
            TimeSeriesSettings.NUM_MIN_SAMPLES,
            searchFeatureDao,
            TimeSeriesSettings.THRESHOLD_MIN_PVALUE,
            featureManager,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            (int) (AD_COOLDOWN_MINUTES.get(settings).getMinutes()),
            -1, // no hard coded random seed
            -1, // interpolation is disabled so we don't need to specify the number of sampled points
            TimeSeriesSettings.MAX_COLD_START_ROUNDS,
            forecastResultMappingVersion
        );

        ForecastModelManager forecastModelManager = new ForecastModelManager(
            forecastCheckpoint,
            getClock(),
            TimeSeriesSettings.NUM_TREES,
            TimeSeriesSettings.NUM_SAMPLES_PER_TREE,
            TimeSeriesSettings.NUM_MIN_SAMPLES,
            forecastColdStarter,
            forecastMemoryTracker,
            featureManager,
            stateManager
        );

        ForecastIndexMemoryPressureAwareResultHandler forecastIndexMemoryPressureAwareResultHandler =
            new ForecastIndexMemoryPressureAwareResultHandler(client, forecastDataManagement, nodeFilter);

        ForecastResultWriteWorker forecastResultWriteQueue = new ForecastResultWriteWorker(
            heapSizeBytes,
            TimeSeriesSettings.RESULT_WRITE_QUEUE_SIZE_IN_BYTES,
            ForecastSettings.FORECAST_RESULT_WRITE_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            circuitBreakerService,
            threadPool,
            settings,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            TimeSeriesSettings.QUEUE_MAINTENANCE,
            forecastIndexMemoryPressureAwareResultHandler,
            xContentRegistry,
            stateManager,
            TimeSeriesSettings.HOURLY_MAINTENANCE
        );

        ForecastSaveResultStrategy forecastSaveResultStrategy = new ForecastSaveResultStrategy(
            forecastResultMappingVersion,
            forecastResultWriteQueue
        );

        TaskCacheManager forecastTaskCacheManager = new TaskCacheManager(settings, clusterService);
        DataAccess forecastTaskSearcher = createDataAccess(
            ForecastSettings.FORECAST_MULTI_TENANCY_ENABLED.get(settings),
            xContentRegistry,
            resourceStateManager,
            indexNameExpressionResolver
        );

        forecastTaskManager = new ForecastTaskManager(
            forecastTaskCacheManager,
            xContentRegistry,
            clusterService,
            settings,
            threadPool,
            forecastTaskSearcher,
            forecastDataManagement,
            stateManager
        );

        ForecastColdStartWorker forecastColdstartQueue = new ForecastColdStartWorker(
            heapSizeBytes,
            TimeSeriesSettings.FEATURE_REQUEST_SIZE_IN_BYTES,
            ForecastSettings.FORECAST_COLD_START_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            circuitBreakerService,
            threadPool,
            settings,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            TimeSeriesSettings.QUEUE_MAINTENANCE,
            forecastColdStarter,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            stateManager,
            forecastPriorityCache,
            forecastModelManager,
            forecastSaveResultStrategy,
            forecastTaskManager,
            forecastCheckpointWriteQueue
        );

        Map<String, TimeSeriesStat<?>> forecastStatsMap = ImmutableMap
            .<String, TimeSeriesStat<?>>builder()
            // forecast stats
            .put(StatNames.FORECAST_EXECUTE_REQUEST_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(StatNames.FORECAST_EXECUTE_FAIL_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(StatNames.FORECAST_HC_EXECUTE_REQUEST_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(StatNames.FORECAST_HC_EXECUTE_FAIL_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(
                StatNames.FORECAST_RESULTS_INDEX_STATUS.getName(),
                new TimeSeriesStat<>(true, new IndexStatusSupplier(indexOperations, ForecastIndex.RESULT.getIndexName()))
            )
            .put(
                StatNames.FORECAST_MODELS_CHECKPOINT_INDEX_STATUS.getName(),
                new TimeSeriesStat<>(true, new IndexStatusSupplier(indexOperations, ForecastIndex.CHECKPOINT.getIndexName()))
            )
            .put(
                StatNames.FORECAST_STATE_STATUS.getName(),
                new TimeSeriesStat<>(true, new IndexStatusSupplier(indexOperations, ForecastIndex.STATE.getIndexName()))
            )
            .put(StatNames.FORECASTER_COUNT.getName(), new TimeSeriesStat<>(true, new SettableSupplier()))
            .put(StatNames.SINGLE_STREAM_FORECASTER_COUNT.getName(), new TimeSeriesStat<>(true, new SettableSupplier()))
            .put(StatNames.HC_FORECASTER_COUNT.getName(), new TimeSeriesStat<>(true, new SettableSupplier()))
            .put(StatNames.FORECAST_MODEL_CORRUPTION_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(
                StatNames.MODEL_INFORMATION.getName(),
                new TimeSeriesStat<>(false, new ForecastModelsOnNodeSupplier(forecastCacheProvider, settings, clusterService))
            )
            .put(
                StatNames.FORECAST_CONFIG_INDEX_STATUS.getName(),
                new TimeSeriesStat<>(true, new IndexStatusSupplier(indexOperations, ForecastCommonName.CONFIG_INDEX))
            )
            .put(
                StatNames.JOB_INDEX_STATUS.getName(),
                new TimeSeriesStat<>(true, new IndexStatusSupplier(indexOperations, CommonName.JOB_INDEX))
            )
            .put(StatNames.MODEL_COUNT.getName(), new TimeSeriesStat<>(false, new ForecastModelsOnNodeCountSupplier(forecastCacheProvider)))
            .build();

        forecastStats = new ForecastStats(forecastStatsMap);

        ForecastRealTimeInferencer forecastInferencer = new ForecastRealTimeInferencer(
            forecastModelManager,
            forecastStats,
            forecastCheckpoint,
            forecastColdstartQueue,
            forecastSaveResultStrategy,
            forecastCacheProvider,
            threadPool,
            getClock(),
            searchFeatureDao
        );

        ForecastCheckpointReadWorker forecastCheckpointReadQueue = new ForecastCheckpointReadWorker(
            heapSizeBytes,
            TimeSeriesSettings.FEATURE_REQUEST_SIZE_IN_BYTES,
            ForecastSettings.FORECAST_CHECKPOINT_READ_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            circuitBreakerService,
            threadPool,
            settings,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            TimeSeriesSettings.QUEUE_MAINTENANCE,
            forecastModelManager,
            forecastCheckpoint,
            forecastColdstartQueue,
            stateManager,
            forecastCacheProvider,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            forecastCheckpointWriteQueue,
            forecastInferencer,
            indexOperations
        );

        ForecastColdEntityWorker forecastColdEntityQueue = new ForecastColdEntityWorker(
            heapSizeBytes,
            TimeSeriesSettings.FEATURE_REQUEST_SIZE_IN_BYTES,
            ForecastSettings.FORECAST_COLD_ENTITY_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            circuitBreakerService,
            threadPool,
            settings,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            forecastCheckpointReadQueue,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            stateManager
        );

        ResultBulkIndexingHandler<ForecastResult, ForecastIndex, ForecastDelegatingDataManagement> forecastResultHandler =
            new ResultBulkIndexingHandler<>(
                dataAccess,
                settings,
                threadPool,
                ForecastIndex.RESULT.getIndexName(),
                forecastDataManagement,
                nodeFilter,
                ForecastSettings.FORECAST_BACKOFF_INITIAL_DELAY,
                ForecastSettings.FORECAST_MAX_RETRY_FOR_BACKOFF
            );

        forecastNodeCommunicator = new ForecastTransportNodeCommunicator(client, nodeFilter);

        forecastResultResponseRecorder = new ExecuteForecastResultResponseRecorder(
            forecastResultHandler,
            forecastTaskManager,
            nodeFilter,
            threadPool,
            forecastNodeCommunicator,
            dataAccess,
            stateManager,
            getClock(),
            forecastResultMappingVersion
        );

        ForecastSearchHandler forecastSearchHandler = new ForecastSearchHandler(
            settings,
            clusterService,
            pluginClient,
            forecastTaskSearcher,
            runContext
        );

        forecastIndexJobActionHandler = new ForecastIndexJobActionHandler(
            client,
            forecastDataManagement,
            xContentRegistry,
            forecastTaskManager,
            forecastResultResponseRecorder,
            stateManager,
            settings,
            runContext
        );

        List<Object> components = new ArrayList<>(
            ImmutableList
                .of(
                    // return objects used by Guice to inject dependencies for e.g.,
                    // transport action handler constructors
                    // common components
                    searchFeatureDao,
                    imputer,
                    gson,
                    jvmService,
                    hashRing,
                    featureManager,
                    stateManager,
                    new ClusterEventListener(clusterService, hashRing),
                    circuitBreakerService,
                    new ClusterManagerTaskRegistry(
                        clusterService,
                        threadPool,
                        client,
                        getClock(),
                        dataAccess,
                        nodeFilter,
                        settings,
                        hashRing,
                        stateManager,
                        xContentRegistry,
                        runContext,
                        eventBridgeHandler
                    ),
                    nodeFilter,
                    // AD components
                    anomalyDetectionIndices,
                    adDataManagement,
                    anomalyDetectorRunner,
                    adModelManager,
                    adStats,
                    adIndexMemoryPressureAwareResultHandler,
                    adCheckpoint,
                    adCacheProvider,
                    adTaskManager,
                    adBatchTaskRunner,
                    adSearchHandler,
                    adColdstartQueue,
                    adResultWriteQueue,
                    adCheckpointReadQueue,
                    adCheckpointWriteQueue,
                    adColdEntityQueue,
                    adEntityColdStarter,
                    adTaskCacheManager,
                    adResultResponseRecorder,
                    adIndexJobActionHandler,
                    adSaveResultStrategy,
                    new ADTaskProfileRunner(hashRing, client),
                    adInferencer,
                    // forecast components
                    forecastIndices,
                    forecastDataManagement,
                    forecastStats,
                    forecastModelManager,
                    forecastIndexMemoryPressureAwareResultHandler,
                    forecastCheckpoint,
                    forecastCacheProvider,
                    forecastColdstartQueue,
                    forecastResultWriteQueue,
                    forecastCheckpointReadQueue,
                    forecastCheckpointWriteQueue,
                    forecastColdEntityQueue,
                    forecastColdStarter,
                    forecastTaskManager,
                    forecastSearchHandler,
                    forecastIndexJobActionHandler,
                    forecastTaskCacheManager,
                    forecastSaveResultStrategy,
                    new ForecastTaskProfileRunner(),
                    forecastInferencer,
                    pluginClient
                )
        );

        return components;
    }

    /**
     * createComponents doesn't work for Clock as OS process cannot start
     * complaining it cannot find Clock instances for transport actions constructors.
     * @return a UTC clock
     */
    protected Clock getClock() {
        return Clock.systemUTC();
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        List<ExecutorBuilderContributor> contributors = List
            .of(
                new CloudMapWatcherContributor(),
                new SQSConsumerContributor(),
                new ADCoordinatorContributor(),
                new ADModelContributor(),
                new ForecastModelContributor()
            );
        List<ExecutorBuilder<?>> builders = new ArrayList<>();
        contributors.forEach(c -> c.contribute(settings, builders));
        return List.copyOf(builders);
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> adEnabledSetting = ADEnabledSetting.getInstance().getSettings();
        List<Setting<?>> adNumericSetting = ADNumericSetting.getInstance().getSettings();

        List<Setting<?>> forecastEnabledSetting = ForecastEnabledSetting.getInstance().getSettings();
        List<Setting<?>> forecastNumericSetting = ForecastNumericSetting.getInstance().getSettings();

        List<Setting<?>> timeSeriesEnabledSetting = TimeSeriesEnabledSetting.getInstance().getSettings();

        List<Setting<?>> systemSetting = ImmutableList
            .of(
                // ======================================
                // AD settings
                // ======================================
                // HCAD cache
                LegacyOpenDistroAnomalyDetectorSettings.MAX_CACHE_MISS_HANDLING_PER_SECOND,
                AnomalyDetectorSettings.AD_DEDICATED_CACHE_SIZE,
                // Detector config
                LegacyOpenDistroAnomalyDetectorSettings.DETECTION_INTERVAL,
                LegacyOpenDistroAnomalyDetectorSettings.DETECTION_WINDOW_DELAY,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_ANOMALY_FEATURES,
                AnomalyDetectorSettings.DETECTION_INTERVAL,
                AnomalyDetectorSettings.DETECTION_WINDOW_DELAY,
                AnomalyDetectorSettings.MAX_ANOMALY_FEATURES,
                // Fault tolerance
                LegacyOpenDistroAnomalyDetectorSettings.REQUEST_TIMEOUT,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE,
                LegacyOpenDistroAnomalyDetectorSettings.COOLDOWN_MINUTES,
                LegacyOpenDistroAnomalyDetectorSettings.BACKOFF_MINUTES,
                LegacyOpenDistroAnomalyDetectorSettings.BACKOFF_INITIAL_DELAY,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_RETRY_FOR_BACKOFF,
                AnomalyDetectorSettings.AD_REQUEST_TIMEOUT,
                AnomalyDetectorSettings.AD_MAX_RETRY_FOR_UNRESPONSIVE_NODE,
                AnomalyDetectorSettings.AD_COOLDOWN_MINUTES,
                AnomalyDetectorSettings.AD_BACKOFF_MINUTES,
                AnomalyDetectorSettings.AD_BACKOFF_INITIAL_DELAY,
                AnomalyDetectorSettings.AD_MAX_RETRY_FOR_BACKOFF,
                // result index rollover
                LegacyOpenDistroAnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD,
                LegacyOpenDistroAnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS,
                LegacyOpenDistroAnomalyDetectorSettings.AD_RESULT_HISTORY_RETENTION_PERIOD,
                AnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD,
                AnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS_PER_SHARD,
                AnomalyDetectorSettings.AD_RESULT_HISTORY_RETENTION_PERIOD,
                // resource usage control
                LegacyOpenDistroAnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_SINGLE_ENTITY_ANOMALY_DETECTORS,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_MULTI_ENTITY_ANOMALY_DETECTORS,
                LegacyOpenDistroAnomalyDetectorSettings.INDEX_PRESSURE_SOFT_LIMIT,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_PRIMARY_SHARDS,
                AnomalyDetectorSettings.AD_MODEL_MAX_SIZE_PERCENTAGE,
                AnomalyDetectorSettings.AD_MAX_SINGLE_ENTITY_ANOMALY_DETECTORS,
                AnomalyDetectorSettings.AD_MAX_HC_ANOMALY_DETECTORS,
                AnomalyDetectorSettings.AD_INDEX_PRESSURE_SOFT_LIMIT,
                AnomalyDetectorSettings.AD_INDEX_PRESSURE_HARD_LIMIT,
                AnomalyDetectorSettings.AD_MAX_PRIMARY_SHARDS,
                // Security
                LegacyOpenDistroAnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES,
                AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES,
                // Historical
                LegacyOpenDistroAnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE,
                LegacyOpenDistroAnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR,
                LegacyOpenDistroAnomalyDetectorSettings.BATCH_TASK_PIECE_SIZE,
                AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE,
                AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS,
                AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR,
                AnomalyDetectorSettings.BATCH_TASK_PIECE_SIZE,
                AnomalyDetectorSettings.MAX_TOP_ENTITIES_FOR_HISTORICAL_ANALYSIS,
                AnomalyDetectorSettings.MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS,
                AnomalyDetectorSettings.MAX_CACHED_DELETED_TASKS,
                // rate limiting
                AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_CONCURRENCY,
                AnomalyDetectorSettings.AD_CHECKPOINT_WRITE_QUEUE_CONCURRENCY,
                AnomalyDetectorSettings.AD_ENTITY_COLD_START_QUEUE_CONCURRENCY,
                AnomalyDetectorSettings.AD_RESULT_WRITE_QUEUE_CONCURRENCY,
                AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_BATCH_SIZE,
                AnomalyDetectorSettings.AD_CHECKPOINT_WRITE_QUEUE_BATCH_SIZE,
                AnomalyDetectorSettings.AD_RESULT_WRITE_QUEUE_BATCH_SIZE,
                AnomalyDetectorSettings.AD_COLD_ENTITY_QUEUE_MAX_HEAP_PERCENT,
                AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_MAX_HEAP_PERCENT,
                AnomalyDetectorSettings.AD_CHECKPOINT_WRITE_QUEUE_MAX_HEAP_PERCENT,
                AnomalyDetectorSettings.AD_RESULT_WRITE_QUEUE_MAX_HEAP_PERCENT,
                AnomalyDetectorSettings.AD_CHECKPOINT_MAINTAIN_QUEUE_MAX_HEAP_PERCENT,
                AnomalyDetectorSettings.AD_ENTITY_COLD_START_QUEUE_MAX_HEAP_PERCENT,
                AnomalyDetectorSettings.AD_EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_MILLISECS,
                AnomalyDetectorSettings.AD_EXPECTED_CHECKPOINT_MAINTAIN_TIME_IN_MILLISECS,
                AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ,
                AnomalyDetectorSettings.AD_CHECKPOINT_TTL,
                // query limit
                LegacyOpenDistroAnomalyDetectorSettings.MAX_ENTITIES_PER_QUERY,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_ENTITIES_FOR_PREVIEW,
                AnomalyDetectorSettings.AD_MAX_ENTITIES_PER_QUERY,
                AnomalyDetectorSettings.MAX_ENTITIES_FOR_PREVIEW,
                AnomalyDetectorSettings.MAX_CONCURRENT_PREVIEW,
                AnomalyDetectorSettings.AD_PAGE_SIZE,
                AnomalyDetectorSettings.AD_SQS_QUEUE_ARN,
                AnomalyDetectorSettings.AD_SCHEDULER_GROUP,
                AnomalyDetectorSettings.AD_SCHEDULER_ROLE_ARN,
                // clean resource
                AnomalyDetectorSettings.DELETE_AD_RESULT_WHEN_DELETE_DETECTOR,
                // stats/profile API
                AnomalyDetectorSettings.AD_MAX_MODEL_SIZE_PER_NODE,
                // SQS settings
                AnomalyDetectorSettings.SQS_QUEUE_URL,
                AnomalyDetectorSettings.SQS_POLLING_INTERVAL,
                AnomalyDetectorSettings.SQS_MAX_MESSAGES,
                AnomalyDetectorSettings.SQS_VISIBILITY_TIMEOUT,
                AnomalyDetectorSettings.SQS_WAIT_TIME,
                AnomalyDetectorSettings.SQS_MAX_CONCURRENT_PROCESSORS,
                // s3
                AnomalyDetectorSettings.AD_S3_CHECKPOINT_BUCKET,
                // multi-tenancy
                AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED,
                // remote metadata
                AnomalyDetectorSettings.REMOTE_METADATA_ENDPOINT,
                // 'es' or 'aoss' for ddb client
                AnomalyDetectorSettings.REMOTE_METADATA_SERVICE_NAME,
                AnomalyDetectorSettings.CONFIG_DOCUMENT_STORE_FACTORY_CLASS,
                TimeSeriesSettings.LOCAL_METADATA_STORE_ENABLED,
                TimeSeriesSettings.INTERNAL_API_SHARED_SECRET,
                // ======================================
                // Forecast settings
                // ======================================
                // HC forecasting cache
                ForecastSettings.FORECAST_DEDICATED_CACHE_SIZE,
                // config parameters
                ForecastSettings.FORECAST_INTERVAL,
                ForecastSettings.FORECAST_WINDOW_DELAY,
                // Fault tolerance
                ForecastSettings.FORECAST_BACKOFF_MINUTES,
                ForecastSettings.FORECAST_BACKOFF_INITIAL_DELAY,
                ForecastSettings.FORECAST_MAX_RETRY_FOR_BACKOFF,
                // result index rollover
                ForecastSettings.FORECAST_RESULT_HISTORY_MAX_DOCS_PER_SHARD,
                ForecastSettings.FORECAST_RESULT_HISTORY_RETENTION_PERIOD,
                ForecastSettings.FORECAST_RESULT_HISTORY_ROLLOVER_PERIOD,
                // resource usage control
                ForecastSettings.FORECAST_MODEL_MAX_SIZE_PERCENTAGE,
                ForecastSettings.FORECAST_INDEX_PRESSURE_SOFT_LIMIT,
                ForecastSettings.FORECAST_INDEX_PRESSURE_HARD_LIMIT,
                ForecastSettings.FORECAST_MAX_PRIMARY_SHARDS,
                // restful apis
                ForecastSettings.FORECAST_REQUEST_TIMEOUT,
                // resource constraint
                // added validation code in AbstractTimeSeriesActionHandler.onSearchTotalConfigResponse
                ForecastSettings.MAX_SINGLE_STREAM_FORECASTERS,
                ForecastSettings.MAX_HC_FORECASTERS,
                // Security
                ForecastSettings.FORECAST_FILTER_BY_BACKEND_ROLES,
                ForecastSettings.FORECAST_MULTI_TENANCY_ENABLED,
                // Historical
                ForecastSettings.MAX_OLD_TASK_DOCS_PER_FORECASTER,
                // rate limiting
                ForecastSettings.FORECAST_CHECKPOINT_READ_QUEUE_CONCURRENCY,
                ForecastSettings.FORECAST_CHECKPOINT_WRITE_QUEUE_CONCURRENCY,
                ForecastSettings.FORECAST_COLD_START_QUEUE_CONCURRENCY,
                ForecastSettings.FORECAST_RESULT_WRITE_QUEUE_CONCURRENCY,
                ForecastSettings.FORECAST_CHECKPOINT_READ_QUEUE_BATCH_SIZE,
                ForecastSettings.FORECAST_CHECKPOINT_WRITE_QUEUE_BATCH_SIZE,
                ForecastSettings.FORECAST_RESULT_WRITE_QUEUE_BATCH_SIZE,
                ForecastSettings.FORECAST_COLD_ENTITY_QUEUE_MAX_HEAP_PERCENT,
                ForecastSettings.FORECAST_CHECKPOINT_READ_QUEUE_MAX_HEAP_PERCENT,
                ForecastSettings.FORECAST_CHECKPOINT_WRITE_QUEUE_MAX_HEAP_PERCENT,
                ForecastSettings.FORECAST_RESULT_WRITE_QUEUE_MAX_HEAP_PERCENT,
                ForecastSettings.FORECAST_CHECKPOINT_MAINTAIN_QUEUE_MAX_HEAP_PERCENT,
                ForecastSettings.FORECAST_COLD_START_QUEUE_MAX_HEAP_PERCENT,
                ForecastSettings.FORECAST_EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_MILLISECS,
                ForecastSettings.FORECAST_EXPECTED_CHECKPOINT_MAINTAIN_TIME_IN_MILLISECS,
                ForecastSettings.FORECAST_CHECKPOINT_SAVING_FREQ,
                ForecastSettings.FORECAST_CHECKPOINT_TTL,
                // query limit
                ForecastSettings.FORECAST_MAX_ENTITIES_PER_INTERVAL,
                ForecastSettings.FORECAST_PAGE_SIZE,
                // stats/profile API
                ForecastSettings.FORECAST_MAX_MODEL_SIZE_PER_NODE,
                // clean resource
                ForecastSettings.DELETE_FORECAST_RESULT_WHEN_DELETE_FORECASTER,
                // ======================================
                // Common settings
                // ======================================
                // Fault tolerance
                TimeSeriesSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE,
                TimeSeriesSettings.BACKOFF_MINUTES,
                TimeSeriesSettings.COOLDOWN_MINUTES,
                TimeSeriesSettings.MAX_CONCURRENT_SDK_INDEX_MAPPING_UPDATES,
                // tasks
                TimeSeriesSettings.MAX_CACHED_DELETED_TASKS,
                TimeSeriesSettings.OPENSEARCH_PORT,
                // node role
                TimeSeriesSettings.NODE_ROLE,
                // cluster membership reader ttl
                TimeSeriesSettings.CLUSTER_MEMBERSHIP_READER_TTL,
                // cloud map ttl
                TimeSeriesSettings.CLOUD_MAP_TTL,
                // cloud map settings
                TimeSeriesSettings.REGION,
                TimeSeriesSettings.CLOUD_MAP_NAMESPACE,
                TimeSeriesSettings.CLOUD_MAP_SERVICE,
                TimeSeriesSettings.CLOUD_MAP_TABLE_NAME,
                TimeSeriesSettings.STATIC_NODES
            );
        return unmodifiableList(
            Stream
                .of(
                    adEnabledSetting.stream(),
                    forecastEnabledSetting.stream(),
                    timeSeriesEnabledSetting.stream(),
                    systemSetting.stream(),
                    adNumericSetting.stream(),
                    forecastNumericSetting.stream()
                )
                .reduce(Stream::concat)
                .orElseGet(Stream::empty)
                .collect(Collectors.toList())
        );
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return ImmutableList
            .of(
                AnomalyDetector.XCONTENT_REGISTRY,
                AnomalyResult.XCONTENT_REGISTRY,
                DetectorInternalState.XCONTENT_REGISTRY,
                Job.XCONTENT_REGISTRY,
                Forecaster.XCONTENT_REGISTRY
            );
    }

    /*
     * Register action and handler so that transportClient can find proxy for action
     */
    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<String> nodeRoles = TimeSeriesSettings.NODE_ROLE.get(pluginSettings);
        boolean coordinatorNode = nodeRoles.contains(TimeSeriesSettings.COORDINATOR_ROLE);
        Class<? extends HandledTransportAction<DeleteConfigRequest, DeleteResponse>> deleteAnomalyDetectorTransportClass = coordinatorNode
            ? DeleteAnomalyDetectorMutliTenantTransportAction.class
            : DeleteAnomalyDetectorTransportAction.class;

        return Arrays
            .asList(
                // AD
                new ActionHandler<>(DeleteADModelAction.INSTANCE, DeleteADModelTransportAction.class),
                new ActionHandler<>(StopDetectorAction.INSTANCE, StopDetectorTransportAction.class),
                new ActionHandler<>(RCFResultAction.INSTANCE, RCFResultTransportAction.class),
                new ActionHandler<>(ThresholdResultAction.INSTANCE, ThresholdResultTransportAction.class),
                new ActionHandler<>(AnomalyResultAction.INSTANCE, AnomalyResultTransportAction.class),
                new ActionHandler<>(CronAction.INSTANCE, CronTransportAction.class),
                new ActionHandler<>(ADStatsNodesAction.INSTANCE, ADStatsNodesTransportAction.class),
                new ActionHandler<>(ADProfileAction.INSTANCE, ADProfileTransportAction.class),
                new ActionHandler<>(RCFPollingAction.INSTANCE, RCFPollingTransportAction.class),
                new ActionHandler<>(SearchAnomalyDetectorAction.INSTANCE, SearchAnomalyDetectorTransportAction.class),
                new ActionHandler<>(SearchAnomalyResultAction.INSTANCE, SearchAnomalyResultTransportAction.class),
                new ActionHandler<>(SearchADTasksAction.INSTANCE, SearchADTasksTransportAction.class),
                new ActionHandler<>(StatsAnomalyDetectorAction.INSTANCE, StatsAnomalyDetectorTransportAction.class),
                new ActionHandler<>(DeleteAnomalyDetectorAction.INSTANCE, deleteAnomalyDetectorTransportClass),
                new ActionHandler<>(GetAnomalyDetectorAction.INSTANCE, GetAnomalyDetectorTransportAction.class),
                new ActionHandler<>(IndexAnomalyDetectorAction.INSTANCE, IndexAnomalyDetectorTransportAction.class),
                new ActionHandler<>(AnomalyDetectorJobAction.INSTANCE, AnomalyDetectorJobTransportAction.class),
                new ActionHandler<>(ADResultBulkAction.INSTANCE, ADResultBulkTransportAction.class),
                new ActionHandler<>(EntityADResultAction.INSTANCE, EntityADResultTransportAction.class),
                new ActionHandler<>(ADEntityProfileAction.INSTANCE, ADEntityProfileTransportAction.class),
                new ActionHandler<>(SearchAnomalyDetectorInfoAction.INSTANCE, SearchAnomalyDetectorInfoTransportAction.class),
                new ActionHandler<>(PreviewAnomalyDetectorAction.INSTANCE, PreviewAnomalyDetectorTransportAction.class),
                new ActionHandler<>(ADBatchAnomalyResultAction.INSTANCE, ADBatchAnomalyResultTransportAction.class),
                new ActionHandler<>(ADBatchTaskRemoteExecutionAction.INSTANCE, ADBatchTaskRemoteExecutionTransportAction.class),
                new ActionHandler<>(ADTaskProfileAction.INSTANCE, ADTaskProfileTransportAction.class),
                new ActionHandler<>(ADCancelTaskAction.INSTANCE, ADCancelTaskTransportAction.class),
                new ActionHandler<>(ForwardADTaskAction.INSTANCE, ForwardADTaskTransportAction.class),
                new ActionHandler<>(DeleteAnomalyResultsAction.INSTANCE, DeleteAnomalyResultsTransportAction.class),
                new ActionHandler<>(SearchTopAnomalyResultAction.INSTANCE, SearchTopAnomalyResultTransportAction.class),
                new ActionHandler<>(ValidateAnomalyDetectorAction.INSTANCE, ValidateAnomalyDetectorTransportAction.class),
                new ActionHandler<>(ADSingleStreamResultAction.INSTANCE, ADSingleStreamResultTransportAction.class),
                new ActionHandler<>(ADHCImputeAction.INSTANCE, ADHCImputeTransportAction.class),
                new ActionHandler<>(SuggestAnomalyDetectorParamAction.INSTANCE, SuggestAnomalyDetectorParamTransportAction.class),
                // forecast
                new ActionHandler<>(IndexForecasterAction.INSTANCE, IndexForecasterTransportAction.class),
                new ActionHandler<>(ForecastResultAction.INSTANCE, ForecastResultTransportAction.class),
                new ActionHandler<>(EntityForecastResultAction.INSTANCE, EntityForecastResultTransportAction.class),
                new ActionHandler<>(ForecastResultBulkAction.INSTANCE, ForecastResultBulkTransportAction.class),
                new ActionHandler<>(ForecastSingleStreamResultAction.INSTANCE, ForecastSingleStreamResultTransportAction.class),
                new ActionHandler<>(ForecasterJobAction.INSTANCE, ForecasterJobTransportAction.class),
                new ActionHandler<>(StopForecasterAction.INSTANCE, StopForecasterTransportAction.class),
                new ActionHandler<>(DeleteForecastModelAction.INSTANCE, DeleteForecastModelTransportAction.class),
                new ActionHandler<>(GetForecasterAction.INSTANCE, GetForecasterTransportAction.class),
                new ActionHandler<>(DeleteForecasterAction.INSTANCE, DeleteForecasterTransportAction.class),
                new ActionHandler<>(SearchForecasterAction.INSTANCE, SearchForecasterTransportAction.class),
                new ActionHandler<>(SearchForecasterInfoAction.INSTANCE, SearchForecasterInfoTransportAction.class),
                new ActionHandler<>(SearchTopForecastResultAction.INSTANCE, SearchTopForecastResultTransportAction.class),
                new ActionHandler<>(ForecastEntityProfileAction.INSTANCE, ForecastEntityProfileTransportAction.class),
                new ActionHandler<>(ForecastProfileAction.INSTANCE, ForecastProfileTransportAction.class),
                new ActionHandler<>(SearchForecastTasksAction.INSTANCE, SearchForecastTasksTransportAction.class),
                new ActionHandler<>(StatsForecasterAction.INSTANCE, StatsForecasterTransportAction.class),
                new ActionHandler<>(ForecastStatsNodesAction.INSTANCE, ForecastStatsNodesTransportAction.class),
                new ActionHandler<>(ForecastRunOnceAction.INSTANCE, ForecastRunOnceTransportAction.class),
                new ActionHandler<>(ForecastRunOnceProfileAction.INSTANCE, ForecastRunOnceProfileTransportAction.class),
                new ActionHandler<>(ValidateForecasterAction.INSTANCE, ValidateForecasterTransportAction.class),
                new ActionHandler<>(SuggestForecasterParamAction.INSTANCE, SuggestForecasterParamTransportAction.class)
            );
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        List<SystemIndexDescriptor> systemIndexDescriptors = new ArrayList<>();
        systemIndexDescriptors.add(new SystemIndexDescriptor(ADCommonName.CONFIG_INDEX, "Anomaly detection config index"));
        systemIndexDescriptors.add(new SystemIndexDescriptor(ForecastCommonName.CONFIG_INDEX, "Forecasting config index"));
        systemIndexDescriptors.add(new SystemIndexDescriptor(ADCommonName.ALL_AD_RESULTS_INDEX_PATTERN, "AD result index pattern"));
        systemIndexDescriptors.add(new SystemIndexDescriptor(CHECKPOINT_INDEX_NAME, "AD Checkpoints index"));
        systemIndexDescriptors.add(new SystemIndexDescriptor(DETECTION_STATE_INDEX, "AD State index"));
        systemIndexDescriptors.add(new SystemIndexDescriptor(FORECAST_CHECKPOINT_INDEX_NAME, "Forecast Checkpoints index"));
        systemIndexDescriptors.add(new SystemIndexDescriptor(FORECAST_STATE_INDEX, "Forecast state index"));
        systemIndexDescriptors.add(new SystemIndexDescriptor(JOB_INDEX, "Time Series Analytics job index"));
        return systemIndexDescriptors;
    }

    @Override
    public String getJobType() {
        return TIME_SERIES_JOB_TYPE;
    }

    @Override
    public String getJobIndex() {
        return CommonName.JOB_INDEX;
    }

    @Override
    public ScheduledJobRunner getJobRunner() {
        return JobRunner.getJobRunnerInstance();
    }

    @Override
    public ScheduledJobParser getJobParser() {
        return (parser, id, jobDocVersion) -> {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            return Job.parse(parser);
        };
    }

    private DataAccess createDataAccess(
        boolean multiTenancyEnabled,
        NamedXContentRegistry xContentRegistry,
        SdkStateManager resourceStateManager,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        SdkClient sdkClient = null;
        if (multiTenancyEnabled) {
            try {
                sdkClient = SdkClientProvider
                    .buildSdkClient(
                        client,
                        xContentRegistry,
                        ADCommonName.AD_THREAD_POOL_NAME,
                        multiTenancyEnabled,
                        pluginSettings,
                        AnomalyDetectorSettings.REMOTE_METADATA_ENDPOINT,
                        AnomalyDetectorSettings.REMOTE_METADATA_SERVICE_NAME,
                        LOG
                    );
            } catch (Exception e) {
                LOG.warn("Failed to initialize task searcher sdk client; falling back to transport client.", e);
            }
        }

        ConfigDocumentStore configDocumentStore = sdkClient == null ? null : createConfigDocumentStore(sdkClient);
        return createDataAccess(multiTenancyEnabled, sdkClient, configDocumentStore, resourceStateManager, indexNameExpressionResolver);
    }

    private ConfigDocumentStore createConfigDocumentStore(SdkClient sdkClient) {
        Settings settings = pluginSettings == null ? Settings.EMPTY : pluginSettings;
        String factoryClassName = AnomalyDetectorSettings.CONFIG_DOCUMENT_STORE_FACTORY_CLASS.get(settings);
        if (factoryClassName == null || factoryClassName.isEmpty()) {
            return new RemoteMetadataConfigDocumentStoreFactory().create(sdkClient, settings, clusterService);
        }

        try {
            Class<?> factoryClass = Class.forName(factoryClassName, true, getClass().getClassLoader());
            if (!ConfigDocumentStoreFactory.class.isAssignableFrom(factoryClass)) {
                throw new IllegalStateException(factoryClassName + " must implement ConfigDocumentStoreFactory");
            }
            ConfigDocumentStoreFactory factory = (ConfigDocumentStoreFactory) factoryClass.getDeclaredConstructor().newInstance();
            return factory.create(sdkClient, settings, clusterService);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Failed to load config document store factory: " + factoryClassName, e);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to instantiate config document store factory: " + factoryClassName, e);
        }
    }

    private TenantEndpointResolver createTenantEndpointResolver() {
        return ServiceLoader.load(TenantEndpointResolver.class).findFirst().orElseGet(DefaultTenantEndpointResolver::new);
    }

    private DataAccess createDataAccess(
        boolean multiTenancyEnabled,
        SdkClient sdkClient,
        ConfigDocumentStore configDocumentStore,
        SdkStateManager resourceStateManager,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        if (multiTenancyEnabled && sdkClient != null) {
            return new SdkDataAccess(
                sdkClient,
                clusterService,
                pluginSettings,
                resourceStateManager,
                configDocumentStore,
                createTenantEndpointResolver()
            );
        }
        return new DefaultDataAccess(client, clusterService, securityClientUtil, indexNameExpressionResolver);
    }

    @Override
    public void close() {
        if (serializeRCFBufferPool != null) {
            try {
                AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                    serializeRCFBufferPool.clear();
                    serializeRCFBufferPool.close();
                    return null;
                });
                serializeRCFBufferPool = null;
            } catch (Exception e) {
                LOG.error("Failed to shut down object Pool", e);
            }
        }
    }

    @Override
    public void assignSubject(PluginSubject pluginSubject) {
        if (this.pluginClient != null) {
            this.pluginClient.setSubject(pluginSubject);
        }
    }
}
