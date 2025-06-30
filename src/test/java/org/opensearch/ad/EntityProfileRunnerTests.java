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

package org.opensearch.ad;

import static java.util.Collections.emptyMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.search.TotalHits;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.NodeCommunicator;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.EntityProfile;
import org.opensearch.timeseries.model.EntityProfileName;
import org.opensearch.timeseries.model.EntityState;
import org.opensearch.timeseries.model.InitProgressProfile;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.model.ModelProfile;
import org.opensearch.timeseries.model.ModelProfileOnNode;
import org.opensearch.timeseries.transport.EntityProfileResponse;

public class EntityProfileRunnerTests extends AbstractTimeSeriesTest {
    private AnomalyDetector detector;
    private int detectorIntervalMin;
    private DataAccess dataAccess;
    private NodeCommunicator nodeCommunicator;
    private StateManager stateManager;
    private ADEntityProfileRunner runner;
    private Set<EntityProfileName> state;
    private Set<EntityProfileName> initNInfo;
    private Set<EntityProfileName> model;
    private String detectorId;
    private String entityValue;
    private int requiredSamples;
    private Job job;

    private int smallUpdates;
    private String categoryField;
    private long latestSampleTimestamp;
    private long latestActiveTimestamp;
    private Boolean isActive;
    private String modelId;
    private long modelSize;
    private String nodeId;
    private Entity entity;

    enum InittedEverResultStatus {
        UNKNOWN,
        INITTED,
        NOT_INITTED,
    }

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(AnomalyDetectorJobRunnerTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
    }

    @SuppressWarnings("unchecked")
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        detectorIntervalMin = 3;

        state = new HashSet<EntityProfileName>();
        state.add(EntityProfileName.STATE);

        initNInfo = new HashSet<EntityProfileName>();
        initNInfo.add(EntityProfileName.INIT_PROGRESS);
        initNInfo.add(EntityProfileName.ENTITY_INFO);

        model = new HashSet<EntityProfileName>();
        model.add(EntityProfileName.MODELS);

        detectorId = "A69pa3UBHuCbh-emo9oR";
        entityValue = "app-0";

        categoryField = "a";
        detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(categoryField));
        job = TestHelpers.randomJob(true);

        requiredSamples = 128;
        stateManager = mock(StateManager.class);
        doAnswer(invocation -> {
            ActionListener<Optional<? extends Config>> listener = invocation.getArgument(4);
            listener.onResponse(Optional.of(detector));
            return null;
        }).when(stateManager).getConfig(any(String.class), any(), eq(AnalysisType.AD), any(boolean.class), any(ActionListener.class));
        doAnswer(invocation -> {
            ActionListener<Optional<Job>> listener = invocation.getArgument(3);
            listener.onResponse(Optional.of(job));
            return null;
        }).when(stateManager).getJob(any(String.class), any(), any(boolean.class), any(ActionListener.class));

        dataAccess = mock(DataAccess.class);
        nodeCommunicator = mock(NodeCommunicator.class);
        runner = new ADEntityProfileRunner(nodeCommunicator, dataAccess, stateManager, requiredSamples);

        entity = Entity.createSingleAttributeEntity(categoryField, entityValue);
        modelId = entity.getModelId(null, detectorId).get();
    }

    private void profile(
        String detectorId,
        Entity entity,
        Set<EntityProfileName> profilesToCollect,
        ActionListener<EntityProfile> listener
    ) {
        runner.profile(detectorId, null, entity, profilesToCollect, listener);
    }

    private SearchResponse createLastSampleTimeSearchResponse() {
        InternalMax maxAgg = new InternalMax(CommonName.AGG_NAME_MAX_TIME, latestSampleTimestamp, DocValueFormat.RAW, emptyMap());
        InternalAggregations internalAggregations = InternalAggregations.from(Collections.singletonList(maxAgg));

        SearchHits hits = new SearchHits(new SearchHit[] {}, null, Float.NaN);
        SearchResponseSections searchSections = new SearchResponseSections(hits, internalAggregations, null, false, false, null, 1);

        return new SearchResponse(searchSections, null, 1, 1, 0, 30, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }

    private SearchResponse createEntityExistsSearchResponse() {
        SearchHits collapsedHits = new SearchHits(
            new SearchHit[] {
                new SearchHit(2, "ID", Collections.emptyMap(), Collections.emptyMap()),
                new SearchHit(3, "ID", Collections.emptyMap(), Collections.emptyMap()) },
            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
            1.0F
        );

        InternalSearchResponse internalSearchResponse = new InternalSearchResponse(collapsedHits, null, null, null, false, null, 1);
        return new SearchResponse(internalSearchResponse, null, 1, 1, 0, 0, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }

    @SuppressWarnings("unchecked")
    private void setUpSearch() {
        latestSampleTimestamp = 1_603_989_830_158L;
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(2);
            listener.onResponse(createLastSampleTimeSearchResponse());
            return null;
        }).when(dataAccess).search(any(), any(), any());
    }

    @SuppressWarnings("unchecked")
    private void setUpExecuteEntityProfileAction(InittedEverResultStatus initted) {
        smallUpdates = 1;
        latestActiveTimestamp = 1603999189758L;
        isActive = Boolean.TRUE;
        modelId = "T4c3dXUBj-2IZN7itix__entity_" + entityValue;
        modelSize = 712480L;
        nodeId = "g6pmr547QR-CfpEvO67M4g";
        doAnswer(invocation -> {
            ActionListener<EntityProfileResponse> listener = invocation.getArgument(1);

            EntityProfileResponse.Builder profileResponseBuilder = new EntityProfileResponse.Builder();
            if (InittedEverResultStatus.UNKNOWN == initted) {
                profileResponseBuilder.setTotalUpdates(0L);
            } else if (InittedEverResultStatus.NOT_INITTED == initted) {
                profileResponseBuilder.setTotalUpdates(smallUpdates);
                profileResponseBuilder.setLastActiveMs(latestActiveTimestamp);
                profileResponseBuilder.setActive(isActive);
            } else {
                profileResponseBuilder.setTotalUpdates(requiredSamples + 1);
                ModelProfileOnNode model = new ModelProfileOnNode(nodeId, new ModelProfile(modelId, entity, modelSize));
                profileResponseBuilder.setModelProfile(model);
            }

            listener.onResponse(profileResponseBuilder.build());
            return null;
        }).when(nodeCommunicator).entityProfile(any(), any());

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(4);
            listener.onResponse(createEntityExistsSearchResponse());
            return null;
        }).when(dataAccess).searchWithInjectedSecurity(any(), any(String.class), any(), eq(AnalysisType.AD), any(ActionListener.class));

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(2);
            listener.onResponse(createLastSampleTimeSearchResponse());
            return null;
        }).when(dataAccess).search(any(), any(), any());
    }

    public void stateTestTemplate(InittedEverResultStatus returnedState, EntityState expectedState) throws InterruptedException {
        setUpExecuteEntityProfileAction(returnedState);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        profile(detectorId, entity, state, ActionListener.wrap(response -> {
            assertEquals(expectedState, response.getState());
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testRunningState() throws InterruptedException {
        stateTestTemplate(InittedEverResultStatus.INITTED, EntityState.RUNNING);
    }

    public void testUnknownState() throws InterruptedException {
        stateTestTemplate(InittedEverResultStatus.UNKNOWN, EntityState.UNKNOWN);
    }

    public void testInitState() throws InterruptedException {
        stateTestTemplate(InittedEverResultStatus.NOT_INITTED, EntityState.INIT);
    }

    public void testEmptyProfile() throws InterruptedException {
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        profile(detectorId, entity, new HashSet<>(), ActionListener.wrap(response -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue(exception.getMessage().contains(CommonMessages.EMPTY_PROFILES_COLLECT));
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testModel() throws InterruptedException {
        setUpExecuteEntityProfileAction(InittedEverResultStatus.INITTED);
        EntityProfile.Builder expectedProfile = new EntityProfile.Builder();

        ModelProfileOnNode modelProfile = new ModelProfileOnNode(nodeId, new ModelProfile(modelId, entity, modelSize));
        expectedProfile.modelProfile(modelProfile);
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        profile(detectorId, entity, model, ActionListener.wrap(response -> {
            assertEquals(expectedProfile.build(), response);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testEmptyModelProfile() throws IOException {
        ModelProfile modelProfile = new ModelProfile(modelId, null, modelSize);
        BytesStreamOutput output = new BytesStreamOutput();
        modelProfile.writeTo(output);
        StreamInput streamInput = output.bytes().streamInput();
        ModelProfile readResponse = new ModelProfile(streamInput);
        assertEquals("serialization has the wrong model id", modelId, readResponse.getModelId());
        assertTrue("serialization has null entity", null == readResponse.getEntity());
        assertEquals("serialization has the wrong model size", modelSize, readResponse.getModelSizeInBytes());

    }

    @SuppressWarnings("unchecked")
    public void testJobIndexNotFound() throws InterruptedException {
        setUpExecuteEntityProfileAction(InittedEverResultStatus.INITTED);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        doAnswer(invocation -> {
            ActionListener<Optional<Job>> listener = invocation.getArgument(3);
            listener.onFailure(new IndexNotFoundException(CommonName.JOB_INDEX));
            return null;
        }).when(stateManager).getJob(any(String.class), any(), any(boolean.class), any(ActionListener.class));

        EntityProfile expectedProfile = new EntityProfile.Builder().build();

        profile(detectorId, entity, initNInfo, ActionListener.wrap(response -> {
            assertEquals(expectedProfile, response);
            inProgressLatch.countDown();
        }, exception -> {
            LOG.error("Unexpected error", exception);
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    @SuppressWarnings("unchecked")
    public void testNotMultiEntityDetector() throws IOException, InterruptedException {
        detector = TestHelpers.randomAnomalyDetectorWithInterval(new IntervalTimeConfiguration(detectorIntervalMin, ChronoUnit.MINUTES));

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        profile(detectorId, entity, state, ActionListener.wrap(response -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue(exception.getMessage().contains(ADEntityProfileRunner.NOT_HC_DETECTOR_ERR_MSG));
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testInitNInfo() throws InterruptedException {
        setUpExecuteEntityProfileAction(InittedEverResultStatus.NOT_INITTED);
        latestSampleTimestamp = 1_603_989_830_158L;

        EntityProfile.Builder expectedProfile = new EntityProfile.Builder();

        // 1 / 128 rounded to 1%
        int neededSamples = requiredSamples - smallUpdates;
        InitProgressProfile profile = new InitProgressProfile("1%", neededSamples * detector.getIntervalInSeconds() / 60, neededSamples);
        expectedProfile.initProgress(profile);
        expectedProfile.isActive(isActive);
        expectedProfile.lastActiveTimestampMs(latestActiveTimestamp);
        expectedProfile.lastSampleTimestampMs(latestSampleTimestamp);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        profile(detectorId, entity, initNInfo, ActionListener.wrap(response -> {
            assertEquals(expectedProfile.build(), response);
            inProgressLatch.countDown();
        }, exception -> {
            LOG.error("Unexpected error", exception);
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }
}
