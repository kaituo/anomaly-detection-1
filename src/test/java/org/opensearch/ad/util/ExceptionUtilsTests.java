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

package org.opensearch.ad.util;

import java.util.concurrent.RejectedExecutionException;

import org.opensearch.OpenSearchException;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.common.exception.LimitExceededException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.OverloadExceptionMatcher;

public class ExceptionUtilsTests extends OpenSearchTestCase {

    @Override
    public void tearDown() throws Exception {
        ExceptionUtil.configureOverloadExceptionMatcher(Settings.EMPTY, getClass().getClassLoader());
        super.tearDown();
    }

    public void testGetShardsFailure() {
        ShardId shardId = new ShardId(randomAlphaOfLength(5), randomAlphaOfLength(5), 1);
        ReplicationResponse.ShardInfo.Failure failure = new ReplicationResponse.ShardInfo.Failure(
            shardId,
            randomAlphaOfLength(5),
            new RuntimeException("test"),
            RestStatus.BAD_REQUEST,
            false
        );
        ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(2, 1, failure);
        IndexResponse indexResponse = new IndexResponse(shardId, "id", randomLong(), randomLong(), randomLong(), randomBoolean());
        indexResponse.setShardInfo(shardInfo);
        String shardsFailure = ExceptionUtil.getShardsFailure(indexResponse);
        assertEquals("RuntimeException[test]", shardsFailure);
    }

    public void testGetShardsFailureWithoutError() {
        ShardId shardId = new ShardId(randomAlphaOfLength(5), randomAlphaOfLength(5), 1);
        IndexResponse indexResponse = new IndexResponse(shardId, "id", randomLong(), randomLong(), randomLong(), randomBoolean());
        assertNull(ExceptionUtil.getShardsFailure(indexResponse));

        ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(2, 1, ReplicationResponse.EMPTY);
        indexResponse.setShardInfo(shardInfo);
        assertNull(ExceptionUtil.getShardsFailure(indexResponse));
    }

    public void testCountInStats() {
        assertTrue(ExceptionUtil.countInStats(new TimeSeriesException("test")));
        assertFalse(ExceptionUtil.countInStats(new TimeSeriesException("test").countedInStats(false)));
        assertTrue(ExceptionUtil.countInStats(new RuntimeException("test")));
    }

    public void testGetErrorMessage() {
        assertEquals("test", ExceptionUtil.getErrorMessage(new TimeSeriesException("test")));
        assertEquals("test", ExceptionUtil.getErrorMessage(new IllegalArgumentException("test")));
        assertEquals("OpenSearchException[test]", ExceptionUtil.getErrorMessage(new OpenSearchException("test")));
        assertTrue(
            ExceptionUtil
                .getErrorMessage(new RuntimeException("test"))
                .contains("at org.opensearch.ad.util.ExceptionUtilsTests.testGetErrorMessage")
        );
    }

    public void testIsOverloaded() {
        assertTrue(ExceptionUtil.isOverloaded(new RuntimeException("wrapper", new RejectedExecutionException("rejected"))));
        assertTrue(ExceptionUtil.isOverloaded(new OpenSearchRejectedExecutionException("rejected")));
        assertTrue(ExceptionUtil.isOverloaded(new LimitExceededException("limit exceeded")));
        assertFalse(ExceptionUtil.isOverloaded(new RuntimeException("some other failure")));
    }

    public void testIsOverloadedUsesConfiguredMatcher() {
        Settings settings = Settings
            .builder()
            .put(TimeSeriesSettings.OVERLOAD_EXCEPTION_MATCHER_CLASS.getKey(), TestOverloadExceptionMatcher.class.getName())
            .build();

        ExceptionUtil.configureOverloadExceptionMatcher(settings, getClass().getClassLoader());

        assertTrue(ExceptionUtil.isOverloaded(new RuntimeException("wrapper", new TestOverloadException())));
    }

    public void testConfigureOverloadExceptionMatcherFailsForUnknownClass() {
        String className = "org.opensearch.timeseries.util.DoesNotExistOverloadExceptionMatcher";
        Settings settings = Settings.builder().put(TimeSeriesSettings.OVERLOAD_EXCEPTION_MATCHER_CLASS.getKey(), className).build();

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> ExceptionUtil.configureOverloadExceptionMatcher(settings, getClass().getClassLoader())
        );

        assertTrue(exception.getMessage().contains("Failed to load overload exception matcher"));
        assertTrue(exception.getMessage().contains(className));
        assertEquals(ClassNotFoundException.class, exception.getCause().getClass());
    }

    public void testIsSearchPhaseExecutionException() {
        assertTrue(
            ExceptionUtil
                .isSearchPhaseExecutionException(
                    new SearchPhaseExecutionException("query", "all shards failed", ShardSearchFailure.EMPTY_ARRAY)
                )
        );
        assertTrue(
            ExceptionUtil
                .isSearchPhaseExecutionException(
                    new RuntimeException(
                        "wrapper",
                        new RuntimeException("Request failed: [search_phase_execution_exception] all shards failed")
                    )
                )
        );
        assertTrue(ExceptionUtil.isSearchPhaseExecutionException(new RuntimeException("Request failed: all shards failed")));
        assertFalse(ExceptionUtil.isSearchPhaseExecutionException(new RuntimeException("some other failure")));
        assertFalse(ExceptionUtil.isSearchPhaseExecutionException(null));
    }

    public static class TestOverloadExceptionMatcher implements OverloadExceptionMatcher {
        @Override
        public boolean isOverloaded(Throwable exception) {
            return exception.getCause() instanceof TestOverloadException;
        }
    }

    private static class TestOverloadException extends RuntimeException {}
}
