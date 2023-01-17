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

package org.opensearch.timeseries.util;

import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.ADNodeStateManager;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.common.Strings;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.model.Config;

public class TimeSeriesSafeSecurityInjector extends SafeSecurityInjector {
    private static final Logger LOG = LogManager.getLogger(TimeSeriesSafeSecurityInjector.class);
    private ADNodeStateManager nodeStateManager;

    public TimeSeriesSafeSecurityInjector(String configId, Settings settings, ThreadContext tc, ADNodeStateManager stateManager) {
        super(configId, settings, tc);
        this.nodeStateManager = stateManager;
    }

    public void injectUserRolesFromDetector(ActionListener<Void> injectListener) {
        // if id is null, we cannot fetch a detector
        if (Strings.isEmpty(id)) {
            LOG.debug("Empty id");
            injectListener.onResponse(null);
            return;
        }

        // for example, if a user exists in thread context, we don't need to inject user/roles
        if (!shouldInject()) {
            LOG.debug("Don't need to inject");
            injectListener.onResponse(null);
            return;
        }

        ActionListener<Optional<? extends Config>> getDetectorListener = ActionListener.wrap(detectorOp -> {
            if (!detectorOp.isPresent()) {
                injectListener.onFailure(new EndRunException(id, "AnomalyDetector is not available.", false));
                return;
            }
            Config detector = detectorOp.get();
            User userInfo = SecurityUtil.getUserFromConfig(detector, settings);
            inject(userInfo.getName(), userInfo.getRoles());
            injectListener.onResponse(null);
        }, injectListener::onFailure);

        // Since we are gonna read user from detector, make sure the anomaly detector exists and fetched from disk or cached memory
        // We don't accept a passed-in AnomalyDetector because the caller might mistakenly not insert any user info in the
        // constructed AnomalyDetector and thus poses risks. In the case, if the user is null, we will give admin role.
        nodeStateManager.getConfig(id, getDetectorListener);
    }

    public void injectUserRoles(User user) {
        if (user == null) {
            LOG.debug("null user");
            return;
        }

        if (shouldInject()) {
            inject(user.getName(), user.getRoles());
        }
    }
}
