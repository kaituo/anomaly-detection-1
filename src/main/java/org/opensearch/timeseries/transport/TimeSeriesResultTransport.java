package org.opensearch.timeseries.transport;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionResponse;
import org.opensearch.timeseries.common.exception.ClientException;
import org.opensearch.timeseries.common.exception.InternalFailure;
import org.opensearch.timeseries.common.exception.TimeSeriesException;

public class TimeSeriesResultTransport {
    public static final String NODE_UNRESPONSIVE_ERR_MSG = "Model node is unresponsive.  Mute node";
    public static final String READ_WRITE_BLOCKED = "Cannot read/write due to global block.";
    public static final String INDEX_READ_BLOCKED = "Cannot read user index due to read block.";
    public static final String NULL_RESPONSE = "Received null response from";
    public static final String TROUBLE_QUERYING_ERR_MSG = "Having trouble querying data: ";
    static final String NO_ACK_ERR = "no acknowledgements from model hosting nodes.";
    static final String WAIT_FOR_THRESHOLD_ERR_MSG = "Exception in waiting for threshold result";

    public static void handleExecuteException(Exception ex, ActionListener<? extends ActionResponse> listener, String id) {
        if (ex instanceof ClientException) {
            listener.onFailure(ex);
        } else if (ex instanceof TimeSeriesException) {
            listener.onFailure(new InternalFailure((TimeSeriesException) ex));
        } else {
            Throwable cause = ExceptionsHelper.unwrapCause(ex);
            listener.onFailure(new InternalFailure(id, cause));
        }
    }
}
