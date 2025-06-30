/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.cloudwatch.HeapUsageMetricPublisher;
import org.opensearch.timeseries.cloudwatch.SqsMetricPublisher;
import org.opensearch.timeseries.rest.handler.EventBridgeHandler;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.timeseries.util.SecurityUtil;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.core.document.Document;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.applicationautoscaling.ApplicationAutoScalingClient;
import software.amazon.awssdk.services.applicationautoscaling.model.CustomizedMetricSpecification;
import software.amazon.awssdk.services.applicationautoscaling.model.DeleteScalingPolicyRequest;
import software.amazon.awssdk.services.applicationautoscaling.model.DeregisterScalableTargetRequest;
import software.amazon.awssdk.services.applicationautoscaling.model.DescribeScalableTargetsRequest;
import software.amazon.awssdk.services.applicationautoscaling.model.DescribeScalingPoliciesRequest;
import software.amazon.awssdk.services.applicationautoscaling.model.MetricDimension;
import software.amazon.awssdk.services.applicationautoscaling.model.MetricStatistic;
import software.amazon.awssdk.services.applicationautoscaling.model.PolicyType;
import software.amazon.awssdk.services.applicationautoscaling.model.PredictiveScalingCustomizedMetricSpecification;
import software.amazon.awssdk.services.applicationautoscaling.model.PredictiveScalingMetric;
import software.amazon.awssdk.services.applicationautoscaling.model.PredictiveScalingMetricDataQuery;
import software.amazon.awssdk.services.applicationautoscaling.model.PredictiveScalingMetricDimension;
import software.amazon.awssdk.services.applicationautoscaling.model.PredictiveScalingMetricSpecification;
import software.amazon.awssdk.services.applicationautoscaling.model.PredictiveScalingMetricStat;
import software.amazon.awssdk.services.applicationautoscaling.model.PredictiveScalingMode;
import software.amazon.awssdk.services.applicationautoscaling.model.PredictiveScalingPolicyConfiguration;
import software.amazon.awssdk.services.applicationautoscaling.model.PutScalingPolicyRequest;
import software.amazon.awssdk.services.applicationautoscaling.model.RegisterScalableTargetRequest;
import software.amazon.awssdk.services.applicationautoscaling.model.ScalableDimension;
import software.amazon.awssdk.services.applicationautoscaling.model.ServiceNamespace;
import software.amazon.awssdk.services.applicationautoscaling.model.TargetTrackingScalingPolicyConfiguration;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DeleteLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.FilterLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.FilteredLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceAlreadyExistsException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import software.amazon.awssdk.services.ec2.model.CreateSecurityGroupRequest;
import software.amazon.awssdk.services.ec2.model.DeleteSecurityGroupRequest;
import software.amazon.awssdk.services.ec2.model.DescribeNetworkInterfacesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeSubnetsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeVpcsRequest;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.IpPermission;
import software.amazon.awssdk.services.ec2.model.IpRange;
import software.amazon.awssdk.services.ec2.model.UserIdGroupPair;
import software.amazon.awssdk.services.ecs.EcsClient;
import software.amazon.awssdk.services.ecs.model.AssignPublicIp;
import software.amazon.awssdk.services.ecs.model.AwsVpcConfiguration;
import software.amazon.awssdk.services.ecs.model.Compatibility;
import software.amazon.awssdk.services.ecs.model.ContainerDefinition;
import software.amazon.awssdk.services.ecs.model.ContainerOverride;
import software.amazon.awssdk.services.ecs.model.DeregisterTaskDefinitionRequest;
import software.amazon.awssdk.services.ecs.model.DescribeServicesRequest;
import software.amazon.awssdk.services.ecs.model.DescribeTaskDefinitionRequest;
import software.amazon.awssdk.services.ecs.model.DescribeTasksRequest;
import software.amazon.awssdk.services.ecs.model.DesiredStatus;
import software.amazon.awssdk.services.ecs.model.KeyValuePair;
import software.amazon.awssdk.services.ecs.model.LaunchType;
import software.amazon.awssdk.services.ecs.model.ListClustersRequest;
import software.amazon.awssdk.services.ecs.model.ListTaskDefinitionsRequest;
import software.amazon.awssdk.services.ecs.model.ListTasksRequest;
import software.amazon.awssdk.services.ecs.model.LogConfiguration;
import software.amazon.awssdk.services.ecs.model.LogDriver;
import software.amazon.awssdk.services.ecs.model.NetworkConfiguration;
import software.amazon.awssdk.services.ecs.model.NetworkMode;
import software.amazon.awssdk.services.ecs.model.PortMapping;
import software.amazon.awssdk.services.ecs.model.RegisterTaskDefinitionRequest;
import software.amazon.awssdk.services.ecs.model.RunTaskRequest;
import software.amazon.awssdk.services.ecs.model.RunTaskResponse;
import software.amazon.awssdk.services.ecs.model.SortOrder;
import software.amazon.awssdk.services.ecs.model.StopTaskRequest;
import software.amazon.awssdk.services.ecs.model.Task;
import software.amazon.awssdk.services.ecs.model.TaskDefinition;
import software.amazon.awssdk.services.ecs.model.TaskDefinitionStatus;
import software.amazon.awssdk.services.ecs.model.TaskOverride;
import software.amazon.awssdk.services.ecs.model.TransportProtocol;
import software.amazon.awssdk.services.ecs.model.UpdateServiceRequest;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.CreateRoleRequest;
import software.amazon.awssdk.services.iam.model.DeleteRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.DeleteRoleRequest;
import software.amazon.awssdk.services.iam.model.GetRoleRequest;
import software.amazon.awssdk.services.iam.model.NoSuchEntityException;
import software.amazon.awssdk.services.iam.model.PutRolePolicyRequest;
import software.amazon.awssdk.services.opensearchserverless.OpenSearchServerlessClient;
import software.amazon.awssdk.services.opensearchserverless.model.AccessPolicySummary;
import software.amazon.awssdk.services.opensearchserverless.model.AccessPolicyType;
import software.amazon.awssdk.services.opensearchserverless.model.CollectionSummary;
import software.amazon.awssdk.services.opensearchserverless.model.GetAccessPolicyRequest;
import software.amazon.awssdk.services.opensearchserverless.model.ListAccessPoliciesRequest;
import software.amazon.awssdk.services.opensearchserverless.model.ListCollectionsRequest;
import software.amazon.awssdk.services.opensearchserverless.model.UpdateAccessPolicyRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.scheduler.SchedulerClient;
import software.amazon.awssdk.services.scheduler.model.DeleteScheduleRequest;
import software.amazon.awssdk.services.scheduler.model.ListSchedulesRequest;
import software.amazon.awssdk.services.scheduler.model.ListSchedulesResponse;
import software.amazon.awssdk.services.scheduler.model.ResourceNotFoundException;
import software.amazon.awssdk.services.scheduler.model.ScheduleSummary;
import software.amazon.awssdk.services.servicediscovery.ServiceDiscoveryClient;
import software.amazon.awssdk.services.servicediscovery.model.CreateHttpNamespaceRequest;
import software.amazon.awssdk.services.servicediscovery.model.CreateServiceRequest;
import software.amazon.awssdk.services.servicediscovery.model.CustomHealthStatus;
import software.amazon.awssdk.services.servicediscovery.model.DeleteServiceRequest;
import software.amazon.awssdk.services.servicediscovery.model.DeregisterInstanceRequest;
import software.amazon.awssdk.services.servicediscovery.model.DnsConfig;
import software.amazon.awssdk.services.servicediscovery.model.DnsRecord;
import software.amazon.awssdk.services.servicediscovery.model.GetOperationRequest;
import software.amazon.awssdk.services.servicediscovery.model.HealthCheckCustomConfig;
import software.amazon.awssdk.services.servicediscovery.model.ListInstancesRequest;
import software.amazon.awssdk.services.servicediscovery.model.ListNamespacesRequest;
import software.amazon.awssdk.services.servicediscovery.model.ListServicesRequest;
import software.amazon.awssdk.services.servicediscovery.model.NamespaceSummary;
import software.amazon.awssdk.services.servicediscovery.model.NamespaceType;
import software.amazon.awssdk.services.servicediscovery.model.OperationStatus;
import software.amazon.awssdk.services.servicediscovery.model.RecordType;
import software.amazon.awssdk.services.servicediscovery.model.RegisterInstanceRequest;
import software.amazon.awssdk.services.servicediscovery.model.RoutingPolicy;
import software.amazon.awssdk.services.servicediscovery.model.ServiceFilter;
import software.amazon.awssdk.services.servicediscovery.model.ServiceFilterName;
import software.amazon.awssdk.services.servicediscovery.model.ServiceSummary;
import software.amazon.awssdk.services.servicediscovery.model.UpdateInstanceCustomHealthStatusRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/**
 * Starts a real ECS-backed three-role AD deployment for the live multi-tenant IT.
 */
final class EcsThreeRoleCluster implements AutoCloseable {
    private static final int S3_ARTIFACT_UPLOAD_ATTEMPTS = 6;
    private static final Gson GSON = new Gson();

    private static final String ECS_CLUSTER_PROPERTY = "tests.multiTenant.ecs.cluster";
    private static final String ECS_TASK_DEFINITION_PROPERTY = "tests.multiTenant.ecs.taskDefinition";
    private static final String ECS_SUBNETS_PROPERTY = "tests.multiTenant.ecs.subnets";
    private static final String ECS_SECURITY_GROUPS_PROPERTY = "tests.multiTenant.ecs.securityGroups";
    private static final String ECS_CONTAINER_NAME_PROPERTY = "tests.multiTenant.ecs.containerName";
    private static final String ECS_LAUNCH_TYPE_PROPERTY = "tests.multiTenant.ecs.launchType";
    private static final String ECS_ASSIGN_PUBLIC_IP_PROPERTY = "tests.multiTenant.ecs.assignPublicIp";
    private static final String ECS_ENDPOINT_ADDRESS_TYPE_PROPERTY = "tests.multiTenant.ecs.endpointAddressType";
    private static final String ECS_OPENSEARCH_PORT_PROPERTY = "tests.multiTenant.ecs.opensearchPort";
    private static final String ECS_IMAGE_PROPERTY = "tests.multiTenant.ecs.image";
    private static final String ECS_OPENSEARCH_JAVA_OPTS_PROPERTY = "tests.multiTenant.ecs.opensearchJavaOpts";
    private static final String ECS_MODEL_OPENSEARCH_JAVA_OPTS_PROPERTY = "tests.multiTenant.ecs.model.opensearchJavaOpts";
    private static final String ECS_CPU_PROPERTY = "tests.multiTenant.ecs.cpu";
    private static final String ECS_MEMORY_PROPERTY = "tests.multiTenant.ecs.memory";
    private static final String ECS_EXECUTION_ROLE_PROPERTY = "tests.multiTenant.ecs.executionRoleArn";
    private static final String ECS_TASK_ROLE_PROPERTY = "tests.multiTenant.ecs.taskRoleArn";
    private static final String ECS_AD_PLUGIN_ZIP_PROPERTY = "tests.multiTenant.ecs.adPluginZip";
    private static final String ECS_JOB_SCHEDULER_PLUGIN_ZIP_PROPERTY = "tests.multiTenant.ecs.jobSchedulerPluginZip";
    private static final String ECS_CLOUD_MAP_SERVICE_PROPERTY = "tests.multiTenant.ecs.cloudMapServiceName";
    private static final String ECS_CREATE_CLOUD_MAP_SERVICE_PROPERTY = "tests.multiTenant.ecs.createCloudMapService";
    private static final String ECS_STOP_TASKS_PROPERTY = "tests.multiTenant.ecs.stopTasksOnCompletion";
    private static final String ECS_ENABLE_EXECUTE_COMMAND_PROPERTY = "tests.multiTenant.ecs.enableExecuteCommand";
    private static final String ECS_MANAGE_EXECUTE_COMMAND_POLICY_PROPERTY = "tests.multiTenant.ecs.manageExecuteCommandPolicy";
    private static final String ECS_PROPAGATE_AWS_CREDENTIALS_PROPERTY = "tests.multiTenant.ecs.propagateAwsCredentials";
    private static final String ECS_CREATE_TASK_ROLE_PROPERTY = "tests.multiTenant.ecs.createTaskRole";
    private static final String ECS_MANAGE_TASK_ROLE_APPLICATION_POLICY_PROPERTY = "tests.multiTenant.ecs.manageTaskRoleApplicationPolicy";
    private static final String ECS_MANAGE_AOSS_ACCESS_POLICY_PROPERTY = "tests.multiTenant.ecs.manageAossAccessPolicy";
    private static final String ECS_AOSS_ACCESS_POLICY_NAME_PROPERTY = "tests.multiTenant.ecs.aossAccessPolicyName";
    private static final String ECS_ALLOWED_CIDR_PROPERTY = "tests.multiTenant.ecs.allowedCidr";
    private static final String ECS_CREATE_SQS_QUEUE_PROPERTY = "tests.multiTenant.ecs.createSqsQueue";
    private static final String ECS_CREATE_EVENT_BRIDGE_ROLES_PROPERTY = "tests.multiTenant.ecs.createEventBridgeRoles";
    private static final String ECS_AUTO_SCALING_ENABLED_PROPERTY = "tests.multiTenant.ecs.autoScaling.enabled";
    private static final String ECS_AUTO_SCALING_MIN_CAPACITY_PROPERTY = "tests.multiTenant.ecs.autoScaling.minCapacity";
    private static final String ECS_AUTO_SCALING_MAX_CAPACITY_PROPERTY = "tests.multiTenant.ecs.autoScaling.maxCapacity";
    private static final String ECS_AUTO_SCALING_TARGET_HEAP_PERCENT_PROPERTY = "tests.multiTenant.ecs.autoScaling.targetHeapPercent";
    private static final String ECS_AUTO_SCALING_COORDINATOR_TARGET_BACKLOG_DELETED_RATIO_PROPERTY =
        "tests.multiTenant.ecs.autoScaling.coordinator.targetBacklogDeletedRatio";
    private static final String ECS_AUTO_SCALING_COORDINATOR_TARGET_DELETED_ARRIVED_RATIO_PROPERTY =
        "tests.multiTenant.ecs.autoScaling.coordinator.targetDeletedArrivedRatio";
    private static final String ECS_AUTO_SCALING_SCALE_IN_COOLDOWN_PROPERTY = "tests.multiTenant.ecs.autoScaling.scaleInCooldown";
    private static final String ECS_AUTO_SCALING_SCALE_OUT_COOLDOWN_PROPERTY = "tests.multiTenant.ecs.autoScaling.scaleOutCooldown";
    private static final String ECS_AUTO_SCALING_METRIC_NAMESPACE_PROPERTY = "tests.multiTenant.ecs.autoScaling.metricNamespace";
    private static final String ECS_PREDICTIVE_SCALING_ENABLED_PROPERTY = "tests.multiTenant.ecs.autoScaling.predictive.enabled";
    private static final String ECS_PREDICTIVE_SCALING_MODE_PROPERTY = "tests.multiTenant.ecs.autoScaling.predictive.mode";
    private static final String ECS_PREDICTIVE_SCALING_BUFFER_SECONDS_PROPERTY =
        "tests.multiTenant.ecs.autoScaling.predictive.schedulingBufferSeconds";
    private static final String ECS_METRIC_INTERVAL_PROPERTY = "tests.multiTenant.ecs.autoScaling.metricPublishInterval";
    private static final String ECS_AUTO_SCALING_VERIFY_SCALE_PROPERTY = "tests.multiTenant.ecs.autoScaling.verifyScale.enabled";
    private static final String ECS_AUTO_SCALING_VERIFY_SCALE_IN_TIMEOUT_MINUTES_PROPERTY =
        "tests.multiTenant.ecs.autoScaling.verifyScale.scaleInTimeoutMinutes";
    private static final ScalableDimension ECS_SERVICE_DESIRED_COUNT = ScalableDimension.ECS_SERVICE_DESIRED_COUNT;
    private static final ServiceNamespace ECS_SERVICE_NAMESPACE = ServiceNamespace.ECS;
    private static final String REGION_PROPERTY = "tests.opensearch.plugins.timeseries.region";
    private static final String CLOUD_MAP_NAMESPACE_PROPERTY = "tests.opensearch.plugins.timeseries.cloud_map_namespace";
    private static final String CLOUD_MAP_SERVICE_PROPERTY = "tests.opensearch.plugins.timeseries.cloud_map_service";
    private static final String CLOUD_MAP_TABLE_PROPERTY = "tests.opensearch.plugins.timeseries.cloud_map_table_name";
    private static final String OPENSEARCH_PORT_PROPERTY = "tests.opensearch.plugins.timeseries.opensearch_port";
    private static final String INTERNAL_API_SHARED_SECRET_PROPERTY = "tests.opensearch.plugins.timeseries.internal_api_shared_secret";
    private static final String SQS_ACCOUNT_IDS_PROPERTY = "tests.opensearch.plugins.timeseries.sqs.account_ids";
    private static final String S3_BUCKET_PROPERTY = "tests.opensearch.plugins.anomaly_detection.s3_checkpoint_bucket";
    private static final String SQS_QUEUE_NAME_PROPERTY = "tests.opensearch.plugins.anomaly_detection.sqs.queue_name";
    private static final String SCHEDULER_GROUP_PROPERTY = "tests.opensearch.plugins.anomaly_detection.scheduler_group";
    private static final String SCHEDULE_MANAGEMENT_ROLE_NAME_PROPERTY =
        "tests.opensearch.plugins.anomaly_detection.event_bridge.schedule_management_role_name";
    private static final String SQS_DELIVERY_ROLE_NAME_PROPERTY =
        "tests.opensearch.plugins.anomaly_detection.event_bridge.sqs_delivery_role_name";
    private static final String DEFAULT_SCHEDULE_MANAGEMENT_ROLE_NAME = "ADScheduleManagementRole";
    private static final String DEFAULT_SQS_DELIVERY_ROLE_NAME = "SchedulerToSQSRole";
    private static final String EVENT_BRIDGE_SCHEDULE_MANAGEMENT_POLICY_NAME = "eventbridge-schedule-management";
    private static final String EVENT_BRIDGE_SQS_DELIVERY_POLICY_NAME = "eventbridge-sqs-delivery";
    private static final String ECS_EXEC_SSM_MESSAGES_POLICY_DOCUMENT =
        "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":["
            + "\"ssmmessages:CreateControlChannel\","
            + "\"ssmmessages:CreateDataChannel\","
            + "\"ssmmessages:OpenControlChannel\","
            + "\"ssmmessages:OpenDataChannel\"],\"Resource\":\"*\"}]}";
    private static final String ECS_TASK_ROLE_TRUST_POLICY_DOCUMENT =
        "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"ecs-tasks.amazonaws.com\"},"
            + "\"Action\":\"sts:AssumeRole\"}]}";

    private static String ecsTaskRoleApplicationPolicyDocument() {
        JsonObject policy = new JsonObject();
        policy.addProperty("Version", "2012-10-17");

        JsonArray statements = new JsonArray();
        statements.add(baseTaskRoleApplicationPolicyStatement());
        statements.add(scheduleManagementAssumeRolePolicyStatement());
        policy.add("Statement", statements);
        return GSON.toJson(policy);
    }

    private static JsonObject baseTaskRoleApplicationPolicyStatement() {
        JsonObject statement = new JsonObject();
        statement.addProperty("Effect", "Allow");
        JsonArray actions = new JsonArray();
        addActions(
            actions,
            "aoss:APIAccessAll",
            "cloudwatch:GetMetricData",
            "cloudwatch:GetMetricStatistics",
            "cloudwatch:ListMetrics",
            "cloudwatch:PutMetricData",
            "dynamodb:BatchGetItem",
            "dynamodb:BatchWriteItem",
            "dynamodb:DeleteItem",
            "dynamodb:DescribeTable",
            "dynamodb:GetItem",
            "dynamodb:PutItem",
            "dynamodb:Query",
            "dynamodb:Scan",
            "dynamodb:UpdateItem",
            "servicediscovery:DiscoverInstances",
            "s3:GetObject",
            "s3:PutObject",
            "s3:DeleteObject",
            "s3:ListBucket",
            "sqs:ReceiveMessage",
            "sqs:DeleteMessage",
            "sqs:ChangeMessageVisibility",
            "sqs:GetQueueAttributes",
            "sqs:GetQueueUrl",
            "ssmmessages:CreateControlChannel",
            "ssmmessages:CreateDataChannel",
            "ssmmessages:OpenControlChannel",
            "ssmmessages:OpenDataChannel"
        );
        statement.add("Action", actions);
        statement.addProperty("Resource", "*");
        return statement;
    }

    private static JsonObject scheduleManagementAssumeRolePolicyStatement() {
        JsonObject statement = new JsonObject();
        statement.addProperty("Effect", "Allow");
        JsonArray actions = new JsonArray();
        actions.add("sts:AssumeRole");
        statement.add("Action", actions);

        JsonArray resources = new JsonArray();
        String roleName = required(SCHEDULE_MANAGEMENT_ROLE_NAME_PROPERTY);
        List<String> accountIds = configuredSqsAccountIds();
        if (accountIds.isEmpty()) {
            throw new IllegalStateException("Missing required system property [" + SQS_ACCOUNT_IDS_PROPERTY + "]");
        }
        for (String accountId : accountIds) {
            resources.add("arn:aws:iam::" + accountId + ":role/" + roleName);
        }
        statement.add("Resource", resources);
        return statement;
    }

    private static List<String> configuredSqsAccountIds() {
        String rawAccountIds = property(SQS_ACCOUNT_IDS_PROPERTY, "");
        String normalized = rawAccountIds.trim();
        if (normalized.startsWith("[") && normalized.endsWith("]")) {
            normalized = normalized.substring(1, normalized.length() - 1);
        }

        List<String> accountIds = new ArrayList<>();
        for (String accountId : splitCsv(normalized)) {
            String normalizedAccountId = accountId.replace("\"", "").replace("'", "").trim();
            if (normalizedAccountId.isBlank() == false) {
                accountIds.add(normalizedAccountId);
            }
        }
        return accountIds;
    }

    private static void addActions(JsonArray actions, String... actionNames) {
        for (String actionName : actionNames) {
            actions.add(actionName);
        }
    }

    private static final String MASTER_CLUSTER_PROPERTY = "tests.master.rest.cluster";
    private static final String COORDINATOR_CLUSTER_PROPERTY = "tests.rest.cluster";
    private static final String MODEL_CLUSTER_PROPERTY = "tests.model.rest.cluster";

    private final String runId;
    private final String ecsCluster;
    private final String taskDefinition;
    private final String containerName;
    private final String protocol;
    private final int opensearchPort;
    private final String endpointAddressType;
    private final boolean stopTasksOnCompletion;
    private final boolean propagateAwsCredentials;
    private final Region region;
    private final String cloudMapNamespaceName;
    private final String cloudMapServiceName;
    private final String cloudMapTableName;
    private final String cloudMapServiceId;
    private final boolean createdCloudMapService;
    private final EcsClient ecsClient;
    private final Ec2Client ec2Client;
    private final S3Client s3Client;
    private final S3Presigner s3Presigner;
    private final CloudWatchLogsClient logsClient;
    private final ApplicationAutoScalingClient autoScalingClient;
    private final IamClient iamClient;
    private final OpenSearchServerlessClient openSearchServerlessClient;
    private final ServiceDiscoveryClient serviceDiscoveryClient;
    private final DynamoDbClient dynamoDbClient;
    private final SqsClient sqsClient;
    private final SqsQueueResources sqsQueueResources;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final List<StartedTask> tasks = new ArrayList<>();
    private final List<StartedService> services = new ArrayList<>();
    private final List<S3ObjectRef> uploadedArtifacts = new ArrayList<>();
    private final List<String> registeredTaskDefinitionArns = new ArrayList<>();
    private final Map<String, String> registeredModelInstances = new ConcurrentHashMap<>();
    private String createdSecurityGroupId;
    private String adPluginUrl;
    private String jobSchedulerPluginUrl;
    private String logGroupName;
    private String modelServiceName;
    private String managedExecuteCommandRoleName;
    private String managedExecuteCommandPolicyName;
    private String createdTaskRoleName;
    private String managedTaskRoleName;
    private String managedTaskRoleApplicationPolicyName;
    private String managedAossAccessPolicyName;
    private String managedAossAccessPolicyRoleArn;
    private String createdScheduleManagementRoleName;
    private String createdSqsDeliveryRoleName;
    private String lastAllowedCallerCidr;
    private ScheduledExecutorService modelCloudMapSyncExecutor;
    private Thread shutdownHook;

    private EcsThreeRoleCluster(
        String runId,
        String ecsCluster,
        String taskDefinition,
        String containerName,
        String protocol,
        int opensearchPort,
        String endpointAddressType,
        boolean stopTasksOnCompletion,
        boolean propagateAwsCredentials,
        Region region,
        String cloudMapNamespaceName,
        String cloudMapServiceName,
        String cloudMapTableName,
        String cloudMapServiceId,
        boolean createdCloudMapService,
        EcsClient ecsClient,
        Ec2Client ec2Client,
        S3Client s3Client,
        S3Presigner s3Presigner,
        CloudWatchLogsClient logsClient,
        ApplicationAutoScalingClient autoScalingClient,
        IamClient iamClient,
        OpenSearchServerlessClient openSearchServerlessClient,
        ServiceDiscoveryClient serviceDiscoveryClient,
        DynamoDbClient dynamoDbClient,
        SqsClient sqsClient,
        SqsQueueResources sqsQueueResources
    ) {
        this.runId = runId;
        this.ecsCluster = ecsCluster;
        this.taskDefinition = taskDefinition;
        this.containerName = containerName;
        this.protocol = protocol;
        this.opensearchPort = opensearchPort;
        this.endpointAddressType = endpointAddressType;
        this.stopTasksOnCompletion = stopTasksOnCompletion;
        this.propagateAwsCredentials = propagateAwsCredentials;
        this.region = region;
        this.cloudMapNamespaceName = cloudMapNamespaceName;
        this.cloudMapServiceName = cloudMapServiceName;
        this.cloudMapTableName = cloudMapTableName;
        this.cloudMapServiceId = cloudMapServiceId;
        this.createdCloudMapService = createdCloudMapService;
        this.ecsClient = ecsClient;
        this.ec2Client = ec2Client;
        this.s3Client = s3Client;
        this.s3Presigner = s3Presigner;
        this.logsClient = logsClient;
        this.autoScalingClient = autoScalingClient;
        this.iamClient = iamClient;
        this.openSearchServerlessClient = openSearchServerlessClient;
        this.serviceDiscoveryClient = serviceDiscoveryClient;
        this.dynamoDbClient = dynamoDbClient;
        this.sqsClient = sqsClient;
        this.sqsQueueResources = sqsQueueResources;
    }

    static EcsThreeRoleCluster start() {
        String runId = "ad-it-" + UUID.randomUUID().toString().substring(0, 8).toLowerCase(Locale.ROOT);
        Region region = Region.of(required(REGION_PROPERTY));
        EcsClient ecsClient = EcsClient
            .builder()
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .region(region)
            .credentialsProvider(SecurityUtil.createCredentialsProvider())
            .build();
        Ec2Client ec2Client = Ec2Client
            .builder()
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .region(region)
            .credentialsProvider(SecurityUtil.createCredentialsProvider())
            .build();
        S3Client s3Client = S3Client
            .builder()
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .region(region)
            .credentialsProvider(SecurityUtil.createCredentialsProvider())
            .build();
        S3Presigner s3Presigner = S3Presigner
            .builder()
            .region(region)
            .credentialsProvider(SecurityUtil.createCredentialsProvider())
            .build();
        CloudWatchLogsClient logsClient = CloudWatchLogsClient
            .builder()
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .region(region)
            .credentialsProvider(SecurityUtil.createCredentialsProvider())
            .build();
        ApplicationAutoScalingClient autoScalingClient = ApplicationAutoScalingClient
            .builder()
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .region(region)
            .credentialsProvider(SecurityUtil.createCredentialsProvider())
            .build();
        IamClient iamClient = IamClient
            .builder()
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .region(Region.AWS_GLOBAL)
            .credentialsProvider(SecurityUtil.createCredentialsProvider())
            .build();
        OpenSearchServerlessClient openSearchServerlessClient = OpenSearchServerlessClient
            .builder()
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .region(region)
            .credentialsProvider(SecurityUtil.createCredentialsProvider())
            .build();
        ServiceDiscoveryClient serviceDiscoveryClient = ServiceDiscoveryClient
            .builder()
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .region(region)
            .credentialsProvider(SecurityUtil.createCredentialsProvider())
            .build();
        DynamoDbClient dynamoDbClient = DynamoDbClient
            .builder()
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .region(region)
            .credentialsProvider(SecurityUtil.createCredentialsProvider())
            .build();
        SqsClient sqsClient = SqsClient
            .builder()
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .region(region)
            .credentialsProvider(SecurityUtil.createCredentialsProvider())
            .build();

        try {
            String ecsCluster = resolveEcsCluster(ecsClient);
            ManagedEventBridgeRoleNames managedEventBridgeRoleNames = prepareRunScopedEventBridgeRoleNames(runId);
            String configuredTaskDefinition = property(ECS_TASK_DEFINITION_PROPERTY, "");
            boolean generatedTaskDefinition = configuredTaskDefinition.isBlank();
            PluginArtifacts pluginArtifacts = generatedTaskDefinition ? uploadPluginArtifacts(s3Client, s3Presigner, runId) : null;
            ManagedTaskRole managedTaskRole = generatedTaskDefinition ? resolveOrCreateTaskRole(ecsClient, iamClient, runId) : null;
            String taskRoleArn = managedTaskRole == null ? "" : managedTaskRole.roleArn();
            String taskDefinition = generatedTaskDefinition
                ? registerOpenSearchTaskDefinition(ecsClient, logsClient, region, runId, pluginArtifacts, taskRoleArn)
                : configuredTaskDefinition;
            String effectiveTaskRoleArn = taskRoleArn(taskDefinition, ecsClient);
            String containerName = generatedTaskDefinition ? "opensearch" : configuredContainerName(ecsClient, taskDefinition);
            String protocol = Boolean.parseBoolean(System.getProperty("https", "false")) ? "https" : "http";
            int opensearchPort = Integer
                .parseInt(property(ECS_OPENSEARCH_PORT_PROPERTY, System.getProperty(OPENSEARCH_PORT_PROPERTY, "9200")));
            String endpointAddressType = property(ECS_ENDPOINT_ADDRESS_TYPE_PROPERTY, "public").toLowerCase(Locale.ROOT);
            boolean stopTasksOnCompletion = Boolean.parseBoolean(property(ECS_STOP_TASKS_PROPERTY, "true"));
            boolean propagateAwsCredentials = Boolean
                .parseBoolean(property(ECS_PROPAGATE_AWS_CREDENTIALS_PROPERTY, effectiveTaskRoleArn.isBlank() ? "true" : "false"));

            String namespaceName = required(CLOUD_MAP_NAMESPACE_PROPERTY);
            NamespaceSummary namespace = resolveOrCreateNamespace(serviceDiscoveryClient, namespaceName, runId);
            boolean createCloudMapService = Boolean.parseBoolean(property(ECS_CREATE_CLOUD_MAP_SERVICE_PROPERTY, "true"));
            String requestedServiceName = property(ECS_CLOUD_MAP_SERVICE_PROPERTY, "");
            String serviceName = requestedServiceName.isBlank() ? runId : requestedServiceName;
            ServiceResolution serviceResolution = resolveOrCreateService(
                serviceDiscoveryClient,
                namespace,
                serviceName,
                createCloudMapService
            );
            SqsQueueResources sqsQueueResources = resolveOrCreateSqsQueue(sqsClient, runId);
            ManagedEventBridgeRoles managedEventBridgeRoles = createRunScopedEventBridgeRolesIfNeeded(
                iamClient,
                region,
                sqsQueueResources,
                managedEventBridgeRoleNames
            );

            System.setProperty(CLOUD_MAP_SERVICE_PROPERTY, serviceName);
            System.setProperty(OPENSEARCH_PORT_PROPERTY, Integer.toString(opensearchPort));
            System.setProperty(SQS_QUEUE_NAME_PROPERTY, sqsQueueResources.queueName());

            EcsThreeRoleCluster cluster = new EcsThreeRoleCluster(
                runId,
                ecsCluster,
                taskDefinition,
                containerName,
                protocol,
                opensearchPort,
                endpointAddressType,
                stopTasksOnCompletion,
                propagateAwsCredentials,
                region,
                namespaceName,
                serviceName,
                required(CLOUD_MAP_TABLE_PROPERTY),
                serviceResolution.serviceId(),
                serviceResolution.created(),
                ecsClient,
                ec2Client,
                s3Client,
                s3Presigner,
                logsClient,
                autoScalingClient,
                iamClient,
                openSearchServerlessClient,
                serviceDiscoveryClient,
                dynamoDbClient,
                sqsClient,
                sqsQueueResources
            );
            cluster.registerShutdownHook();
            if (generatedTaskDefinition) {
                cluster.registeredTaskDefinitionArns.add(taskDefinition);
                cluster.adPluginUrl = pluginArtifacts.adPluginUrl();
                cluster.jobSchedulerPluginUrl = pluginArtifacts.jobSchedulerPluginUrl();
                cluster.logGroupName = logGroupName(runId);
                cluster.uploadedArtifacts.addAll(pluginArtifacts.uploadedObjects());
                if (managedTaskRole != null && managedTaskRole.created()) {
                    cluster.createdTaskRoleName = managedTaskRole.roleName();
                }
            }
            if (managedEventBridgeRoles != null) {
                cluster.createdScheduleManagementRoleName = managedEventBridgeRoles.scheduleManagementRoleName();
                cluster.createdSqsDeliveryRoleName = managedEventBridgeRoles.sqsDeliveryRoleName();
            }

            try {
                if (generatedTaskDefinition) {
                    cluster.configureTaskRoleApplicationPolicy(effectiveTaskRoleArn);
                    cluster.configureAossAccessPolicy(effectiveTaskRoleArn);
                    cluster.configureExecuteCommandTaskRolePolicy();
                }
                cluster.startTasks();
                if (cluster.modelServiceName == null) {
                    cluster.registerModelTaskInCloudMap();
                }
                cluster.waitForModelTaskRevision();
                cluster.publishRestClusterProperties();
                return cluster;
            } catch (RuntimeException e) {
                cluster.close();
                throw e;
            }
        } catch (RuntimeException e) {
            ecsClient.close();
            ec2Client.close();
            s3Client.close();
            s3Presigner.close();
            logsClient.close();
            autoScalingClient.close();
            iamClient.close();
            openSearchServerlessClient.close();
            serviceDiscoveryClient.close();
            dynamoDbClient.close();
            sqsClient.close();
            throw e;
        }
    }

    private static ManagedTaskRole resolveOrCreateTaskRole(EcsClient ecsClient, IamClient iamClient, String runId) {
        String configured = property(ECS_TASK_ROLE_PROPERTY, "");
        if (configured.isBlank() == false) {
            return new ManagedTaskRole(configured, roleNameFromArn(configured), false);
        }
        if (Boolean.parseBoolean(property(ECS_CREATE_TASK_ROLE_PROPERTY, "true")) == false) {
            String resolved = resolveTaskRoleArn(ecsClient);
            return resolved.isBlank() ? null : new ManagedTaskRole(resolved, roleNameFromArn(resolved), false);
        }

        String roleName = runId + "-task-role";
        try {
            iamClient
                .createRole(
                    CreateRoleRequest
                        .builder()
                        .roleName(roleName)
                        .assumeRolePolicyDocument(ECS_TASK_ROLE_TRUST_POLICY_DOCUMENT)
                        .description("Temporary ECS task role for " + runId)
                        .build()
                );
        } catch (software.amazon.awssdk.services.iam.model.EntityAlreadyExistsException ignored) {
            // Reused run ids are unlikely, but cleanup can safely handle an existing matching role.
        }
        String roleArn = iamClient.getRole(GetRoleRequest.builder().roleName(roleName).build()).role().arn();
        return new ManagedTaskRole(roleArn, roleName, true);
    }

    private static ManagedEventBridgeRoleNames prepareRunScopedEventBridgeRoleNames(String runId) {
        if (shouldCreateRunScopedEventBridgeRoles() == false) {
            return null;
        }
        if (DEFAULT_SCHEDULE_MANAGEMENT_ROLE_NAME.equals(property(SCHEDULE_MANAGEMENT_ROLE_NAME_PROPERTY, "")) == false
            || DEFAULT_SQS_DELIVERY_ROLE_NAME.equals(property(SQS_DELIVERY_ROLE_NAME_PROPERTY, "")) == false) {
            return null;
        }

        String scheduleManagementRoleName = runScopedScheduleManagementRoleName(runId);
        String sqsDeliveryRoleName = runScopedSqsDeliveryRoleName(runId);
        System.setProperty(SCHEDULE_MANAGEMENT_ROLE_NAME_PROPERTY, scheduleManagementRoleName);
        System.setProperty(SQS_DELIVERY_ROLE_NAME_PROPERTY, sqsDeliveryRoleName);
        return new ManagedEventBridgeRoleNames(scheduleManagementRoleName, sqsDeliveryRoleName);
    }

    private static boolean shouldCreateRunScopedEventBridgeRoles() {
        String configuredQueueName = property(SQS_QUEUE_NAME_PROPERTY, "");
        boolean createQueue = Boolean
            .parseBoolean(property(ECS_CREATE_SQS_QUEUE_PROPERTY, configuredQueueName.isBlank() ? "true" : "false"));
        return createQueue && Boolean.parseBoolean(property(ECS_CREATE_EVENT_BRIDGE_ROLES_PROPERTY, "true"));
    }

    private static ManagedEventBridgeRoles createRunScopedEventBridgeRolesIfNeeded(
        IamClient iamClient,
        Region region,
        SqsQueueResources sqsQueueResources,
        ManagedEventBridgeRoleNames roleNames
    ) {
        if (roleNames == null || sqsQueueResources.created() == false || sqsQueueResources.queueUrl().isBlank()) {
            return null;
        }

        String accountId = accountIdFromQueueUrl(sqsQueueResources.queueUrl());
        String queueArn = "arn:aws:sqs:" + region.id() + ":" + accountId + ":" + sqsQueueResources.queueName();
        String sqsDeliveryRoleArn = createRoleWithInlinePolicy(
            iamClient,
            roleNames.sqsDeliveryRoleName(),
            schedulerSqsDeliveryTrustPolicyDocument(accountId),
            EVENT_BRIDGE_SQS_DELIVERY_POLICY_NAME,
            sqsDeliveryPolicyDocument(queueArn)
        );
        String scheduleManagementRoleArn = createRoleWithInlinePolicy(
            iamClient,
            roleNames.scheduleManagementRoleName(),
            accountRootTrustPolicyDocument(accountId),
            EVENT_BRIDGE_SCHEDULE_MANAGEMENT_POLICY_NAME,
            scheduleManagementPolicyDocument(sqsDeliveryRoleArn, queueArn)
        );
        return new ManagedEventBridgeRoles(
            roleNames.scheduleManagementRoleName(),
            scheduleManagementRoleArn,
            roleNames.sqsDeliveryRoleName(),
            sqsDeliveryRoleArn
        );
    }

    private static String createRoleWithInlinePolicy(
        IamClient iamClient,
        String roleName,
        String trustPolicyDocument,
        String policyName,
        String policyDocument
    ) {
        try {
            iamClient
                .createRole(
                    CreateRoleRequest
                        .builder()
                        .roleName(roleName)
                        .assumeRolePolicyDocument(trustPolicyDocument)
                        .description("Temporary EventBridge/SQS role for ECS integration test")
                        .build()
                );
        } catch (software.amazon.awssdk.services.iam.model.EntityAlreadyExistsException ignored) {
            // Cleanup from a reused run id can safely remove the matching role later.
        }
        iamClient
            .putRolePolicy(PutRolePolicyRequest.builder().roleName(roleName).policyName(policyName).policyDocument(policyDocument).build());
        return iamClient.getRole(GetRoleRequest.builder().roleName(roleName).build()).role().arn();
    }

    private static String accountRootTrustPolicyDocument(String accountId) {
        JsonObject principal = new JsonObject();
        principal.addProperty("AWS", "arn:aws:iam::" + accountId + ":root");
        return trustPolicyDocument(principal);
    }

    private static String schedulerSqsDeliveryTrustPolicyDocument(String accountId) {
        JsonObject principal = new JsonObject();
        principal.addProperty("AWS", "arn:aws:iam::" + accountId + ":root");
        principal.addProperty("Service", "scheduler.amazonaws.com");
        return trustPolicyDocument(principal);
    }

    private static String trustPolicyDocument(JsonObject principal) {
        JsonObject statement = new JsonObject();
        statement.addProperty("Effect", "Allow");
        statement.add("Principal", principal);
        JsonArray actions = new JsonArray();
        actions.add("sts:AssumeRole");
        statement.add("Action", actions);

        JsonObject policy = new JsonObject();
        policy.addProperty("Version", "2012-10-17");
        JsonArray statements = new JsonArray();
        statements.add(statement);
        policy.add("Statement", statements);
        return GSON.toJson(policy);
    }

    private static String scheduleManagementPolicyDocument(String sqsDeliveryRoleArn, String queueArn) {
        JsonArray statements = new JsonArray();
        statements
            .add(
                policyStatement(
                    List
                        .of(
                            "scheduler:CreateSchedule",
                            "scheduler:UpdateSchedule",
                            "scheduler:DeleteSchedule",
                            "scheduler:GetSchedule",
                            "scheduler:ListSchedules",
                            "cloudwatch:GetMetricData",
                            "cloudwatch:GetMetricStatistics",
                            "cloudwatch:ListMetrics"
                        ),
                    "*"
                )
            );
        statements
            .add(
                policyStatement(
                    List
                        .of(
                            "sqs:ReceiveMessage",
                            "sqs:DeleteMessage",
                            "sqs:ChangeMessageVisibility",
                            "sqs:GetQueueAttributes",
                            "sqs:GetQueueUrl"
                        ),
                    queueArn
                )
            );
        statements.add(policyStatement(List.of("iam:PassRole"), sqsDeliveryRoleArn));
        return policyDocument(statements);
    }

    private static String sqsDeliveryPolicyDocument(String queueArn) {
        return policyDocument(policyStatement(List.of("sqs:SendMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"), queueArn));
    }

    private static String policyDocument(JsonObject statement) {
        JsonArray statements = new JsonArray();
        statements.add(statement);
        return policyDocument(statements);
    }

    private static String policyDocument(JsonArray statements) {
        JsonObject policy = new JsonObject();
        policy.addProperty("Version", "2012-10-17");
        policy.add("Statement", statements);
        return GSON.toJson(policy);
    }

    private static JsonObject policyStatement(List<String> actions, String resource) {
        JsonObject statement = new JsonObject();
        statement.addProperty("Effect", "Allow");
        JsonArray actionArray = new JsonArray();
        actions.forEach(actionArray::add);
        statement.add("Action", actionArray);
        statement.addProperty("Resource", resource);
        return statement;
    }

    private static String accountIdFromQueueUrl(String queueUrl) {
        String[] parts = queueUrl.split("/");
        if (parts.length < 5 || parts[3].isBlank()) {
            throw new IllegalArgumentException("Unable to extract account id from SQS queue URL: " + queueUrl);
        }
        return parts[3];
    }

    private static String runScopedScheduleManagementRoleName(String runId) {
        return runId + "-schedule-role";
    }

    private static String runScopedSqsDeliveryRoleName(String runId) {
        return runId + "-sqs-delivery-role";
    }

    private void configureTaskRoleApplicationPolicy(String taskRoleArn) {
        if (taskRoleArn == null
            || taskRoleArn.isBlank()
            || Boolean.parseBoolean(property(ECS_MANAGE_TASK_ROLE_APPLICATION_POLICY_PROPERTY, "true")) == false) {
            return;
        }
        String roleName = roleNameFromArn(taskRoleArn);
        String policyName = runId + "-ecs-task-app-access";
        iamClient
            .putRolePolicy(
                PutRolePolicyRequest
                    .builder()
                    .roleName(roleName)
                    .policyName(policyName)
                    .policyDocument(ecsTaskRoleApplicationPolicyDocument())
                    .build()
            );
        managedTaskRoleName = roleName;
        managedTaskRoleApplicationPolicyName = policyName;
    }

    private void configureAossAccessPolicy(String taskRoleArn) {
        if (taskRoleArn == null
            || taskRoleArn.isBlank()
            || Boolean.parseBoolean(property(ECS_MANAGE_AOSS_ACCESS_POLICY_PROPERTY, "true")) == false) {
            return;
        }
        String policyName = resolveAossAccessPolicyName();
        if (policyName.isBlank()) {
            return;
        }

        var policy = openSearchServerlessClient
            .getAccessPolicy(GetAccessPolicyRequest.builder().type(AccessPolicyType.DATA).name(policyName).build())
            .accessPolicyDetail();
        String policyDocument = policyDocumentToJson(policy.policy());
        JsonArray updatedPolicy = JsonParser.parseString(policyDocument).getAsJsonArray();
        if (updateAossPolicyPrincipal(updatedPolicy, taskRoleArn, true)) {
            openSearchServerlessClient
                .updateAccessPolicy(
                    UpdateAccessPolicyRequest
                        .builder()
                        .type(AccessPolicyType.DATA)
                        .name(policyName)
                        .policyVersion(policy.policyVersion())
                        .policy(updatedPolicy.toString())
                        .build()
                );
        }
        managedAossAccessPolicyName = policyName;
        managedAossAccessPolicyRoleArn = taskRoleArn;
    }

    private String resolveAossAccessPolicyName() {
        return resolveAossAccessPolicyName(openSearchServerlessClient);
    }

    private static String resolveAossAccessPolicyName(OpenSearchServerlessClient openSearchServerlessClient) {
        String configured = property(ECS_AOSS_ACCESS_POLICY_NAME_PROPERTY, "");
        if (configured.isBlank() == false) {
            return configured;
        }
        String collectionName = resolveAossCollectionName(openSearchServerlessClient);
        if (collectionName == null || collectionName.isBlank()) {
            return "";
        }
        for (AccessPolicySummary summary : openSearchServerlessClient
            .listAccessPolicies(ListAccessPoliciesRequest.builder().type(AccessPolicyType.DATA).build())
            .accessPolicySummaries()) {
            Document policy = openSearchServerlessClient
                .getAccessPolicy(GetAccessPolicyRequest.builder().type(AccessPolicyType.DATA).name(summary.name()).build())
                .accessPolicyDetail()
                .policy();
            String policyDocument = policyDocumentToJson(policy);
            if (aossPolicyReferencesCollection(policyDocument, collectionName)) {
                return summary.name();
            }
        }
        return "";
    }

    private static String resolveAossCollectionName(OpenSearchServerlessClient openSearchServerlessClient) {
        String endpoint = firstNonBlank(
            System.getProperty("tests.opensearch.plugins.anomaly_detection.remote_metadata_endpoint"),
            System.getProperty("tests.opensearch.plugins.timeseries.dataplane.endpoint")
        );
        if (endpoint == null || endpoint.isBlank()) {
            return "";
        }
        String collectionId = collectionIdFromEndpoint(endpoint);
        if (collectionId.isBlank()) {
            return "";
        }
        for (CollectionSummary collection : openSearchServerlessClient
            .listCollections(ListCollectionsRequest.builder().build())
            .collectionSummaries()) {
            if (collectionId.equals(collection.id())) {
                return collection.name();
            }
        }
        return "";
    }

    private static String collectionIdFromEndpoint(String endpoint) {
        try {
            String host = URI.create(endpoint).getHost();
            int separator = host.indexOf('.');
            return separator < 0 ? host : host.substring(0, separator);
        } catch (Exception e) {
            return "";
        }
    }

    private static boolean aossPolicyReferencesCollection(String policyDocument, String collectionName) {
        JsonArray policy = JsonParser.parseString(policyDocument).getAsJsonArray();
        for (JsonElement statementElement : policy) {
            JsonObject statement = statementElement.getAsJsonObject();
            JsonArray rules = statement.getAsJsonArray("Rules");
            if (rules == null) {
                continue;
            }
            for (JsonElement ruleElement : rules) {
                JsonArray resources = ruleElement.getAsJsonObject().getAsJsonArray("Resource");
                if (resources == null) {
                    continue;
                }
                for (JsonElement resourceElement : resources) {
                    String resource = resourceElement.getAsString();
                    if (resource.equals("collection/" + collectionName) || resource.startsWith("index/" + collectionName + "/")) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static boolean updateAossPolicyPrincipal(JsonArray policy, String roleArn, boolean add) {
        boolean changed = false;
        for (JsonElement statementElement : policy) {
            JsonObject statement = statementElement.getAsJsonObject();
            JsonArray principals = statement.getAsJsonArray("Principal");
            if (principals == null) {
                continue;
            }
            int existingIndex = indexOfString(principals, roleArn);
            if (add) {
                if (existingIndex < 0) {
                    principals.add(roleArn);
                    changed = true;
                }
            } else if (existingIndex >= 0) {
                principals.remove(existingIndex);
                changed = true;
            }
        }
        return changed;
    }

    private static int indexOfString(JsonArray array, String value) {
        for (int i = 0; i < array.size(); i++) {
            if (Objects.equals(value, array.get(i).getAsString())) {
                return i;
            }
        }
        return -1;
    }

    private static String policyDocumentToJson(Document policy) {
        return GSON.toJson(policy.unwrap());
    }

    private static String firstNonBlank(String first, String second) {
        if (first != null && first.isBlank() == false) {
            return first;
        }
        if (second != null && second.isBlank() == false) {
            return second;
        }
        return null;
    }

    private void configureExecuteCommandTaskRolePolicy() {
        if (Boolean.parseBoolean(property(ECS_ENABLE_EXECUTE_COMMAND_PROPERTY, "true")) == false
            || Boolean.parseBoolean(property(ECS_MANAGE_EXECUTE_COMMAND_POLICY_PROPERTY, "true")) == false) {
            return;
        }
        String taskRoleArn = taskRoleArn(taskDefinition);
        if (taskRoleArn.isBlank()) {
            return;
        }
        String roleName = roleNameFromArn(taskRoleArn);
        String policyName = runId + "-ecs-exec-ssmmessages";
        iamClient
            .putRolePolicy(
                PutRolePolicyRequest
                    .builder()
                    .roleName(roleName)
                    .policyName(policyName)
                    .policyDocument(ECS_EXEC_SSM_MESSAGES_POLICY_DOCUMENT)
                    .build()
            );
        managedExecuteCommandRoleName = roleName;
        managedExecuteCommandPolicyName = policyName;
    }

    private String taskRoleArn(String taskDefinitionArn) {
        return taskRoleArn(taskDefinitionArn, ecsClient);
    }

    private static String taskRoleArn(String taskDefinitionArn, EcsClient ecsClient) {
        TaskDefinition described = ecsClient
            .describeTaskDefinition(DescribeTaskDefinitionRequest.builder().taskDefinition(taskDefinitionArn).build())
            .taskDefinition();
        return described.taskRoleArn() == null ? "" : described.taskRoleArn();
    }

    private static String roleNameFromArn(String roleArn) {
        int separator = roleArn.lastIndexOf('/');
        if (separator < 0 || separator == roleArn.length() - 1) {
            return roleArn;
        }
        return roleArn.substring(separator + 1);
    }

    String modelPrivateIp() {
        return task(TimeSeriesSettings.MODEL_ROLE).privateIp();
    }

    String currentCoordinatorEndpoint() {
        if (services.stream().anyMatch(service -> TimeSeriesSettings.COORDINATOR_ROLE.equals(service.role()))) {
            refreshAllowedCallerIngress();
            StartedTask current = task(TimeSeriesSettings.COORDINATOR_ROLE);
            if (isOpenSearchEndpointHealthy(current.endpoint()) == false) {
                refreshServiceTaskReference(service(TimeSeriesSettings.COORDINATOR_ROLE));
                publishRestClusterProperties();
            }
        }
        return task(TimeSeriesSettings.COORDINATOR_ROLE).endpoint();
    }

    String runId() {
        return runId;
    }

    String diagnostics() {
        StringBuilder builder = new StringBuilder("ECS tasks for ").append(runId).append(":\n");
        for (StartedTask task : tasks) {
            try {
                builder.append("- ").append(task.role()).append(": ").append(taskSummary(describeTask(task.taskArn()))).append('\n');
            } catch (Exception | AssertionError e) {
                builder
                    .append("- ")
                    .append(task.role())
                    .append(": failed to describe task ")
                    .append(task.taskArn())
                    .append(": ")
                    .append(e)
                    .append('\n');
            }
        }
        for (StartedService service : services) {
            builder.append("- service ").append(service.role()).append(": ").append(serviceSummary(service)).append('\n');
        }
        return builder.append(recentContainerLogs()).toString();
    }

    void verifyAutoScalingConfiguration() {
        if (autoScalingEnabled() == false) {
            return;
        }
        int minCapacity = autoScalingMinCapacity();
        int maxCapacity = autoScalingMaxCapacity();
        for (StartedService service : services) {
            if (service.policyNames().isEmpty()) {
                software.amazon.awssdk.services.ecs.model.Service ecsService = describeService(service);
                assertEquals(
                    "Expected non-autoscaled ECS service " + service.serviceName() + " to keep desired count 1",
                    1,
                    ecsService.desiredCount().intValue()
                );
                continue;
            }
            var scalableTargets = autoScalingClient
                .describeScalableTargets(
                    DescribeScalableTargetsRequest
                        .builder()
                        .serviceNamespace(ECS_SERVICE_NAMESPACE)
                        .scalableDimension(ECS_SERVICE_DESIRED_COUNT)
                        .resourceIds(service.resourceId())
                        .build()
                )
                .scalableTargets();
            assertEquals("Expected one scalable target for " + service.serviceName(), 1, scalableTargets.size());
            assertEquals(
                "Unexpected min capacity for " + service.serviceName(),
                minCapacity,
                scalableTargets.get(0).minCapacity().intValue()
            );
            assertEquals(
                "Unexpected max capacity for " + service.serviceName(),
                maxCapacity,
                scalableTargets.get(0).maxCapacity().intValue()
            );

            var policies = autoScalingClient
                .describeScalingPolicies(
                    DescribeScalingPoliciesRequest
                        .builder()
                        .serviceNamespace(ECS_SERVICE_NAMESPACE)
                        .scalableDimension(ECS_SERVICE_DESIRED_COUNT)
                        .resourceId(service.resourceId())
                        .build()
                )
                .scalingPolicies();
            Set<String> installedPolicyNames = policies
                .stream()
                .map(policy -> policy.policyName())
                .collect(java.util.stream.Collectors.toSet());
            for (String expectedPolicyName : service.policyNames()) {
                assertTrue(
                    "Expected scaling policy " + expectedPolicyName + " for " + service.serviceName(),
                    installedPolicyNames.contains(expectedPolicyName)
                );
            }
        }
        waitForModelServiceHashRing(minCapacity);
        refreshServiceTaskReferences();
        publishRestClusterProperties();
    }

    void verifyMetricDrivenAutoScaling(ThrowingRunnable triggerWork, ThrowingRunnable stopWork, String tenantId) throws Exception {
        if (autoScalingEnabled() == false || Boolean.parseBoolean(property(ECS_AUTO_SCALING_VERIFY_SCALE_PROPERTY, "true")) == false) {
            return;
        }
        int minCapacity = autoScalingMinCapacity();
        int scaleOutCapacity = Math.min(autoScalingMaxCapacity(), Math.max(minCapacity + 1, 2));
        assertTrue(
            "Metric-driven auto-scaling verification requires " + ECS_AUTO_SCALING_MAX_CAPACITY_PROPERTY + " greater than min capacity",
            scaleOutCapacity > minCapacity
        );

        StartedService modelService = service(TimeSeriesSettings.MODEL_ROLE);
        StartedService coordinatorService = service(TimeSeriesSettings.COORDINATOR_ROLE);
        int modelObservedPeak = scaleOutCapacity;
        triggerWork.run();
        try {
            waitForServiceRunningCountAtLeast(modelService, scaleOutCapacity);
            waitForServiceTasksHealthyAtLeast(modelService, scaleOutCapacity);
            waitForModelServiceHashRingAtLeast(scaleOutCapacity);
            waitForCoordinatorStatsModelNodesAtLeast(scaleOutCapacity, tenantId);
            waitForServiceRunningCountAtLeast(coordinatorService, scaleOutCapacity);
            waitForServiceTasksHealthyAtLeast(coordinatorService, scaleOutCapacity);
            modelObservedPeak = Math.max(modelObservedPeak, desiredCountOrDefault(modelService, scaleOutCapacity));
        } finally {
            stopWork.run();
        }
        waitForModelCountAtMost(0L, tenantId);
        int scaledInModelTaskCount = waitForServiceScaleInFromObservedPeak(modelService, modelObservedPeak);
        waitForModelServiceHashRingAtMost(scaledInModelTaskCount);
        waitForServiceRunningCount(coordinatorService, minCapacity);
        waitForServiceTasksHealthy(coordinatorService, minCapacity);
        refreshServiceTaskReferences();
        publishRestClusterProperties();
    }

    void waitForModelTaskRevision() {
        Awaitility.await().atMost(Duration.ofMinutes(3)).pollInterval(Duration.ofSeconds(3)).untilAsserted(() -> {
            List<String> tasks = latestHashRingTasks();
            assertTrue(
                "Expected hash-ring revision for " + cloudMapServiceName + " to contain " + modelPrivateIp() + " but got " + tasks,
                tasks.contains(modelPrivateIp())
            );
        });
    }

    private void waitForCoordinatorStatsModelNodesAtLeast(int expectedModelNodes, String tenantId) {
        Awaitility.await().atMost(Duration.ofMinutes(5)).pollInterval(Duration.ofSeconds(10)).untilAsserted(() -> {
            int observedNodes = coordinatorStatsNodeCount(tenantId);
            assertTrue(
                "Expected coordinator stats to include at least "
                    + expectedModelNodes
                    + " model nodes plus the coordinator, but got "
                    + observedNodes,
                observedNodes >= expectedModelNodes + 1
            );
        });
    }

    private void waitForModelCountAtMost(long expectedModelCount, String tenantId) {
        Awaitility.await().atMost(Duration.ofMinutes(20)).pollInterval(Duration.ofSeconds(15)).untilAsserted(() -> {
            long modelCount = totalModelCountOnRunningModelTasks(tenantId);
            assertTrue(
                "Expected stop/delete detector to clear autoscaling workload models; model_count="
                    + modelCount
                    + ", expected_at_most="
                    + expectedModelCount,
                modelCount <= expectedModelCount
            );
        });
    }

    private int coordinatorStatsNodeCount(String tenantId) {
        StartedTask coordinatorTask = task(TimeSeriesSettings.COORDINATOR_ROLE);
        Map<String, Object> response = requestStats(
            coordinatorTask.endpoint() + TimeSeriesAnalyticsPlugin.AD_BASE_URI + "/stats/" + StatNames.MODEL_COUNT.getName(),
            tenantId,
            false
        );
        return statsNodes(response).size();
    }

    private long totalModelCountOnRunningModelTasks(String tenantId) {
        if (services.stream().noneMatch(service -> TimeSeriesSettings.MODEL_ROLE.equals(service.role()))) {
            return 0L;
        }
        StartedService modelService = service(TimeSeriesSettings.MODEL_ROLE);
        long total = 0L;
        for (Task runningTask : runningServiceTasks(modelService)) {
            StartedTask modelTask = startedTaskFromRunningTask(TimeSeriesSettings.MODEL_ROLE, runningTask);
            total += modelCountOnTask(modelTask, tenantId);
        }
        return total;
    }

    private long modelCountOnTask(StartedTask modelTask, String tenantId) {
        Map<String, Object> response = requestStats(
            modelTask.endpoint()
                + TimeSeriesAnalyticsPlugin.AD_BASE_INTERNAL_DETECTORS_URI
                + "/"
                + RestHandlerUtils.STATS_NODES
                + "?stat="
                + StatNames.MODEL_COUNT.getName(),
            tenantId,
            true
        );
        long total = 0L;
        for (Object nodeStats : statsNodes(response).values()) {
            if (nodeStats instanceof Map<?, ?> stats) {
                Object count = stats.get(StatNames.MODEL_COUNT.getName());
                if (count instanceof Number number) {
                    total += number.longValue();
                }
            }
        }
        return total;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> statsNodes(Map<String, Object> response) {
        Object nodes = response.get("nodes");
        if (nodes instanceof Map<?, ?> nodesMap) {
            return (Map<String, Object>) nodesMap;
        }
        return Collections.emptyMap();
    }

    private Map<String, Object> requestStats(String endpoint, String tenantId, boolean internal) {
        try {
            HttpURLConnection connection = (HttpURLConnection) URI.create(endpoint).toURL().openConnection();
            if (connection instanceof HttpsURLConnection httpsConnection) {
                trustAll(httpsConnection);
            }
            connection.setConnectTimeout((int) Duration.ofSeconds(10).toMillis());
            connection.setReadTimeout((int) Duration.ofSeconds(30).toMillis());
            connection.setRequestMethod("GET");
            if (internal) {
                connection.setRequestProperty("x-timeseries-internal-token", required(INTERNAL_API_SHARED_SECRET_PROPERTY));
            }
            if (tenantId != null) {
                connection.setRequestProperty("x-tenant-id", tenantId);
            }
            connection.connect();
            int status = connection.getResponseCode();
            if (status >= HttpURLConnection.HTTP_BAD_REQUEST) {
                throw new IllegalStateException("Stats request to " + endpoint + " failed with status " + status);
            }
            String responseBody;
            try (InputStream input = connection.getInputStream()) {
                responseBody = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            }
            return XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), responseBody, false);
        } catch (IOException | GeneralSecurityException e) {
            throw new IllegalStateException("Failed to request stats from " + endpoint, e);
        }
    }

    private List<String> latestHashRingTasks() {
        QueryRequest request = QueryRequest
            .builder()
            .tableName(cloudMapTableName)
            .keyConditionExpression("PK = :pk")
            .expressionAttributeValues(Map.of(":pk", AttributeValue.fromS("service#" + cloudMapServiceName)))
            .scanIndexForward(false)
            .limit(1)
            .build();
        var response = dynamoDbClient.query(request);
        if (response.items().isEmpty()) {
            return List.of();
        }
        AttributeValue tasks = response.items().get(0).get("tasks");
        if (tasks == null || tasks.l() == null) {
            return List.of();
        }
        return tasks.l().stream().map(AttributeValue::s).filter(value -> value != null && value.isBlank() == false).toList();
    }

    private void startTasks() {
        NetworkResources networkResources = resolveNetworkResources();
        List<String> subnets = networkResources.subnets();
        List<String> securityGroups = networkResources.securityGroups();
        AssignPublicIp assignPublicIp = AssignPublicIp.fromValue(property(ECS_ASSIGN_PUBLIC_IP_PROPERTY, "ENABLED"));
        LaunchType launchType = LaunchType.fromValue(property(ECS_LAUNCH_TYPE_PROPERTY, "FARGATE"));

        StartedService masterService = createServiceForRole(
            TimeSeriesSettings.MASTER_ROLE,
            subnets,
            securityGroups,
            assignPublicIp,
            launchType
        );
        services.add(masterService);
        tasks.add(waitForServiceTask(masterService));
        if (autoScalingEnabled()) {
            StartedService coordinatorService = createServiceForRole(
                TimeSeriesSettings.COORDINATOR_ROLE,
                subnets,
                securityGroups,
                assignPublicIp,
                launchType
            );
            StartedService modelService = createServiceForRole(
                TimeSeriesSettings.MODEL_ROLE,
                subnets,
                securityGroups,
                assignPublicIp,
                launchType
            );
            services.add(coordinatorService);
            services.add(modelService);
            modelServiceName = modelService.serviceName();
            tasks.add(waitForServiceTask(coordinatorService));
            tasks.add(waitForServiceTask(modelService));
            startModelCloudMapSync();
            syncModelServiceCloudMapInstances();
        } else {
            tasks.add(runTask(TimeSeriesSettings.COORDINATOR_ROLE, subnets, securityGroups, assignPublicIp, launchType));
            tasks.add(runTask(TimeSeriesSettings.MODEL_ROLE, subnets, securityGroups, assignPublicIp, launchType));
        }

        for (StartedTask task : tasks) {
            waitForHttp(task);
        }
    }

    private boolean autoScalingEnabled() {
        return Boolean.parseBoolean(property(ECS_AUTO_SCALING_ENABLED_PROPERTY, "true"));
    }

    private boolean enableExecuteCommand(String taskDefinitionArn) {
        if (Boolean.parseBoolean(property(ECS_ENABLE_EXECUTE_COMMAND_PROPERTY, "true")) == false) {
            return false;
        }
        TaskDefinition described = ecsClient
            .describeTaskDefinition(DescribeTaskDefinitionRequest.builder().taskDefinition(taskDefinitionArn).build())
            .taskDefinition();
        return described.taskRoleArn() != null && described.taskRoleArn().isBlank() == false;
    }

    private StartedService createServiceForRole(
        String role,
        List<String> subnets,
        List<String> securityGroups,
        AssignPublicIp assignPublicIp,
        LaunchType launchType
    ) {
        String serviceName = ecsServiceName(role);
        String roleTaskDefinition = registerRoleTaskDefinition(role, serviceName);
        software.amazon.awssdk.services.ecs.model.CreateServiceRequest request =
            software.amazon.awssdk.services.ecs.model.CreateServiceRequest
                .builder()
                .cluster(ecsCluster)
                .serviceName(serviceName)
                .taskDefinition(roleTaskDefinition)
                .desiredCount(serviceDesiredCount(role))
                .launchType(launchType)
                .networkConfiguration(
                    NetworkConfiguration
                        .builder()
                        .awsvpcConfiguration(
                            AwsVpcConfiguration
                                .builder()
                                .subnets(subnets)
                                .securityGroups(securityGroups)
                                .assignPublicIp(assignPublicIp)
                                .build()
                        )
                        .build()
                )
                .enableECSManagedTags(true)
                .enableExecuteCommand(enableExecuteCommand(roleTaskDefinition))
                .tags(software.amazon.awssdk.services.ecs.model.Tag.builder().key("ad-integ-test-run").value(runId).build())
                .build();
        ecsClient.createService(request);

        String resourceId = autoScalingResourceId(serviceName);
        List<String> policyNames = shouldConfigureAutoScaling(role)
            ? configureAutoScalingPolicies(role, serviceName, resourceId)
            : List.of();
        return new StartedService(role, serviceName, roleTaskDefinition, resourceId, policyNames);
    }

    private int serviceDesiredCount(String role) {
        return TimeSeriesSettings.MASTER_ROLE.equals(role) ? 1 : autoScalingMinCapacity();
    }

    private boolean shouldConfigureAutoScaling(String role) {
        return autoScalingEnabled() && (TimeSeriesSettings.COORDINATOR_ROLE.equals(role) || TimeSeriesSettings.MODEL_ROLE.equals(role));
    }

    private int desiredCountOrDefault(StartedService service, int defaultCount) {
        try {
            return describeService(service).desiredCount();
        } catch (AssertionError e) {
            if (e.getCause() instanceof SdkException) {
                return defaultCount;
            }
            throw e;
        }
    }

    private StartedTask waitForServiceTask(StartedService service) {
        final Task[] found = new Task[1];
        Awaitility.await().atMost(Duration.ofMinutes(8)).pollInterval(Duration.ofSeconds(5)).untilAsserted(() -> {
            try {
                List<String> taskArns = listServiceTaskArns(service.serviceName());
                assertFalse("Expected ECS service " + service.serviceName() + " to have a running task", taskArns.isEmpty());
                var response = ecsClient.describeTasks(DescribeTasksRequest.builder().cluster(ecsCluster).tasks(taskArns).build());
                List<Task> runningTasks = response.tasks().stream().filter(task -> "RUNNING".equals(task.lastStatus())).toList();
                assertFalse("ECS service tasks did not have a RUNNING task for " + service.serviceName(), runningTasks.isEmpty());
                Task task = runningTasks.get(0);
                found[0] = task;
            } catch (SdkException e) {
                throw new AssertionError("Transient AWS client failure waiting for ECS service task " + service.serviceName(), e);
            }
        });
        return startedTaskFromRunningTask(service.role(), found[0]);
    }

    private void updateServiceDesiredCount(StartedService service, int desiredCount) {
        ecsClient
            .updateService(
                UpdateServiceRequest.builder().cluster(ecsCluster).service(service.serviceName()).desiredCount(desiredCount).build()
            );
    }

    private void waitForServiceRunningCount(StartedService service, int expectedCount) {
        Awaitility.await().atMost(Duration.ofMinutes(35)).pollInterval(Duration.ofSeconds(10)).untilAsserted(() -> {
            software.amazon.awssdk.services.ecs.model.Service ecsService = describeService(service);
            List<Task> runningTasks = runningServiceTasks(service);
            assertEquals(
                "Unexpected desired count for ECS service " + service.serviceName() + ": " + serviceSummary(service),
                expectedCount,
                ecsService.desiredCount().intValue()
            );
            assertEquals(
                "Unexpected running count for ECS service " + service.serviceName() + ": " + serviceSummary(service),
                expectedCount,
                ecsService.runningCount().intValue()
            );
            assertEquals(
                "Unexpected RUNNING task count for ECS service " + service.serviceName() + ": " + serviceSummary(service),
                expectedCount,
                runningTasks.size()
            );
        });
    }

    private int waitForServiceScaleInFromObservedPeak(StartedService service, int initialPeak) {
        AtomicInteger observedPeak = new AtomicInteger(initialPeak);
        AtomicInteger scaledInMaximumCount = new AtomicInteger(initialPeak - 1);
        assertTrue(
            "Scale-in verification requires an observed peak above min capacity",
            scaledInMaximumCount.get() >= autoScalingMinCapacity()
        );
        Awaitility
            .await()
            .atMost(Duration.ofMinutes(autoScalingVerifyScaleInTimeoutMinutes()))
            .pollInterval(Duration.ofSeconds(10))
            .untilAsserted(() -> {
                software.amazon.awssdk.services.ecs.model.Service ecsService = describeService(service);
                List<Task> runningTasks = runningServiceTasks(service);
                int currentPeak = Math.max(ecsService.desiredCount(), Math.max(ecsService.runningCount(), runningTasks.size()));
                int highWater = observedPeak.updateAndGet(previous -> Math.max(previous, currentPeak));
                int maximumCount = highWater - 1;
                scaledInMaximumCount.set(maximumCount);
                assertTrue(
                    "Expected desired count for ECS service "
                        + service.serviceName()
                        + " to scale in below observed high-water "
                        + highWater
                        + ": "
                        + serviceSummary(service),
                    ecsService.desiredCount() <= maximumCount
                );
                assertTrue(
                    "Expected running count for ECS service "
                        + service.serviceName()
                        + " to scale in below observed high-water "
                        + highWater
                        + ": "
                        + serviceSummary(service),
                    ecsService.runningCount() <= maximumCount
                );
                assertTrue(
                    "Expected RUNNING task count for ECS service "
                        + service.serviceName()
                        + " to scale in below observed high-water "
                        + highWater
                        + ": "
                        + serviceSummary(service),
                    runningTasks.size() <= maximumCount
                );
            });
        return scaledInMaximumCount.get();
    }

    private void waitForServiceRunningCountAtLeast(StartedService service, int minimumCount) {
        Awaitility.await().atMost(Duration.ofMinutes(20)).pollInterval(Duration.ofSeconds(10)).untilAsserted(() -> {
            software.amazon.awssdk.services.ecs.model.Service ecsService = describeService(service);
            List<Task> runningTasks = runningServiceTasks(service);
            assertTrue(
                "Expected desired count for ECS service "
                    + service.serviceName()
                    + " to be at least "
                    + minimumCount
                    + ": "
                    + serviceSummary(service),
                ecsService.desiredCount() >= minimumCount
            );
            assertTrue(
                "Expected running count for ECS service "
                    + service.serviceName()
                    + " to be at least "
                    + minimumCount
                    + ": "
                    + serviceSummary(service),
                ecsService.runningCount() >= minimumCount
            );
            assertTrue(
                "Expected RUNNING task count for ECS service "
                    + service.serviceName()
                    + " to be at least "
                    + minimumCount
                    + ": "
                    + serviceSummary(service),
                runningTasks.size() >= minimumCount
            );
        });
    }

    private void waitForServiceTasksHealthy(StartedService service, int expectedCount) {
        List<Task> runningTasks = runningServiceTasks(service);
        assertEquals("Expected healthy task count check to see all running service tasks", expectedCount, runningTasks.size());
        for (Task task : runningTasks) {
            waitForHttp(startedTaskFromRunningTask(service.role(), task));
        }
    }

    private void waitForServiceTasksHealthyAtLeast(StartedService service, int minimumCount) {
        List<Task> runningTasks = runningServiceTasks(service);
        assertTrue(
            "Expected healthy task count check to see at least " + minimumCount + " running service tasks",
            runningTasks.size() >= minimumCount
        );
        for (Task task : runningTasks) {
            waitForHttp(startedTaskFromRunningTask(service.role(), task));
        }
    }

    private void waitForModelServiceHashRingAtMost(int maximumCount) {
        StartedService modelService = service(TimeSeriesSettings.MODEL_ROLE);
        Awaitility.await().atMost(Duration.ofMinutes(5)).pollInterval(Duration.ofSeconds(5)).untilAsserted(() -> {
            syncModelServiceCloudMapInstances();
            List<String> runningModelIps = runningServicePrivateIps(modelService);
            assertTrue("Expected at most " + maximumCount + " running model service task IPs", runningModelIps.size() <= maximumCount);
            Set<String> expectedIps = new HashSet<>(runningModelIps);
            Set<String> actualIps = new HashSet<>(latestHashRingTasks());
            assertEquals(
                "Expected hash-ring revision for " + cloudMapServiceName + " to match running model service task IPs",
                expectedIps,
                actualIps
            );
        });
    }

    private void waitForModelServiceHashRing(int expectedCount) {
        StartedService modelService = service(TimeSeriesSettings.MODEL_ROLE);
        Awaitility.await().atMost(Duration.ofMinutes(5)).pollInterval(Duration.ofSeconds(5)).untilAsserted(() -> {
            syncModelServiceCloudMapInstances();
            List<String> runningModelIps = runningServicePrivateIps(modelService);
            assertEquals("Expected running model service task IP count", expectedCount, runningModelIps.size());
            Set<String> expectedIps = new HashSet<>(runningModelIps);
            Set<String> actualIps = new HashSet<>(latestHashRingTasks());
            assertEquals(
                "Expected hash-ring revision for " + cloudMapServiceName + " to match running model service task IPs",
                expectedIps,
                actualIps
            );
        });
    }

    private void waitForModelServiceHashRingAtLeast(int minimumCount) {
        StartedService modelService = service(TimeSeriesSettings.MODEL_ROLE);
        Awaitility.await().atMost(Duration.ofMinutes(5)).pollInterval(Duration.ofSeconds(5)).untilAsserted(() -> {
            syncModelServiceCloudMapInstances();
            List<String> runningModelIps = runningServicePrivateIps(modelService);
            assertTrue("Expected at least " + minimumCount + " running model service task IPs", runningModelIps.size() >= minimumCount);
            Set<String> expectedIps = new HashSet<>(runningModelIps);
            Set<String> actualIps = new HashSet<>(latestHashRingTasks());
            assertEquals(
                "Expected hash-ring revision for " + cloudMapServiceName + " to match running model service task IPs",
                expectedIps,
                actualIps
            );
        });
    }

    private void refreshServiceTaskReferences() {
        for (StartedService service : services) {
            refreshServiceTaskReference(service);
        }
    }

    private void refreshServiceTaskReference(StartedService service) {
        StartedTask replacement = waitForServiceTask(service);
        replaceTask(replacement);
        waitForHttp(replacement);
    }

    private void replaceTask(StartedTask replacement) {
        for (int i = 0; i < tasks.size(); i++) {
            if (tasks.get(i).role().equals(replacement.role())) {
                tasks.set(i, replacement);
                return;
            }
        }
        tasks.add(replacement);
    }

    private StartedService service(String role) {
        return services
            .stream()
            .filter(service -> service.role().equals(role))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Missing ECS service for role " + role));
    }

    private software.amazon.awssdk.services.ecs.model.Service describeService(StartedService service) {
        try {
            var response = ecsClient
                .describeServices(DescribeServicesRequest.builder().cluster(ecsCluster).services(service.serviceName()).build());
            assertTrue(
                "Failed to describe ECS service " + service.serviceName() + ": " + response.failures(),
                response.failures().isEmpty()
            );
            assertEquals("Expected exactly one ECS service description for " + service.serviceName(), 1, response.services().size());
            return response.services().get(0);
        } catch (SdkException e) {
            throw new AssertionError("Transient AWS client failure describing ECS service " + service.serviceName(), e);
        }
    }

    private List<Task> runningServiceTasks(StartedService service) {
        try {
            List<String> taskArns = listServiceTaskArns(service.serviceName());
            if (taskArns.isEmpty()) {
                return List.of();
            }
            return ecsClient
                .describeTasks(DescribeTasksRequest.builder().cluster(ecsCluster).tasks(taskArns).build())
                .tasks()
                .stream()
                .filter(task -> "RUNNING".equals(task.lastStatus()))
                .toList();
        } catch (SdkException e) {
            throw new AssertionError("Transient AWS client failure listing ECS service tasks for " + service.serviceName(), e);
        }
    }

    private List<String> runningServicePrivateIps(StartedService service) {
        return runningServiceTasks(service)
            .stream()
            .map(task -> eniDetail(task, "privateIPv4Address"))
            .filter(ip -> ip != null && ip.isBlank() == false)
            .toList();
    }

    private List<String> listServiceTaskArns(String serviceName) {
        try {
            List<String> taskArns = new ArrayList<>();
            String nextToken = null;
            do {
                var response = ecsClient
                    .listTasks(
                        ListTasksRequest
                            .builder()
                            .cluster(ecsCluster)
                            .serviceName(serviceName)
                            .desiredStatus(DesiredStatus.RUNNING)
                            .nextToken(nextToken)
                            .build()
                    );
                taskArns.addAll(response.taskArns());
                nextToken = response.nextToken();
            } while (nextToken != null && nextToken.isBlank() == false);
            return taskArns;
        } catch (SdkException e) {
            throw new AssertionError("Transient AWS client failure listing ECS service tasks for " + serviceName, e);
        }
    }

    private StartedTask runTask(
        String role,
        List<String> subnets,
        List<String> securityGroups,
        AssignPublicIp assignPublicIp,
        LaunchType launchType
    ) {
        ContainerOverride containerOverride = ContainerOverride.builder().name(containerName).environment(environmentFor(role)).build();

        RunTaskRequest request = RunTaskRequest
            .builder()
            .cluster(ecsCluster)
            .taskDefinition(taskDefinition)
            .launchType(launchType)
            .startedBy(runId)
            .overrides(TaskOverride.builder().containerOverrides(containerOverride).build())
            .networkConfiguration(
                NetworkConfiguration
                    .builder()
                    .awsvpcConfiguration(
                        AwsVpcConfiguration.builder().subnets(subnets).securityGroups(securityGroups).assignPublicIp(assignPublicIp).build()
                    )
                    .build()
            )
            .build();

        RunTaskResponse response = ecsClient.runTask(request);
        assertTrue("ECS RunTask failures: " + response.failures(), response.failures().isEmpty());
        assertEquals("Expected exactly one ECS task for role " + role, 1, response.tasks().size());
        Task runningTask = waitForRunningTask(response.tasks().get(0).taskArn(), role);
        return startedTaskFromRunningTask(role, runningTask);
    }

    private StartedTask startedTaskFromRunningTask(String role, Task runningTask) {
        String eniId = eniDetail(runningTask, "networkInterfaceId");
        String privateIp = eniDetail(runningTask, "privateIPv4Address");
        assertNotNull("Expected private IP for ECS task " + runningTask.taskArn(), privateIp);
        String publicIp = waitForPublicIpIfNeeded(eniId);
        String endpointIp = "private".equals(endpointAddressType) ? privateIp : publicIp;
        return new StartedTask(
            role,
            runningTask.taskArn(),
            eniId,
            privateIp,
            publicIp,
            protocol + "://" + endpointIp + ":" + opensearchPort
        );
    }

    private String waitForPublicIpIfNeeded(String eniId) {
        if ("private".equals(endpointAddressType)) {
            return null;
        }
        final String[] publicIp = new String[1];
        Awaitility.await().atMost(Duration.ofMinutes(2)).pollInterval(Duration.ofSeconds(2)).ignoreExceptions().untilAsserted(() -> {
            publicIp[0] = publicIp(eniId);
            assertTrue("Expected public IP for ECS task ENI " + eniId, publicIp[0] != null && publicIp[0].isBlank() == false);
        });
        return publicIp[0];
    }

    private Task waitForRunningTask(String taskArn, String role) {
        final Task[] found = new Task[1];
        Awaitility.await().atMost(Duration.ofMinutes(5)).pollInterval(Duration.ofSeconds(5)).untilAsserted(() -> {
            var response = ecsClient.describeTasks(DescribeTasksRequest.builder().cluster(ecsCluster).tasks(taskArn).build());
            assertFalse("ECS task disappeared: " + taskArn, response.tasks().isEmpty());
            Task task = response.tasks().get(0);
            assertEquals("ECS task did not reach RUNNING for role " + role + ": " + task.stoppedReason(), "RUNNING", task.lastStatus());
            found[0] = task;
        });
        return found[0];
    }

    private String registerRoleTaskDefinition(String role, String serviceName) {
        TaskDefinition base = ecsClient
            .describeTaskDefinition(DescribeTaskDefinitionRequest.builder().taskDefinition(taskDefinition).build())
            .taskDefinition();
        List<ContainerDefinition> roleContainers = base.containerDefinitions().stream().map(container -> {
            if (container.name().equals(containerName) == false) {
                return container;
            }
            Map<String, String> env = new LinkedHashMap<>();
            if (container.environment() != null) {
                container.environment().forEach(pair -> env.put(pair.name(), pair.value()));
            }
            environmentFor(role, serviceName).forEach(pair -> env.put(pair.name(), pair.value()));
            return container
                .toBuilder()
                .environment(
                    env
                        .entrySet()
                        .stream()
                        .map(entry -> KeyValuePair.builder().name(entry.getKey()).value(entry.getValue()).build())
                        .toList()
                )
                .build();
        }).toList();

        RegisterTaskDefinitionRequest.Builder request = RegisterTaskDefinitionRequest
            .builder()
            .family(runId + "-" + role)
            .networkMode(base.networkMode())
            .containerDefinitions(roleContainers)
            .cpu(base.cpu())
            .memory(base.memory());
        if (base.taskRoleArn() != null && base.taskRoleArn().isBlank() == false) {
            request.taskRoleArn(base.taskRoleArn());
        }
        if (base.executionRoleArn() != null && base.executionRoleArn().isBlank() == false) {
            request.executionRoleArn(base.executionRoleArn());
        }
        if (base.hasRequiresCompatibilities()) {
            request.requiresCompatibilities(base.requiresCompatibilities());
        }
        if (base.hasVolumes()) {
            request.volumes(base.volumes());
        }
        if (base.hasPlacementConstraints()) {
            request.placementConstraints(base.placementConstraints());
        }
        if (base.runtimePlatform() != null) {
            request.runtimePlatform(base.runtimePlatform());
        }
        if (base.proxyConfiguration() != null) {
            request.proxyConfiguration(base.proxyConfiguration());
        }
        if (base.hasInferenceAccelerators()) {
            request.inferenceAccelerators(base.inferenceAccelerators());
        }
        if (base.pidMode() != null) {
            request.pidMode(base.pidMode());
        }
        if (base.ipcMode() != null) {
            request.ipcMode(base.ipcMode());
        }
        if (base.ephemeralStorage() != null) {
            request.ephemeralStorage(base.ephemeralStorage());
        }
        if (base.enableFaultInjection() != null) {
            request.enableFaultInjection(base.enableFaultInjection());
        }

        String arn = ecsClient.registerTaskDefinition(request.build()).taskDefinition().taskDefinitionArn();
        registeredTaskDefinitionArns.add(arn);
        return arn;
    }

    private List<String> configureAutoScalingPolicies(String role, String serviceName, String resourceId) {
        int minCapacity = autoScalingMinCapacity();
        int maxCapacity = autoScalingMaxCapacity();
        autoScalingClient
            .registerScalableTarget(
                RegisterScalableTargetRequest
                    .builder()
                    .serviceNamespace(ECS_SERVICE_NAMESPACE)
                    .scalableDimension(ECS_SERVICE_DESIRED_COUNT)
                    .resourceId(resourceId)
                    .minCapacity(minCapacity)
                    .maxCapacity(maxCapacity)
                    .tags(Map.of("ad-integ-test-run", runId))
                    .build()
            );

        List<String> policyNames = new ArrayList<>();
        if (TimeSeriesSettings.MODEL_ROLE.equals(role)) {
            configureModelAutoScalingPolicies(role, serviceName, resourceId, policyNames);
        } else if (TimeSeriesSettings.COORDINATOR_ROLE.equals(role)) {
            configureCoordinatorAutoScalingPolicies(serviceName, resourceId, policyNames);
        }
        return policyNames;
    }

    private void configureModelAutoScalingPolicies(String role, String serviceName, String resourceId, List<String> policyNames) {
        String targetTrackingPolicyName = serviceName + "-heap-target-tracking";
        autoScalingClient
            .putScalingPolicy(
                PutScalingPolicyRequest
                    .builder()
                    .serviceNamespace(ECS_SERVICE_NAMESPACE)
                    .scalableDimension(ECS_SERVICE_DESIRED_COUNT)
                    .resourceId(resourceId)
                    .policyName(targetTrackingPolicyName)
                    .policyType(PolicyType.TARGET_TRACKING_SCALING)
                    .targetTrackingScalingPolicyConfiguration(
                        TargetTrackingScalingPolicyConfiguration
                            .builder()
                            .targetValue(autoScalingTargetHeapPercent())
                            .disableScaleIn(false)
                            .scaleInCooldown(autoScalingScaleInCooldown())
                            .scaleOutCooldown(autoScalingScaleOutCooldown())
                            .customizedMetricSpecification(targetTrackingHeapMetric(role, serviceName))
                            .build()
                    )
                    .build()
            );
        policyNames.add(targetTrackingPolicyName);

        if (Boolean.parseBoolean(property(ECS_PREDICTIVE_SCALING_ENABLED_PROPERTY, "true"))) {
            String predictivePolicyName = serviceName + "-heap-predictive";
            autoScalingClient
                .putScalingPolicy(
                    PutScalingPolicyRequest
                        .builder()
                        .serviceNamespace(ECS_SERVICE_NAMESPACE)
                        .scalableDimension(ECS_SERVICE_DESIRED_COUNT)
                        .resourceId(resourceId)
                        .policyName(predictivePolicyName)
                        .policyType(PolicyType.PREDICTIVE_SCALING)
                        .predictiveScalingPolicyConfiguration(predictiveScalingPolicy(role, serviceName))
                        .build()
                );
            policyNames.add(predictivePolicyName);
        }
    }

    private void configureCoordinatorAutoScalingPolicies(String serviceName, String resourceId, List<String> policyNames) {
        String targetTrackingPolicyName = serviceName + "-sqs-backlog-target-tracking";
        autoScalingClient
            .putScalingPolicy(
                PutScalingPolicyRequest
                    .builder()
                    .serviceNamespace(ECS_SERVICE_NAMESPACE)
                    .scalableDimension(ECS_SERVICE_DESIRED_COUNT)
                    .resourceId(resourceId)
                    .policyName(targetTrackingPolicyName)
                    .policyType(PolicyType.TARGET_TRACKING_SCALING)
                    .targetTrackingScalingPolicyConfiguration(
                        TargetTrackingScalingPolicyConfiguration
                            .builder()
                            .targetValue(autoScalingTargetCoordinatorSqsRatio())
                            .disableScaleIn(false)
                            .scaleInCooldown(autoScalingScaleInCooldown())
                            .scaleOutCooldown(autoScalingScaleOutCooldown())
                            .customizedMetricSpecification(targetTrackingCoordinatorSqsMetric(serviceName))
                            .build()
                    )
                    .build()
            );
        policyNames.add(targetTrackingPolicyName);
    }

    private void registerScalableTargetCapacity(StartedService service, int minCapacity, int maxCapacity) {
        autoScalingClient
            .registerScalableTarget(
                RegisterScalableTargetRequest
                    .builder()
                    .serviceNamespace(ECS_SERVICE_NAMESPACE)
                    .scalableDimension(ECS_SERVICE_DESIRED_COUNT)
                    .resourceId(service.resourceId())
                    .minCapacity(minCapacity)
                    .maxCapacity(maxCapacity)
                    .build()
            );
    }

    private CustomizedMetricSpecification targetTrackingHeapMetric(String role, String serviceName) {
        return CustomizedMetricSpecification
            .builder()
            .namespace(autoScalingMetricNamespace())
            .metricName(HeapUsageMetricPublisher.HEAP_USED_PERCENT_METRIC)
            .statistic(MetricStatistic.AVERAGE)
            .unit("Percent")
            .dimensions(targetTrackingDimensions(role, serviceName))
            .build();
    }

    private List<MetricDimension> targetTrackingDimensions(String role, String serviceName) {
        return List
            .of(
                MetricDimension.builder().name(HeapUsageMetricPublisher.CLUSTER_NAME_DIMENSION).value(ecsClusterName()).build(),
                MetricDimension.builder().name(HeapUsageMetricPublisher.SERVICE_NAME_DIMENSION).value(serviceName).build(),
                MetricDimension.builder().name(HeapUsageMetricPublisher.ROLE_DIMENSION).value(role).build()
            );
    }

    private CustomizedMetricSpecification targetTrackingCoordinatorSqsMetric(String serviceName) {
        return CustomizedMetricSpecification
            .builder()
            .namespace(autoScalingMetricNamespace())
            .metricName(SqsMetricPublisher.TOTAL_SQS_BACKLOG_DELETED_PRESSURE_METRIC)
            .statistic(MetricStatistic.AVERAGE)
            .unit("None")
            .dimensions(targetTrackingDimensions(TimeSeriesSettings.COORDINATOR_ROLE, serviceName))
            .build();
    }

    private PredictiveScalingPolicyConfiguration predictiveScalingPolicy(String role, String serviceName) {
        return PredictiveScalingPolicyConfiguration
            .builder()
            .mode(PredictiveScalingMode.fromValue(property(ECS_PREDICTIVE_SCALING_MODE_PROPERTY, "ForecastAndScale")))
            .schedulingBufferTime(Integer.parseInt(property(ECS_PREDICTIVE_SCALING_BUFFER_SECONDS_PROPERTY, "900")))
            .metricSpecifications(
                PredictiveScalingMetricSpecification
                    .builder()
                    .targetValue(autoScalingTargetHeapPercent())
                    .customizedScalingMetricSpecification(
                        predictiveMetric(
                            "scaling",
                            HeapUsageMetricPublisher.HEAP_USED_PERCENT_METRIC,
                            "Average",
                            "Percent",
                            role,
                            serviceName
                        )
                    )
                    .customizedLoadMetricSpecification(
                        predictiveMetric("load", HeapUsageMetricPublisher.HEAP_USED_BYTES_METRIC, "Sum", "Bytes", role, serviceName)
                    )
                    .build()
            )
            .build();
    }

    private PredictiveScalingCustomizedMetricSpecification predictiveMetric(
        String id,
        String metricName,
        String statistic,
        String unit,
        String role,
        String serviceName
    ) {
        return PredictiveScalingCustomizedMetricSpecification
            .builder()
            .metricDataQueries(
                PredictiveScalingMetricDataQuery
                    .builder()
                    .id(id)
                    .returnData(true)
                    .metricStat(
                        PredictiveScalingMetricStat
                            .builder()
                            .stat(statistic)
                            .unit(unit)
                            .metric(
                                PredictiveScalingMetric
                                    .builder()
                                    .namespace(autoScalingMetricNamespace())
                                    .metricName(metricName)
                                    .dimensions(predictiveDimensions(role, serviceName))
                                    .build()
                            )
                            .build()
                    )
                    .build()
            )
            .build();
    }

    private List<PredictiveScalingMetricDimension> predictiveDimensions(String role, String serviceName) {
        return List
            .of(
                PredictiveScalingMetricDimension
                    .builder()
                    .name(HeapUsageMetricPublisher.CLUSTER_NAME_DIMENSION)
                    .value(ecsClusterName())
                    .build(),
                PredictiveScalingMetricDimension.builder().name(HeapUsageMetricPublisher.SERVICE_NAME_DIMENSION).value(serviceName).build(),
                PredictiveScalingMetricDimension.builder().name(HeapUsageMetricPublisher.ROLE_DIMENSION).value(role).build()
            );
    }

    private List<KeyValuePair> environmentFor(String role) {
        return environmentFor(role, "");
    }

    private List<KeyValuePair> environmentFor(String role, String serviceName) {
        Map<String, String> env = new LinkedHashMap<>();
        env.put("cluster.name", runId + "-" + role);
        env.put("node.name", runId + "-" + role);
        env.put("discovery.type", "single-node");
        env.put("network.host", "0.0.0.0");
        env.put("http.host", "0.0.0.0");
        env.put("http.port", Integer.toString(opensearchPort));
        env.put("plugins.timeseries.node.roles", role);
        env.put("plugins.anomaly_detection.multi_tenancy.enabled", "true");
        env.put("plugins.anomaly_detection.event_bridge_cell_id_from_header", "true");
        env.put("plugins.timeseries.cloud_map_ttl", "1s");
        env.put("plugins.timeseries.cluster_membership_reader_ttl", "1s");
        env.put("AWS_REGION", region.id());
        env.put("AWS_DEFAULT_REGION", region.id());
        env.put("OPENSEARCH_JAVA_OPTS", javaOptsFor(role));
        env.put("DISABLE_INSTALL_DEMO_CONFIG", "true");
        env.put("DISABLE_SECURITY_PLUGIN", "true");
        if (adPluginUrl != null && adPluginUrl.isBlank() == false) {
            env.put("AD_PLUGIN_URL", adPluginUrl);
        }
        if (jobSchedulerPluginUrl != null && jobSchedulerPluginUrl.isBlank() == false) {
            env.put("JOB_SCHEDULER_PLUGIN_URL", jobSchedulerPluginUrl);
        }

        addOpenSearchSetting(env, "plugins.timeseries.region");
        addOpenSearchSetting(env, "plugins.timeseries.cloud_map_namespace");
        addOpenSearchSetting(env, "plugins.timeseries.cloud_map_service");
        addOpenSearchSetting(env, "plugins.timeseries.cloud_map_table_name");
        addOpenSearchSetting(env, "plugins.timeseries.opensearch_port");
        addOpenSearchSetting(env, "plugins.timeseries.sqs.account_ids");
        addOpenSearchSetting(env, "plugins.timeseries.internal_api_shared_secret");
        addOpenSearchSetting(env, "plugins.timeseries.dataplane.endpoint");
        addOpenSearchSetting(env, "plugins.timeseries.api_dataplane.endpoint");
        addOpenSearchSetting(env, "plugins.timeseries.dataplane.endpoint.context_key");
        addOpenSearchSetting(env, "plugins.anomaly_detection.remote_metadata_endpoint");
        addOpenSearchSetting(env, "plugins.anomaly_detection.remote_metadata_service_name");
        addOpenSearchSetting(env, "plugins.anomaly_detection.s3_checkpoint_bucket");
        addOpenSearchSetting(env, "plugins.anomaly_detection.scheduler_group");
        addOpenSearchSetting(env, "plugins.anomaly_detection.sqs.queue_name");
        addOpenSearchSetting(env, "plugins.anomaly_detection.sqs.extra_queue_names");
        addOpenSearchSetting(env, "plugins.anomaly_detection.event_bridge.schedule_management_role_name");
        addOpenSearchSetting(env, "plugins.anomaly_detection.event_bridge.sqs_delivery_role_name");
        addOpenSearchSetting(env, "plugins.anomaly_detection.checkpoint_store_factory_class");
        addOpenSearchSetting(env, "plugins.anomaly_detection.config_document_store_factory_class");
        addOpenSearchSetting(env, "plugins.anomaly_detection.data_source_endpoint_resolver_factory_class");
        addOpenSearchSetting(env, "plugins.anomaly_detection.api_data_source_endpoint_resolver_factory_class");
        addOpenSearchSetting(env, "plugins.anomaly_detection.model_max_size_percent");
        addOpenSearchSetting(env, "plugins.anomaly_detection.dedicated_cache_size");
        addOpenSearchSetting(env, "plugins.anomaly_detection.max_model_size_per_node");

        if (serviceName != null && serviceName.isBlank() == false) {
            if (TimeSeriesSettings.MODEL_ROLE.equals(role)) {
                env.put("plugins.timeseries.cloudwatch.heap_metrics.enabled", "true");
                env.put("plugins.timeseries.cloudwatch.metrics.namespace", autoScalingMetricNamespace());
                env.put("plugins.timeseries.cloudwatch.metrics.cluster_name", ecsClusterName());
                env.put("plugins.timeseries.cloudwatch.metrics.service_name", serviceName);
                env.put("plugins.timeseries.cloudwatch.heap_metrics.interval", property(ECS_METRIC_INTERVAL_PROPERTY, "1m"));
            } else if (TimeSeriesSettings.MASTER_ROLE.equals(role) && autoScalingEnabled()) {
                env.put("plugins.timeseries.cloudwatch.sqs_metrics.enabled", "true");
                env.put("plugins.timeseries.cloudwatch.metrics.namespace", autoScalingMetricNamespace());
                env.put("plugins.timeseries.cloudwatch.metrics.cluster_name", ecsClusterName());
                env.put("plugins.timeseries.cloudwatch.metrics.service_name", ecsServiceName(TimeSeriesSettings.COORDINATOR_ROLE));
                env.put("plugins.timeseries.cloudwatch.sqs_metrics.interval", property(ECS_METRIC_INTERVAL_PROPERTY, "1m"));
            }
        }

        if (propagateAwsCredentials) {
            addAwsCredentials(env);
        }

        return env.entrySet().stream().map(entry -> KeyValuePair.builder().name(entry.getKey()).value(entry.getValue()).build()).toList();
    }

    private String ecsServiceName(String role) {
        return runId + "-" + role;
    }

    private String javaOptsFor(String role) {
        String defaultJavaOpts = property(ECS_OPENSEARCH_JAVA_OPTS_PROPERTY, "-Xms1g -Xmx1g");
        if (TimeSeriesSettings.MODEL_ROLE.equals(role)) {
            return property(ECS_MODEL_OPENSEARCH_JAVA_OPTS_PROPERTY, defaultJavaOpts);
        }
        return defaultJavaOpts;
    }

    private String autoScalingResourceId(String serviceName) {
        return "service/" + ecsClusterName() + "/" + serviceName;
    }

    private String ecsClusterName() {
        int slash = ecsCluster.lastIndexOf('/');
        return slash >= 0 ? ecsCluster.substring(slash + 1) : ecsCluster;
    }

    private int autoScalingMinCapacity() {
        return Integer.parseInt(property(ECS_AUTO_SCALING_MIN_CAPACITY_PROPERTY, "1"));
    }

    private int autoScalingMaxCapacity() {
        return Integer.parseInt(property(ECS_AUTO_SCALING_MAX_CAPACITY_PROPERTY, "3"));
    }

    private double autoScalingTargetHeapPercent() {
        return Double.parseDouble(property(ECS_AUTO_SCALING_TARGET_HEAP_PERCENT_PROPERTY, "60"));
    }

    private double autoScalingTargetCoordinatorSqsRatio() {
        String target = property(ECS_AUTO_SCALING_COORDINATOR_TARGET_BACKLOG_DELETED_RATIO_PROPERTY, "");
        if (target.isBlank()) {
            target = property(ECS_AUTO_SCALING_COORDINATOR_TARGET_DELETED_ARRIVED_RATIO_PROPERTY, "1");
        }
        return Double.parseDouble(target);
    }

    private int autoScalingScaleInCooldown() {
        return Integer.parseInt(property(ECS_AUTO_SCALING_SCALE_IN_COOLDOWN_PROPERTY, "300"));
    }

    private int autoScalingScaleOutCooldown() {
        return Integer.parseInt(property(ECS_AUTO_SCALING_SCALE_OUT_COOLDOWN_PROPERTY, "60"));
    }

    private long autoScalingVerifyScaleInTimeoutMinutes() {
        return Long.parseLong(property(ECS_AUTO_SCALING_VERIFY_SCALE_IN_TIMEOUT_MINUTES_PROPERTY, "75"));
    }

    private String autoScalingMetricNamespace() {
        return property(ECS_AUTO_SCALING_METRIC_NAMESPACE_PROPERTY, "OpenSearch/AnomalyDetection");
    }

    private void addOpenSearchSetting(Map<String, String> env, String settingKey) {
        String value = System.getProperty("tests.opensearch." + settingKey);
        if (value != null && value.isBlank() == false) {
            if ("plugins.timeseries.sqs.account_ids".equals(settingKey)) {
                value = normalizeListSetting(value);
            }
            env.put(settingKey, value);
        }
    }

    private String normalizeListSetting(String value) {
        String normalized = value.trim();
        if (normalized.startsWith("[") && normalized.endsWith("]")) {
            normalized = normalized.substring(1, normalized.length() - 1);
        }
        return java.util.Arrays
            .stream(normalized.split(","))
            .map(String::trim)
            .map(item -> item.replace("\"", ""))
            .filter(item -> item.isBlank() == false)
            .collect(java.util.stream.Collectors.joining(","));
    }

    private void addAwsCredentials(Map<String, String> env) {
        AwsCredentials credentials = SecurityUtil.createCredentialsProvider().resolveCredentials();
        env.put("AWS_ACCESS_KEY_ID", credentials.accessKeyId());
        env.put("AWS_SECRET_ACCESS_KEY", credentials.secretAccessKey());
        if (credentials instanceof AwsSessionCredentials sessionCredentials) {
            env.put("AWS_SESSION_TOKEN", sessionCredentials.sessionToken());
        }
    }

    private void registerModelTaskInCloudMap() {
        String registeredModelInstanceId = runId + "-model";
        registerModelInstance(registeredModelInstanceId, modelPrivateIp());
    }

    private void startModelCloudMapSync() {
        if (modelServiceName == null || modelServiceName.isBlank()) {
            return;
        }
        modelCloudMapSyncExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "ad-it-model-cloud-map-sync-" + runId);
            thread.setDaemon(true);
            return thread;
        });
        modelCloudMapSyncExecutor.scheduleWithFixedDelay(this::syncModelServiceCloudMapInstancesSafely, 30, 30, TimeUnit.SECONDS);
    }

    private void syncModelServiceCloudMapInstancesSafely() {
        try {
            syncModelServiceCloudMapInstances();
        } catch (Exception ignored) {
            // Best-effort background sync for scaled model service tasks.
        }
    }

    private void syncModelServiceCloudMapInstances() {
        if (modelServiceName == null || modelServiceName.isBlank()) {
            return;
        }
        List<String> taskArns = listServiceTaskArns(modelServiceName);
        Map<String, String> currentInstances = new LinkedHashMap<>();
        if (taskArns.isEmpty() == false) {
            var response = ecsClient.describeTasks(DescribeTasksRequest.builder().cluster(ecsCluster).tasks(taskArns).build());
            for (Task task : response.tasks()) {
                if ("RUNNING".equals(task.lastStatus()) == false) {
                    continue;
                }
                String privateIp = eniDetail(task, "privateIPv4Address");
                if (privateIp == null || privateIp.isBlank()) {
                    continue;
                }
                String instanceId = cloudMapInstanceId(task.taskArn());
                currentInstances.put(instanceId, privateIp);
                if (privateIp.equals(registeredModelInstances.get(instanceId)) == false) {
                    registerModelInstance(instanceId, privateIp);
                }
            }
        }

        for (String existingInstanceId : new HashSet<>(registeredModelInstances.keySet())) {
            if (currentInstances.containsKey(existingInstanceId) == false) {
                deregisterModelInstance(existingInstanceId);
            }
        }
    }

    private String cloudMapInstanceId(String taskArn) {
        int slash = taskArn.lastIndexOf('/');
        String taskId = slash >= 0 ? taskArn.substring(slash + 1) : taskArn;
        return runId + "-model-" + taskId.replace(':', '-');
    }

    private void registerModelInstance(String instanceId, String privateIp) {
        waitOperation(
            serviceDiscoveryClient
                .registerInstance(
                    RegisterInstanceRequest
                        .builder()
                        .serviceId(cloudMapServiceId)
                        .instanceId(instanceId)
                        .attributes(Map.of("AWS_INSTANCE_IPV4", privateIp, "AWS_INSTANCE_PORT", Integer.toString(opensearchPort)))
                        .build()
                )
                .operationId()
        );
        registeredModelInstances.put(instanceId, privateIp);
        try {
            serviceDiscoveryClient
                .updateInstanceCustomHealthStatus(
                    UpdateInstanceCustomHealthStatusRequest
                        .builder()
                        .serviceId(cloudMapServiceId)
                        .instanceId(instanceId)
                        .status(CustomHealthStatus.HEALTHY)
                        .build()
                );
        } catch (Exception ignored) {
            // Existing services without custom health checks do not need this call.
        }
    }

    private void deregisterModelInstance(String instanceId) {
        try {
            waitOperation(
                serviceDiscoveryClient
                    .deregisterInstance(DeregisterInstanceRequest.builder().serviceId(cloudMapServiceId).instanceId(instanceId).build())
                    .operationId()
            );
        } catch (Exception ignored) {
            // Best-effort cleanup.
        } finally {
            registeredModelInstances.remove(instanceId);
        }
    }

    private void waitOperation(String operationId) {
        waitOperation(serviceDiscoveryClient, operationId);
    }

    private void publishRestClusterProperties() {
        System.setProperty(MASTER_CLUSTER_PROPERTY, restClusterEndpoint(task(TimeSeriesSettings.MASTER_ROLE)));
        System.setProperty(COORDINATOR_CLUSTER_PROPERTY, restClusterEndpoint(task(TimeSeriesSettings.COORDINATOR_ROLE)));
        System.setProperty(MODEL_CLUSTER_PROPERTY, restClusterEndpoint(task(TimeSeriesSettings.MODEL_ROLE)));
    }

    private String restClusterEndpoint(StartedTask task) {
        int schemeSeparator = task.endpoint().indexOf("://");
        return schemeSeparator < 0 ? task.endpoint() : task.endpoint().substring(schemeSeparator + 3);
    }

    private StartedTask task(String role) {
        return tasks
            .stream()
            .filter(task -> task.role().equals(role))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Missing ECS task for role " + role));
    }

    private String eniDetail(Task task, String detailName) {
        return task
            .attachments()
            .stream()
            .filter(attachment -> "ElasticNetworkInterface".equals(attachment.type()))
            .flatMap(attachment -> attachment.details().stream())
            .filter(detail -> detailName.equals(detail.name()))
            .map(detail -> detail.value())
            .findFirst()
            .orElse(null);
    }

    private String publicIp(String eniId) {
        if (eniId == null || eniId.isBlank()) {
            return null;
        }
        var response = ec2Client.describeNetworkInterfaces(DescribeNetworkInterfacesRequest.builder().networkInterfaceIds(eniId).build());
        if (response.networkInterfaces().isEmpty() || response.networkInterfaces().get(0).association() == null) {
            return null;
        }
        return response.networkInterfaces().get(0).association().publicIp();
    }

    private NetworkResources resolveNetworkResources() {
        List<String> configuredSubnets = splitCsv(property(ECS_SUBNETS_PROPERTY, ""));
        List<String> subnets = configuredSubnets.isEmpty() ? defaultVpcSubnets() : configuredSubnets;
        List<String> configuredSecurityGroups = splitCsv(property(ECS_SECURITY_GROUPS_PROPERTY, ""));
        if (configuredSecurityGroups.isEmpty() == false) {
            return new NetworkResources(subnets, configuredSecurityGroups);
        }
        String vpcId = defaultVpcId();
        String securityGroupId = createSecurityGroup(vpcId);
        createdSecurityGroupId = securityGroupId;
        return new NetworkResources(subnets, List.of(securityGroupId));
    }

    private String defaultVpcId() {
        var vpcs = ec2Client
            .describeVpcs(DescribeVpcsRequest.builder().filters(Filter.builder().name("is-default").values("true").build()).build())
            .vpcs();
        if (vpcs.isEmpty()) {
            throw new IllegalStateException(
                "Set " + ECS_SUBNETS_PROPERTY + " and " + ECS_SECURITY_GROUPS_PROPERTY + "; no default VPC exists"
            );
        }
        return vpcs.get(0).vpcId();
    }

    private List<String> defaultVpcSubnets() {
        String vpcId = defaultVpcId();
        List<String> subnets = ec2Client
            .describeSubnets(DescribeSubnetsRequest.builder().filters(Filter.builder().name("vpc-id").values(vpcId).build()).build())
            .subnets()
            .stream()
            .map(subnet -> subnet.subnetId())
            .toList();
        if (subnets.isEmpty()) {
            throw new IllegalStateException("Set " + ECS_SUBNETS_PROPERTY + "; no default VPC subnets exist");
        }
        return subnets;
    }

    private String createSecurityGroup(String vpcId) {
        String groupId = ec2Client
            .createSecurityGroup(
                CreateSecurityGroupRequest.builder().vpcId(vpcId).groupName(runId).description("Temporary AD ECS IT access").build()
            )
            .groupId();

        IpPermission localAccess = localAccessPermission("IT runner");
        IpPermission selfAccess = IpPermission
            .builder()
            .ipProtocol("-1")
            .userIdGroupPairs(UserIdGroupPair.builder().groupId(groupId).description("ECS task to task").build())
            .build();
        ec2Client
            .authorizeSecurityGroupIngress(
                AuthorizeSecurityGroupIngressRequest.builder().groupId(groupId).ipPermissions(localAccess, selfAccess).build()
            );
        return groupId;
    }

    private IpPermission localAccessPermission(String description) {
        return localAccessPermission(allowedCallerCidr(), description);
    }

    private IpPermission localAccessPermission(String cidr, String description) {
        return IpPermission
            .builder()
            .ipProtocol("tcp")
            .fromPort(opensearchPort)
            .toPort(opensearchPort)
            .ipRanges(IpRange.builder().cidrIp(cidr).description(description).build())
            .build();
    }

    private void refreshAllowedCallerIngress() {
        if ("private".equals(endpointAddressType) || createdSecurityGroupId == null || createdSecurityGroupId.isBlank()) {
            return;
        }
        String cidr = refreshAllowedCallerCidr();
        if (cidr == null || cidr.isBlank()) {
            return;
        }
        try {
            ec2Client
                .authorizeSecurityGroupIngress(
                    AuthorizeSecurityGroupIngressRequest
                        .builder()
                        .groupId(createdSecurityGroupId)
                        .ipPermissions(localAccessPermission(cidr, "IT runner refreshed"))
                        .build()
                );
        } catch (Ec2Exception e) {
            if ("InvalidPermission.Duplicate".equals(e.awsErrorDetails().errorCode())) {
                return;
            }
            throw e;
        } catch (SdkException e) {
            // Best-effort refresh only. The existing ingress rule may still be valid.
        }
    }

    private String refreshAllowedCallerCidr() {
        try {
            return allowedCallerCidr();
        } catch (RuntimeException e) {
            return lastAllowedCallerCidr;
        }
    }

    private String allowedCallerCidr() {
        String configured = property(ECS_ALLOWED_CIDR_PROPERTY, "auto").trim();
        if (configured.isBlank() || "auto".equalsIgnoreCase(configured)) {
            return rememberAllowedCallerCidr(publicCallerCidr());
        }
        if ("0.0.0.0/0".equals(configured) || "::/0".equals(configured)) {
            throw new IllegalArgumentException(
                ECS_ALLOWED_CIDR_PROPERTY + " must not open the temporary ECS task REST port to the world. Use 'auto' or a narrow CIDR."
            );
        }
        return rememberAllowedCallerCidr(configured);
    }

    private String rememberAllowedCallerCidr(String cidr) {
        lastAllowedCallerCidr = cidr;
        return cidr;
    }

    private String publicCallerCidr() {
        try {
            HttpURLConnection connection = (HttpURLConnection) URI.create("https://checkip.amazonaws.com").toURL().openConnection();
            connection.setConnectTimeout((int) Duration.ofSeconds(5).toMillis());
            connection.setReadTimeout((int) Duration.ofSeconds(5).toMillis());
            String ip = new String(connection.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).trim();
            if (ip.isBlank() == false) {
                return publicCallerCidr(ip);
            }
        } catch (Exception e) {
            throw new IllegalStateException(
                "Failed to auto-detect the IT runner public IP. Set "
                    + ECS_ALLOWED_CIDR_PROPERTY
                    + " or MULTI_TENANT_ECS_ALLOWED_CIDR to a narrow CIDR, or run with "
                    + ECS_ENDPOINT_ADDRESS_TYPE_PROPERTY
                    + "=private from inside the VPC.",
                e
            );
        }
        throw new IllegalStateException("Failed to auto-detect the IT runner public IP: empty response from checkip.amazonaws.com");
    }

    private String publicCallerCidr(String ip) {
        if (ip.contains(":")) {
            return ip + "/128";
        }
        String[] parts = ip.split("\\.");
        if (parts.length == 4) {
            return parts[0] + "." + parts[1] + "." + parts[2] + ".0/24";
        }
        return ip + "/32";
    }

    private void waitForHttp(StartedTask startedTask) {
        try {
            Awaitility.await().atMost(Duration.ofMinutes(8)).pollInterval(Duration.ofSeconds(5)).untilAsserted(() -> {
                refreshAllowedCallerIngress();
                Task currentTask = describeTask(startedTask.taskArn());
                if ("STOPPED".equals(currentTask.lastStatus())) {
                    throw new AssertionError(
                        "ECS task stopped before OpenSearch became healthy: " + taskSummary(currentTask) + "\n" + recentContainerLogs()
                    );
                }
                try {
                    int status = httpStatus(startedTask.endpoint() + "/_cluster/health?wait_for_status=yellow&timeout=1s");
                    assertTrue(
                        "Expected OpenSearch task endpoint " + startedTask.endpoint() + " to be healthy but got " + status,
                        status >= 200 && status < 300
                    );
                } catch (Exception e) {
                    throw new AssertionError("OpenSearch task endpoint " + startedTask.endpoint() + " is not healthy yet: " + e, e);
                }
            });
        } catch (ConditionTimeoutException e) {
            throw new AssertionError(
                "Timed out waiting for OpenSearch task endpoint "
                    + startedTask.endpoint()
                    + " to become healthy. "
                    + taskSummary(describeTask(startedTask.taskArn()))
                    + "\n"
                    + recentContainerLogs(),
                e
            );
        }
    }

    private boolean isOpenSearchEndpointHealthy(String endpoint) {
        try {
            int status = httpStatus(endpoint + "/_cluster/health?wait_for_status=yellow&timeout=1s");
            return status >= 200 && status < 300;
        } catch (Exception e) {
            return false;
        }
    }

    private Task describeTask(String taskArn) {
        var response = ecsClient.describeTasks(DescribeTasksRequest.builder().cluster(ecsCluster).tasks(taskArn).build());
        assertFalse("ECS task disappeared while describing " + taskArn + ": " + response.failures(), response.tasks().isEmpty());
        return response.tasks().get(0);
    }

    private String recentContainerLogs() {
        if (logGroupName == null || logGroupName.isBlank()) {
            return "";
        }
        try {
            List<FilteredLogEvent> events = new ArrayList<>();
            String nextToken = null;
            do {
                var response = logsClient
                    .filterLogEvents(FilterLogEventsRequest.builder().logGroupName(logGroupName).nextToken(nextToken).limit(1000).build());
                events.addAll(response.events());
                nextToken = response.nextToken();
            } while (nextToken != null && nextToken.isBlank() == false && events.size() < 5000);

            StringBuilder builder = new StringBuilder("CloudWatch logs from ").append(logGroupName).append(":\n");
            List<FilteredLogEvent> selectedEvents = new ArrayList<>();
            for (FilteredLogEvent event : events) {
                if (includeDiagnosticLog(event)) {
                    selectedEvents.add(event);
                }
            }
            int tailStart = Math.max(0, events.size() - 600);
            for (FilteredLogEvent event : events.subList(tailStart, events.size())) {
                if (selectedEvents.contains(event) == false) {
                    selectedEvents.add(event);
                }
            }
            selectedEvents.forEach(event -> {
                builder.append('[').append(event.logStreamName()).append("] ").append(redact(event.message())).append('\n');
            });
            return builder.toString();
        } catch (Exception e) {
            return "Failed to read CloudWatch logs from " + logGroupName + ": " + e;
        }
    }

    private boolean includeDiagnosticLog(FilteredLogEvent event) {
        String streamName = event.logStreamName() == null ? "" : event.logStreamName().toLowerCase(Locale.ROOT);
        String message = event.message() == null ? "" : event.message().toLowerCase(Locale.ROOT);
        return streamName.contains("coordinator")
            || message.contains("[error")
            || message.contains("exception")
            || message.contains("fail")
            || message.contains("detector")
            || message.contains("data-plane")
            || message.contains("single stream")
            || message.contains("realtime ")
            || message.contains("result write")
            || message.contains("result bulk")
            || message.contains("bulk ")
            || message.contains("sqs")
            || message.contains("timeout")
            || message.contains("tenant");
    }

    private String taskSummary(Task task) {
        String containerStatus = task
            .containers()
            .stream()
            .map(container -> container.name() + "[status=" + container.lastStatus() + ", exitCode=" + container.exitCode() + "]")
            .toList()
            .toString();
        return "Task[arn="
            + task.taskArn()
            + ", lastStatus="
            + task.lastStatus()
            + ", stoppedReason="
            + task.stoppedReason()
            + ", stopCode="
            + task.stopCode()
            + ", containers="
            + containerStatus
            + "]";
    }

    private String serviceSummary(StartedService startedService) {
        try {
            software.amazon.awssdk.services.ecs.model.Service ecsService = describeService(startedService);
            List<String> deploymentStatus = ecsService
                .deployments()
                .stream()
                .map(
                    deployment -> deployment.status()
                        + "[desired="
                        + deployment.desiredCount()
                        + ", running="
                        + deployment.runningCount()
                        + ", pending="
                        + deployment.pendingCount()
                        + "]"
                )
                .toList();
            List<String> taskStatus = runningServiceTasks(startedService).stream().map(this::taskSummary).toList();
            return "Service[name="
                + startedService.serviceName()
                + ", desired="
                + ecsService.desiredCount()
                + ", running="
                + ecsService.runningCount()
                + ", pending="
                + ecsService.pendingCount()
                + ", deployments="
                + deploymentStatus
                + ", runningTasks="
                + taskStatus
                + "]";
        } catch (RuntimeException | AssertionError e) {
            return "failed to describe service " + startedService.serviceName() + ": " + e;
        }
    }

    private String redact(String value) {
        if (value == null) {
            return null;
        }
        return value
            .replaceAll("(?i)(X-Amz-Security-Token=)[^&\\s]+", "$1<redacted>")
            .replaceAll("(?i)(X-Amz-Credential=)[^&\\s]+", "$1<redacted>")
            .replaceAll("(?i)(X-Amz-Signature=)[^&\\s]+", "$1<redacted>")
            .replaceAll("(?i)(AWS_ACCESS_KEY_ID, Value=)[^\\])]+", "$1<redacted>")
            .replaceAll("(?i)(AWS_SECRET_ACCESS_KEY, Value=)[^\\])]+", "$1<redacted>")
            .replaceAll("(?i)(AWS_SESSION_TOKEN, Value=)[^\\])]+", "$1<redacted>");
    }

    private int httpStatus(String endpoint) throws IOException, GeneralSecurityException {
        HttpURLConnection connection = (HttpURLConnection) URI.create(endpoint).toURL().openConnection();
        if (connection instanceof HttpsURLConnection httpsConnection) {
            trustAll(httpsConnection);
        }
        connection.setConnectTimeout((int) Duration.ofSeconds(5).toMillis());
        connection.setReadTimeout((int) Duration.ofSeconds(5).toMillis());
        connection.setRequestMethod("GET");
        String user = System.getProperty("user");
        String password = System.getProperty("password");
        if (user != null && user.isBlank() == false && password != null) {
            String encoded = java.util.Base64
                .getEncoder()
                .encodeToString((user + ":" + password).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            connection.setRequestProperty("Authorization", "Basic " + encoded);
        }
        connection.connect();
        return connection.getResponseCode();
    }

    private void trustAll(HttpsURLConnection connection) throws GeneralSecurityException {
        TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return null;
            }

            @Override
            public void checkClientTrusted(X509Certificate[] certs, String authType) {}

            @Override
            public void checkServerTrusted(X509Certificate[] certs, String authType) {}
        } };
        SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(null, trustAllCerts, new SecureRandom());
        HostnameVerifier allHostsValid = (hostname, session) -> true;
        connection.setSSLSocketFactory(sslContext.getSocketFactory());
        connection.setHostnameVerifier(allHostsValid);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true) == false) {
            return;
        }
        removeShutdownHook();
        RuntimeException firstFailure = null;
        firstFailure = cleanupStep(firstFailure, this::shutdownModelCloudMapSync);
        firstFailure = cleanupStep(firstFailure, this::cleanupCloudMapInstance);
        firstFailure = cleanupStep(firstFailure, this::cleanupHashRingRevisions);
        firstFailure = cleanupStep(firstFailure, this::cleanupServices);
        firstFailure = cleanupStep(firstFailure, this::cleanupTasks);
        firstFailure = cleanupStep(firstFailure, this::cleanupRegisteredTaskDefinitions);
        firstFailure = cleanupStep(firstFailure, this::cleanupExecuteCommandTaskRolePolicy);
        firstFailure = cleanupStep(firstFailure, this::cleanupTaskRoleApplicationPolicy);
        firstFailure = cleanupStep(firstFailure, this::cleanupAossAccessPolicy);
        firstFailure = cleanupStep(firstFailure, this::cleanupCreatedTaskRole);
        firstFailure = cleanupStep(firstFailure, this::cleanupSecurityGroup);
        firstFailure = cleanupStep(firstFailure, this::cleanupS3Artifacts);
        firstFailure = cleanupStep(firstFailure, this::cleanupLogGroup);
        firstFailure = cleanupStep(firstFailure, this::cleanupSqsQueues);
        firstFailure = cleanupStep(firstFailure, this::cleanupCloudMapService);
        firstFailure = cleanupStep(firstFailure, () -> cleanupRun(runId));
        firstFailure = cleanupStep(firstFailure, this::cleanupCreatedEventBridgeRoles);
        firstFailure = cleanupStep(firstFailure, this::closeClients);
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    private void registerShutdownHook() {
        shutdownHook = new Thread(() -> {
            try {
                close();
            } catch (RuntimeException ignored) {
                // Best-effort cleanup during JVM shutdown.
            }
        }, "ecs-three-role-cleanup-" + runId);
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    private void removeShutdownHook() {
        if (shutdownHook == null) {
            return;
        }
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException ignored) {
            // The JVM is already shutting down; the hook is currently responsible for cleanup.
        }
    }

    static void cleanupRun(String runId) {
        if (runId == null || runId.isBlank()) {
            return;
        }
        Region cleanupRegion = Region.of(property(REGION_PROPERTY, Region.US_WEST_2.id()));
        try (
            EcsClient ecs = EcsClient
                .builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .region(cleanupRegion)
                .credentialsProvider(SecurityUtil.createCredentialsProvider())
                .build();
            Ec2Client ec2 = Ec2Client
                .builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .region(cleanupRegion)
                .credentialsProvider(SecurityUtil.createCredentialsProvider())
                .build();
            CloudWatchLogsClient logs = CloudWatchLogsClient
                .builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .region(cleanupRegion)
                .credentialsProvider(SecurityUtil.createCredentialsProvider())
                .build();
            ApplicationAutoScalingClient autoScaling = ApplicationAutoScalingClient
                .builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .region(cleanupRegion)
                .credentialsProvider(SecurityUtil.createCredentialsProvider())
                .build();
            IamClient iam = IamClient
                .builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .region(Region.AWS_GLOBAL)
                .credentialsProvider(SecurityUtil.createCredentialsProvider())
                .build();
            OpenSearchServerlessClient openSearchServerless = OpenSearchServerlessClient
                .builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .region(cleanupRegion)
                .credentialsProvider(SecurityUtil.createCredentialsProvider())
                .build();
            ServiceDiscoveryClient serviceDiscovery = ServiceDiscoveryClient
                .builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .region(cleanupRegion)
                .credentialsProvider(SecurityUtil.createCredentialsProvider())
                .build();
            SchedulerClient scheduler = SchedulerClient
                .builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .region(cleanupRegion)
                .credentialsProvider(SecurityUtil.createCredentialsProvider())
                .build();
            DynamoDbClient dynamoDb = DynamoDbClient
                .builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .region(cleanupRegion)
                .credentialsProvider(SecurityUtil.createCredentialsProvider())
                .build();
            SqsClient sqs = SqsClient
                .builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .region(cleanupRegion)
                .credentialsProvider(SecurityUtil.createCredentialsProvider())
                .build();
            S3Client s3 = S3Client
                .builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .region(cleanupRegion)
                .credentialsProvider(SecurityUtil.createCredentialsProvider())
                .build()
        ) {
            String ecsCluster = resolveEcsCluster(ecs);
            cleanupServicesByRunId(ecs, autoScaling, ecsCluster, runId);
            stopTasksByRunId(ecs, ecsCluster, runId);
            waitForRunTasksToStop(ecs, ecsCluster, runId);
            cleanupTaskDefinitionsByRunId(ecs, iam, runId);
            cleanupRunTaskRole(openSearchServerless, iam, runId);
            cleanupEventBridgeRolesByRunId(iam, runId);
            cleanupCloudMapByRunId(serviceDiscovery, runId);
            cleanupHashRingByRunId(dynamoDb, runId);
            cleanupEventBridgeSchedulesByRunId(scheduler, runId);
            cleanupSqsByRunId(sqs, runId);
            cleanupS3ByRunId(s3, runId);
            cleanupLogGroupByRunId(logs, runId);
            cleanupSecurityGroupByRunId(ec2, runId);
        } catch (Exception | AssertionError ignored) {
            // Best-effort cleanup. The test diagnostics and AWS-side verification commands expose any leftovers.
        }
    }

    private static void cleanupServicesByRunId(EcsClient ecs, ApplicationAutoScalingClient autoScaling, String ecsCluster, String runId) {
        for (String serviceName : serviceNames(runId)) {
            cleanupAutoScalingByResource(autoScaling, "service/" + ecsClusterNamePart(ecsCluster) + "/" + serviceName);
            try {
                ecs.updateService(UpdateServiceRequest.builder().cluster(ecsCluster).service(serviceName).desiredCount(0).build());
            } catch (Exception ignored) {
                // Best-effort cleanup.
            }
        }
        stopTasksByRunId(ecs, ecsCluster, runId);
        for (String serviceName : serviceNames(runId)) {
            try {
                ecs
                    .deleteService(
                        software.amazon.awssdk.services.ecs.model.DeleteServiceRequest
                            .builder()
                            .cluster(ecsCluster)
                            .service(serviceName)
                            .force(true)
                            .build()
                    );
            } catch (Exception ignored) {
                // Best-effort cleanup.
            }
        }
    }

    private static void cleanupAutoScalingByResource(ApplicationAutoScalingClient autoScaling, String resourceId) {
        try {
            var policies = autoScaling
                .describeScalingPolicies(
                    DescribeScalingPoliciesRequest
                        .builder()
                        .serviceNamespace(ECS_SERVICE_NAMESPACE)
                        .scalableDimension(ECS_SERVICE_DESIRED_COUNT)
                        .resourceId(resourceId)
                        .build()
                )
                .scalingPolicies();
            for (var policy : policies) {
                try {
                    autoScaling
                        .deleteScalingPolicy(
                            DeleteScalingPolicyRequest
                                .builder()
                                .serviceNamespace(ECS_SERVICE_NAMESPACE)
                                .scalableDimension(ECS_SERVICE_DESIRED_COUNT)
                                .resourceId(resourceId)
                                .policyName(policy.policyName())
                                .build()
                        );
                } catch (Exception ignored) {
                    // Best-effort cleanup.
                }
            }
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
        try {
            autoScaling
                .deregisterScalableTarget(
                    DeregisterScalableTargetRequest
                        .builder()
                        .serviceNamespace(ECS_SERVICE_NAMESPACE)
                        .scalableDimension(ECS_SERVICE_DESIRED_COUNT)
                        .resourceId(resourceId)
                        .build()
                );
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
    }

    private static void stopTasksByRunId(EcsClient ecs, String ecsCluster, String runId) {
        Set<String> taskArns = new HashSet<>();
        for (DesiredStatus desiredStatus : List.of(DesiredStatus.RUNNING, DesiredStatus.PENDING)) {
            try {
                taskArns
                    .addAll(
                        ecs
                            .listTasks(ListTasksRequest.builder().cluster(ecsCluster).startedBy(runId).desiredStatus(desiredStatus).build())
                            .taskArns()
                    );
            } catch (Exception ignored) {
                // Best-effort cleanup.
            }
            for (String serviceName : serviceNames(runId)) {
                try {
                    taskArns
                        .addAll(
                            ecs
                                .listTasks(
                                    ListTasksRequest
                                        .builder()
                                        .cluster(ecsCluster)
                                        .serviceName(serviceName)
                                        .desiredStatus(desiredStatus)
                                        .build()
                                )
                                .taskArns()
                        );
                } catch (Exception ignored) {
                    // Best-effort cleanup.
                }
            }
        }
        for (String taskArn : taskArns) {
            try {
                ecs.stopTask(StopTaskRequest.builder().cluster(ecsCluster).task(taskArn).reason("Cleaning up " + runId).build());
            } catch (Exception ignored) {
                // Best-effort cleanup.
            }
        }
    }

    private static void waitForRunTasksToStop(EcsClient ecs, String ecsCluster, String runId) {
        long deadlineNanos = System.nanoTime() + Duration.ofMinutes(6).toNanos();
        while (System.nanoTime() < deadlineNanos) {
            if (hasActiveRunTasks(ecs, ecsCluster, runId) == false) {
                return;
            }
            sleepQuietly(Duration.ofSeconds(10));
        }
    }

    private static boolean hasActiveRunTasks(EcsClient ecs, String ecsCluster, String runId) {
        for (DesiredStatus desiredStatus : List.of(DesiredStatus.RUNNING, DesiredStatus.PENDING)) {
            try {
                if (ecs
                    .listTasks(ListTasksRequest.builder().cluster(ecsCluster).startedBy(runId).desiredStatus(desiredStatus).build())
                    .taskArns()
                    .isEmpty() == false) {
                    return true;
                }
            } catch (Exception ignored) {
                // Best-effort cleanup.
            }
            for (String serviceName : serviceNames(runId)) {
                try {
                    if (ecs
                        .listTasks(
                            ListTasksRequest.builder().cluster(ecsCluster).serviceName(serviceName).desiredStatus(desiredStatus).build()
                        )
                        .taskArns()
                        .isEmpty() == false) {
                        return true;
                    }
                } catch (Exception ignored) {
                    // Best-effort cleanup.
                }
            }
        }
        return false;
    }

    private static void cleanupTaskDefinitionsByRunId(EcsClient ecs, IamClient iam, String runId) {
        try {
            var taskDefinitions = ecs
                .listTaskDefinitions(ListTaskDefinitionsRequest.builder().familyPrefix(runId).status(TaskDefinitionStatus.ACTIVE).build())
                .taskDefinitionArns();
            for (String taskDefinitionArn : taskDefinitions) {
                cleanupExecuteCommandTaskRolePolicyByTaskDefinition(ecs, iam, taskDefinitionArn, runId);
                try {
                    ecs.deregisterTaskDefinition(DeregisterTaskDefinitionRequest.builder().taskDefinition(taskDefinitionArn).build());
                } catch (Exception ignored) {
                    // Best-effort cleanup.
                }
            }
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
    }

    private static void cleanupExecuteCommandTaskRolePolicyByTaskDefinition(
        EcsClient ecs,
        IamClient iam,
        String taskDefinitionArn,
        String runId
    ) {
        try {
            TaskDefinition described = ecs
                .describeTaskDefinition(DescribeTaskDefinitionRequest.builder().taskDefinition(taskDefinitionArn).build())
                .taskDefinition();
            if (described.taskRoleArn() == null || described.taskRoleArn().isBlank()) {
                return;
            }
            String roleName = roleNameFromArn(described.taskRoleArn());
            try {
                iam
                    .deleteRolePolicy(
                        DeleteRolePolicyRequest.builder().roleName(roleName).policyName(runId + "-ecs-exec-ssmmessages").build()
                    );
            } catch (Exception ignored) {
                // Best-effort cleanup.
            }
            try {
                iam
                    .deleteRolePolicy(
                        DeleteRolePolicyRequest.builder().roleName(roleName).policyName(runId + "-ecs-task-app-access").build()
                    );
            } catch (Exception ignored) {
                // Best-effort cleanup.
            }
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
    }

    private static void cleanupRunTaskRole(OpenSearchServerlessClient openSearchServerless, IamClient iam, String runId) {
        String roleName = runId + "-task-role";
        String roleArn = null;
        try {
            roleArn = iam.getRole(GetRoleRequest.builder().roleName(roleName).build()).role().arn();
        } catch (NoSuchEntityException ignored) {
            return;
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
        if (roleArn != null && roleArn.isBlank() == false) {
            cleanupAossAccessPolicy(openSearchServerless, resolveAossAccessPolicyName(openSearchServerless), roleArn);
        }
        cleanupCreatedTaskRole(iam, roleName, runId);
    }

    private static void cleanupCloudMapByRunId(ServiceDiscoveryClient serviceDiscovery, String runId) {
        String namespaceName = property(CLOUD_MAP_NAMESPACE_PROPERTY, "");
        if (namespaceName.isBlank()) {
            return;
        }
        try {
            NamespaceSummary namespace = serviceDiscovery
                .listNamespaces(ListNamespacesRequest.builder().build())
                .namespaces()
                .stream()
                .filter(candidate -> namespaceName.equals(candidate.name()))
                .findFirst()
                .orElse(null);
            if (namespace == null) {
                return;
            }
            ServiceSummary service = findService(serviceDiscovery, namespace.id(), runId);
            if (service == null) {
                return;
            }
            try {
                var instances = serviceDiscovery.listInstances(ListInstancesRequest.builder().serviceId(service.id()).build()).instances();
                for (var instance : instances) {
                    try {
                        waitOperation(
                            serviceDiscovery,
                            serviceDiscovery
                                .deregisterInstance(
                                    DeregisterInstanceRequest.builder().serviceId(service.id()).instanceId(instance.id()).build()
                                )
                                .operationId()
                        );
                    } catch (Exception | AssertionError ignored) {
                        // Best-effort cleanup.
                    }
                }
            } catch (Exception ignored) {
                // Best-effort cleanup.
            }
            try {
                serviceDiscovery.deleteService(DeleteServiceRequest.builder().id(service.id()).build());
            } catch (Exception ignored) {
                // Best-effort cleanup.
            }
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
    }

    private static void cleanupHashRingByRunId(DynamoDbClient dynamoDb, String runId) {
        String tableName = property(CLOUD_MAP_TABLE_PROPERTY, "");
        if (tableName.isBlank()) {
            return;
        }
        try {
            var response = dynamoDb
                .query(
                    QueryRequest
                        .builder()
                        .tableName(tableName)
                        .keyConditionExpression("PK = :pk")
                        .expressionAttributeValues(Map.of(":pk", AttributeValue.fromS("service#" + runId)))
                        .build()
                );
            for (Map<String, AttributeValue> item : response.items()) {
                try {
                    dynamoDb
                        .deleteItem(
                            DeleteItemRequest
                                .builder()
                                .tableName(tableName)
                                .key(Map.of("PK", item.get("PK"), "revisionId", item.get("revisionId")))
                                .build()
                        );
                } catch (Exception ignored) {
                    // Best-effort cleanup.
                }
            }
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
    }

    private static void cleanupEventBridgeSchedulesByRunId(SchedulerClient scheduler, String runId) {
        String queueName = uniqueQueueName(runId);
        String scheduleGroup = EventBridgeHandler.resolveConfigScheduleGroup(property(SCHEDULER_GROUP_PROPERTY, ""), AnalysisType.AD);
        String nextToken = null;
        try {
            do {
                ListSchedulesRequest.Builder request = ListSchedulesRequest.builder().groupName(scheduleGroup).namePrefix("ad-");
                if (nextToken != null && nextToken.isBlank() == false) {
                    request.nextToken(nextToken);
                }
                ListSchedulesResponse response = scheduler.listSchedules(request.build());
                for (ScheduleSummary schedule : response.schedules()) {
                    if (scheduleTargetsQueue(schedule, queueName)) {
                        deleteScheduleQuietly(scheduler, scheduleGroup, schedule.name());
                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null && nextToken.isBlank() == false);
        } catch (ResourceNotFoundException ignored) {
            // The schedule group may not exist in partially configured test accounts.
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
    }

    private static boolean scheduleTargetsQueue(ScheduleSummary schedule, String queueName) {
        return schedule != null
            && schedule.target() != null
            && schedule.target().arn() != null
            && schedule.target().arn().endsWith(":" + queueName);
    }

    private static void deleteScheduleQuietly(SchedulerClient scheduler, String scheduleGroup, String scheduleName) {
        try {
            scheduler.deleteSchedule(DeleteScheduleRequest.builder().groupName(scheduleGroup).name(scheduleName).build());
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
    }

    private static void cleanupSqsByRunId(SqsClient sqs, String runId) {
        deleteQueueByName(sqs, uniqueQueueName(runId));
        deleteQueueByName(sqs, dlqName(uniqueQueueName(runId)));
    }

    private static void deleteQueueByName(SqsClient sqs, String queueName) {
        try {
            String queueUrl = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()).queueUrl();
            sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
    }

    private static void cleanupS3ByRunId(S3Client s3, String runId) {
        String bucket = property(S3_BUCKET_PROPERTY, "");
        if (bucket.isBlank()) {
            return;
        }
        try {
            String prefix = "integration-tests/" + runId + "/";
            var objects = s3.listObjectsV2(ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build()).contents();
            for (var object : objects) {
                try {
                    s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(object.key()).build());
                } catch (Exception ignored) {
                    // Best-effort cleanup.
                }
            }
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
    }

    private static void cleanupLogGroupByRunId(CloudWatchLogsClient logs, String runId) {
        try {
            logs.deleteLogGroup(DeleteLogGroupRequest.builder().logGroupName(logGroupName(runId)).build());
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
    }

    private static void cleanupSecurityGroupByRunId(Ec2Client ec2, String runId) {
        long deadlineNanos = System.nanoTime() + Duration.ofMinutes(6).toNanos();
        do {
            try {
                var groups = ec2
                    .describeSecurityGroups(
                        software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsRequest
                            .builder()
                            .filters(Filter.builder().name("group-name").values(runId).build())
                            .build()
                    )
                    .securityGroups();
                if (groups.isEmpty()) {
                    return;
                }
                for (var group : groups) {
                    ec2.deleteSecurityGroup(DeleteSecurityGroupRequest.builder().groupId(group.groupId()).build());
                }
                return;
            } catch (Exception ignored) {
                sleepQuietly(Duration.ofSeconds(10));
            }
        } while (System.nanoTime() < deadlineNanos);
    }

    private static List<String> serviceNames(String runId) {
        return List
            .of(
                runId + "-" + TimeSeriesSettings.MASTER_ROLE,
                runId + "-" + TimeSeriesSettings.COORDINATOR_ROLE,
                runId + "-" + TimeSeriesSettings.MODEL_ROLE
            );
    }

    private static String ecsClusterNamePart(String ecsCluster) {
        int slash = ecsCluster.lastIndexOf('/');
        return slash < 0 ? ecsCluster : ecsCluster.substring(slash + 1);
    }

    private static void sleepQuietly(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private RuntimeException cleanupStep(RuntimeException firstFailure, Runnable cleanup) {
        try {
            cleanup.run();
        } catch (RuntimeException e) {
            if (firstFailure == null) {
                return e;
            }
            firstFailure.addSuppressed(e);
        }
        return firstFailure;
    }

    private void closeClients() {
        ecsClient.close();
        ec2Client.close();
        s3Client.close();
        s3Presigner.close();
        logsClient.close();
        autoScalingClient.close();
        iamClient.close();
        openSearchServerlessClient.close();
        serviceDiscoveryClient.close();
        dynamoDbClient.close();
        sqsClient.close();
    }

    private void shutdownModelCloudMapSync() {
        if (modelCloudMapSyncExecutor != null) {
            modelCloudMapSyncExecutor.shutdownNow();
        }
    }

    private void cleanupCloudMapInstance() {
        for (String instanceId : new HashSet<>(registeredModelInstances.keySet())) {
            deregisterModelInstance(instanceId);
        }
    }

    private void cleanupHashRingRevisions() {
        try {
            QueryRequest request = QueryRequest
                .builder()
                .tableName(cloudMapTableName)
                .keyConditionExpression("PK = :pk")
                .expressionAttributeValues(Map.of(":pk", AttributeValue.fromS("service#" + cloudMapServiceName)))
                .build();
            var response = dynamoDbClient.query(request);
            for (Map<String, AttributeValue> item : response.items()) {
                AttributeValue revisionId = item.get("revisionId");
                if (revisionId != null) {
                    dynamoDbClient
                        .deleteItem(
                            DeleteItemRequest
                                .builder()
                                .tableName(cloudMapTableName)
                                .key(
                                    Map
                                        .of(
                                            "PK",
                                            AttributeValue.fromS("service#" + cloudMapServiceName),
                                            "revisionId",
                                            AttributeValue.fromN(revisionId.n())
                                        )
                                )
                                .build()
                        );
                }
            }
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
    }

    private void cleanupTasks() {
        if (stopTasksOnCompletion == false) {
            return;
        }
        for (StartedTask task : tasks) {
            try {
                ecsClient.stopTask(StopTaskRequest.builder().cluster(ecsCluster).task(task.taskArn()).reason("Completed " + runId).build());
            } catch (Exception ignored) {
                // Best-effort cleanup.
            }
        }
    }

    private void cleanupServices() {
        for (StartedService service : services) {
            cleanupAutoScaling(service);
            try {
                ecsClient
                    .updateService(
                        UpdateServiceRequest.builder().cluster(ecsCluster).service(service.serviceName()).desiredCount(0).build()
                    );
            } catch (Exception ignored) {
                // Best-effort cleanup.
            }
            try {
                ecsClient
                    .deleteService(
                        software.amazon.awssdk.services.ecs.model.DeleteServiceRequest
                            .builder()
                            .cluster(ecsCluster)
                            .service(service.serviceName())
                            .force(true)
                            .build()
                    );
            } catch (Exception ignored) {
                // Best-effort cleanup.
            }
        }
    }

    private void cleanupAutoScaling(StartedService service) {
        if (service.policyNames().isEmpty()) {
            return;
        }
        for (String policyName : service.policyNames()) {
            try {
                autoScalingClient
                    .deleteScalingPolicy(
                        DeleteScalingPolicyRequest
                            .builder()
                            .serviceNamespace(ECS_SERVICE_NAMESPACE)
                            .scalableDimension(ECS_SERVICE_DESIRED_COUNT)
                            .resourceId(service.resourceId())
                            .policyName(policyName)
                            .build()
                    );
            } catch (Exception ignored) {
                // Best-effort cleanup.
            }
        }
        try {
            autoScalingClient
                .deregisterScalableTarget(
                    DeregisterScalableTargetRequest
                        .builder()
                        .serviceNamespace(ECS_SERVICE_NAMESPACE)
                        .scalableDimension(ECS_SERVICE_DESIRED_COUNT)
                        .resourceId(service.resourceId())
                        .build()
                );
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
    }

    private void cleanupRegisteredTaskDefinitions() {
        for (String taskDefinitionArn : new ArrayList<>(registeredTaskDefinitionArns)) {
            try {
                ecsClient.deregisterTaskDefinition(DeregisterTaskDefinitionRequest.builder().taskDefinition(taskDefinitionArn).build());
            } catch (Exception ignored) {
                // Best-effort cleanup.
            }
        }
    }

    private void cleanupExecuteCommandTaskRolePolicy() {
        if (managedExecuteCommandRoleName == null
            || managedExecuteCommandRoleName.isBlank()
            || managedExecuteCommandPolicyName == null
            || managedExecuteCommandPolicyName.isBlank()) {
            return;
        }
        try {
            iamClient
                .deleteRolePolicy(
                    DeleteRolePolicyRequest
                        .builder()
                        .roleName(managedExecuteCommandRoleName)
                        .policyName(managedExecuteCommandPolicyName)
                        .build()
                );
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
    }

    private void cleanupTaskRoleApplicationPolicy() {
        if (managedTaskRoleName == null
            || managedTaskRoleName.isBlank()
            || managedTaskRoleApplicationPolicyName == null
            || managedTaskRoleApplicationPolicyName.isBlank()) {
            return;
        }
        try {
            iamClient
                .deleteRolePolicy(
                    DeleteRolePolicyRequest.builder().roleName(managedTaskRoleName).policyName(managedTaskRoleApplicationPolicyName).build()
                );
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
    }

    private void cleanupAossAccessPolicy() {
        cleanupAossAccessPolicy(openSearchServerlessClient, managedAossAccessPolicyName, managedAossAccessPolicyRoleArn);
    }

    private static void cleanupAossAccessPolicy(OpenSearchServerlessClient openSearchServerlessClient, String policyName, String roleArn) {
        if (openSearchServerlessClient == null || policyName == null || policyName.isBlank() || roleArn == null || roleArn.isBlank()) {
            return;
        }
        try {
            var policy = openSearchServerlessClient
                .getAccessPolicy(GetAccessPolicyRequest.builder().type(AccessPolicyType.DATA).name(policyName).build())
                .accessPolicyDetail();
            JsonArray updatedPolicy = JsonParser.parseString(policyDocumentToJson(policy.policy())).getAsJsonArray();
            if (updateAossPolicyPrincipal(updatedPolicy, roleArn, false)) {
                openSearchServerlessClient
                    .updateAccessPolicy(
                        UpdateAccessPolicyRequest
                            .builder()
                            .type(AccessPolicyType.DATA)
                            .name(policyName)
                            .policyVersion(policy.policyVersion())
                            .policy(updatedPolicy.toString())
                            .build()
                    );
            }
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
    }

    private void cleanupCreatedTaskRole() {
        cleanupCreatedTaskRole(iamClient, createdTaskRoleName, runId);
    }

    private void cleanupCreatedEventBridgeRoles() {
        cleanupEventBridgeRole(iamClient, createdScheduleManagementRoleName, EVENT_BRIDGE_SCHEDULE_MANAGEMENT_POLICY_NAME);
        cleanupEventBridgeRole(iamClient, createdSqsDeliveryRoleName, EVENT_BRIDGE_SQS_DELIVERY_POLICY_NAME);
    }

    private static void cleanupEventBridgeRolesByRunId(IamClient iamClient, String runId) {
        cleanupEventBridgeRole(iamClient, runScopedScheduleManagementRoleName(runId), EVENT_BRIDGE_SCHEDULE_MANAGEMENT_POLICY_NAME);
        cleanupEventBridgeRole(iamClient, runScopedSqsDeliveryRoleName(runId), EVENT_BRIDGE_SQS_DELIVERY_POLICY_NAME);
    }

    private static void cleanupEventBridgeRole(IamClient iamClient, String roleName, String policyName) {
        if (roleName == null || roleName.isBlank()) {
            return;
        }
        try {
            iamClient.deleteRolePolicy(DeleteRolePolicyRequest.builder().roleName(roleName).policyName(policyName).build());
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
        try {
            iamClient.deleteRole(DeleteRoleRequest.builder().roleName(roleName).build());
        } catch (NoSuchEntityException ignored) {
            // Already gone.
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
    }

    private static void cleanupCreatedTaskRole(IamClient iamClient, String roleName, String runId) {
        if (roleName == null || roleName.isBlank()) {
            return;
        }
        try {
            iamClient
                .deleteRolePolicy(DeleteRolePolicyRequest.builder().roleName(roleName).policyName(runId + "-ecs-task-app-access").build());
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
        try {
            iamClient
                .deleteRolePolicy(DeleteRolePolicyRequest.builder().roleName(roleName).policyName(runId + "-ecs-exec-ssmmessages").build());
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
        try {
            iamClient.deleteRole(DeleteRoleRequest.builder().roleName(roleName).build());
        } catch (NoSuchEntityException ignored) {
            // Already gone.
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
    }

    private void cleanupSecurityGroup() {
        if (createdSecurityGroupId == null || createdSecurityGroupId.isBlank()) {
            return;
        }
        try {
            Awaitility.await().atMost(Duration.ofMinutes(3)).pollInterval(Duration.ofSeconds(5)).ignoreExceptions().untilAsserted(() -> {
                ec2Client.deleteSecurityGroup(DeleteSecurityGroupRequest.builder().groupId(createdSecurityGroupId).build());
            });
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
    }

    private void cleanupS3Artifacts() {
        for (S3ObjectRef object : uploadedArtifacts) {
            try {
                s3Client.deleteObject(DeleteObjectRequest.builder().bucket(object.bucket()).key(object.key()).build());
            } catch (Exception ignored) {
                // Best-effort cleanup.
            }
        }
    }

    private void cleanupLogGroup() {
        if (logGroupName == null || logGroupName.isBlank()) {
            return;
        }
        try {
            logsClient.deleteLogGroup(DeleteLogGroupRequest.builder().logGroupName(logGroupName).build());
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
    }

    private void cleanupSqsQueues() {
        if (sqsQueueResources.created() == false) {
            return;
        }
        deleteQueueQuietly(sqsQueueResources.queueUrl());
        deleteQueueQuietly(sqsQueueResources.dlqUrl());
    }

    private void deleteQueueQuietly(String queueUrl) {
        if (queueUrl == null || queueUrl.isBlank()) {
            return;
        }
        try {
            sqsClient.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
    }

    private void cleanupCloudMapService() {
        if (createdCloudMapService == false) {
            return;
        }
        try {
            serviceDiscoveryClient.deleteService(DeleteServiceRequest.builder().id(cloudMapServiceId).build());
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
    }

    private static SqsQueueResources resolveOrCreateSqsQueue(SqsClient sqsClient, String runId) {
        String configuredQueueName = property(SQS_QUEUE_NAME_PROPERTY, "");
        boolean createQueue = Boolean
            .parseBoolean(property(ECS_CREATE_SQS_QUEUE_PROPERTY, configuredQueueName.isBlank() ? "true" : "false"));
        String queueName = configuredQueueName.isBlank() ? uniqueQueueName(runId) : configuredQueueName;
        if (createQueue == false) {
            return new SqsQueueResources(queueName, "", "", false);
        }

        String dlqName = dlqName(queueName);
        String dlqUrl = sqsClient
            .createQueue(
                CreateQueueRequest
                    .builder()
                    .queueName(dlqName)
                    .attributes(Map.of(QueueAttributeName.FIFO_QUEUE, "true", QueueAttributeName.CONTENT_BASED_DEDUPLICATION, "false"))
                    .build()
            )
            .queueUrl();
        String dlqArn = queueArn(sqsClient, dlqUrl);
        String redrivePolicy = String.format(Locale.ROOT, "{\"deadLetterTargetArn\":\"%s\",\"maxReceiveCount\":\"3\"}", dlqArn);
        String queueUrl = sqsClient
            .createQueue(
                CreateQueueRequest
                    .builder()
                    .queueName(queueName)
                    .attributes(
                        Map
                            .of(
                                QueueAttributeName.FIFO_QUEUE,
                                "true",
                                QueueAttributeName.CONTENT_BASED_DEDUPLICATION,
                                "false",
                                QueueAttributeName.VISIBILITY_TIMEOUT,
                                "300",
                                QueueAttributeName.RECEIVE_MESSAGE_WAIT_TIME_SECONDS,
                                "1",
                                QueueAttributeName.REDRIVE_POLICY,
                                redrivePolicy
                            )
                    )
                    .build()
            )
            .queueUrl();
        return new SqsQueueResources(queueName, queueUrl, dlqUrl, true);
    }

    private static String queueArn(SqsClient sqsClient, String queueUrl) {
        return sqsClient
            .getQueueAttributes(GetQueueAttributesRequest.builder().queueUrl(queueUrl).attributeNames(QueueAttributeName.QUEUE_ARN).build())
            .attributes()
            .get(QueueAttributeName.QUEUE_ARN);
    }

    private static String uniqueQueueName(String runId) {
        return "ad-jobs-" + runId + ".fifo";
    }

    private static String dlqName(String queueName) {
        String fifoSuffix = ".fifo";
        String baseName = queueName.endsWith(fifoSuffix) ? queueName.substring(0, queueName.length() - fifoSuffix.length()) : queueName;
        String dlqBaseName = baseName + "-dlq";
        int maxBaseLength = 80 - fifoSuffix.length();
        if (dlqBaseName.length() > maxBaseLength) {
            dlqBaseName = dlqBaseName.substring(0, maxBaseLength);
        }
        return dlqBaseName + fifoSuffix;
    }

    private static String resolveEcsCluster(EcsClient ecsClient) {
        String configured = property(ECS_CLUSTER_PROPERTY, "");
        if (configured.isBlank() == false) {
            return configured;
        }
        List<String> clusters = ecsClient.listClusters(ListClustersRequest.builder().build()).clusterArns();
        if (clusters.size() == 1) {
            return clusters.get(0);
        }
        return clusters
            .stream()
            .filter(cluster -> cluster.endsWith("/default") || cluster.endsWith("/demo-cluster"))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Set " + ECS_CLUSTER_PROPERTY + "; discovered ECS clusters: " + clusters));
    }

    private static PluginArtifacts uploadPluginArtifacts(S3Client s3Client, S3Presigner s3Presigner, String runId) {
        String bucket = required(S3_BUCKET_PROPERTY);
        Path adPluginZip = resolveAdPluginZip();
        Path jobSchedulerPluginZip = resolveJobSchedulerPluginZip();
        S3ObjectRef adObject = cachedArtifactObject(bucket, adPluginZip);
        S3ObjectRef jobSchedulerObject = cachedArtifactObject(bucket, jobSchedulerPluginZip);
        ensureObjectAvailable(s3Client, adObject, adPluginZip);
        ensureObjectAvailable(s3Client, jobSchedulerObject, jobSchedulerPluginZip);
        return new PluginArtifacts(
            presignedUrl(s3Presigner, adObject),
            presignedUrl(s3Presigner, jobSchedulerObject),
            Collections.emptyList()
        );
    }

    private static S3ObjectRef cachedArtifactObject(String bucket, Path source) {
        return new S3ObjectRef(bucket, "integration-tests/artifacts/" + sha256(source) + "/" + source.getFileName());
    }

    private static String sha256(Path source) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(Files.readAllBytes(source));
            StringBuilder hex = new StringBuilder(hash.length * 2);
            for (byte value : hash) {
                hex.append(String.format(Locale.ROOT, "%02x", value));
            }
            return hex.toString();
        } catch (IOException | GeneralSecurityException e) {
            throw new IllegalStateException("Failed to hash ECS plugin artifact " + source, e);
        }
    }

    private static void ensureObjectAvailable(S3Client s3Client, S3ObjectRef object, Path source) {
        if (objectExists(s3Client, object)) {
            return;
        }
        putObjectWithRetry(s3Client, object, source);
    }

    private static boolean objectExists(S3Client s3Client, S3ObjectRef object) {
        try {
            s3Client.headObject(HeadObjectRequest.builder().bucket(object.bucket()).key(object.key()).build());
            return true;
        } catch (S3Exception e) {
            if (e.statusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                return false;
            }
            throw e;
        }
    }

    private static void putObjectWithRetry(S3Client s3Client, S3ObjectRef object, Path source) {
        RuntimeException failure = null;
        for (int attempt = 1; attempt <= S3_ARTIFACT_UPLOAD_ATTEMPTS; attempt++) {
            try {
                putObject(s3Client, object, source);
                return;
            } catch (RuntimeException e) {
                failure = e;
                sleepBeforeRetry(attempt);
            }
        }
        throw failure;
    }

    private static void putObject(S3Client s3Client, S3ObjectRef object, Path source) {
        try {
            s3Client
                .putObject(
                    PutObjectRequest.builder().bucket(object.bucket()).key(object.key()).build(),
                    RequestBody.fromBytes(Files.readAllBytes(source))
                );
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read ECS plugin artifact " + source, e);
        }
    }

    private static void sleepBeforeRetry(int attempt) {
        try {
            Thread.sleep(Duration.ofSeconds(attempt * 3L).toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while retrying S3 artifact upload", e);
        }
    }

    private static void deleteObjectQuietly(S3Client s3Client, S3ObjectRef object) {
        try {
            s3Client.deleteObject(DeleteObjectRequest.builder().bucket(object.bucket()).key(object.key()).build());
        } catch (Exception ignored) {
            // Best-effort cleanup after an artifact upload failure.
        }
    }

    private static String presignedUrl(S3Presigner s3Presigner, S3ObjectRef object) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(object.bucket()).key(object.key()).build();
        GetObjectPresignRequest presignRequest = GetObjectPresignRequest
            .builder()
            .signatureDuration(Duration.ofHours(2))
            .getObjectRequest(getObjectRequest)
            .build();
        return s3Presigner.presignGetObject(presignRequest).url().toString();
    }

    private static Path resolveAdPluginZip() {
        String configured = property(ECS_AD_PLUGIN_ZIP_PROPERTY, "");
        if (configured.isBlank() == false) {
            return existingPath(configured);
        }
        try (var paths = Files.list(Path.of("build", "distributions"))) {
            return paths
                .filter(path -> path.getFileName().toString().startsWith("opensearch-anomaly-detection-"))
                .filter(path -> path.getFileName().toString().endsWith(".zip"))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Run bundlePlugin or set " + ECS_AD_PLUGIN_ZIP_PROPERTY));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to resolve AD plugin zip", e);
        }
    }

    private static Path resolveJobSchedulerPluginZip() {
        String configured = property(ECS_JOB_SCHEDULER_PLUGIN_ZIP_PROPERTY, "");
        if (configured.isBlank() == false) {
            return existingPath(configured);
        }
        Path cacheRoot = Path
            .of(
                System.getProperty("user.home"),
                ".gradle",
                "caches",
                "modules-2",
                "files-2.1",
                "org.opensearch.plugin",
                "opensearch-job-scheduler"
            );
        try (var versions = Files.list(cacheRoot)) {
            return versions.flatMap(version -> {
                try {
                    return Files.walk(version, 2);
                } catch (IOException e) {
                    return java.util.stream.Stream.<Path>empty();
                }
            })
                .filter(path -> path.getFileName().toString().startsWith("opensearch-job-scheduler-"))
                .filter(path -> path.getFileName().toString().endsWith(".zip"))
                .findFirst()
                .orElseThrow(
                    () -> new IllegalStateException(
                        "Could not find opensearch-job-scheduler plugin zip; set " + ECS_JOB_SCHEDULER_PLUGIN_ZIP_PROPERTY
                    )
                );
        } catch (IOException e) {
            throw new IllegalStateException("Failed to resolve Job Scheduler plugin zip", e);
        }
    }

    private static Path existingPath(String value) {
        Path path = Path.of(value).toAbsolutePath();
        if (Files.exists(path) == false) {
            throw new IllegalStateException("File does not exist: " + path);
        }
        return path;
    }

    private static String registerOpenSearchTaskDefinition(
        EcsClient ecsClient,
        CloudWatchLogsClient logsClient,
        Region region,
        String runId,
        PluginArtifacts pluginArtifacts,
        String taskRoleArn
    ) {
        String logGroup = logGroupName(runId);
        createLogGroup(logsClient, logGroup);
        String executionRoleArn = resolveExecutionRoleArn(ecsClient);
        ContainerDefinition.Builder container = ContainerDefinition
            .builder()
            .name("opensearch")
            .image(property(ECS_IMAGE_PROPERTY, "opensearchproject/opensearch:3.3.0"))
            .essential(true)
            .portMappings(PortMapping.builder().containerPort(9200).hostPort(9200).protocol(TransportProtocol.TCP).build())
            .entryPoint("bash", "-c")
            .command(startupCommand())
            .logConfiguration(
                LogConfiguration
                    .builder()
                    .logDriver(LogDriver.AWSLOGS)
                    .options(Map.of("awslogs-group", logGroup, "awslogs-region", region.id(), "awslogs-stream-prefix", "opensearch"))
                    .build()
            )
            .environment(
                KeyValuePair.builder().name("AD_PLUGIN_URL").value(pluginArtifacts.adPluginUrl()).build(),
                KeyValuePair.builder().name("JOB_SCHEDULER_PLUGIN_URL").value(pluginArtifacts.jobSchedulerPluginUrl()).build()
            );

        RegisterTaskDefinitionRequest.Builder request = RegisterTaskDefinitionRequest
            .builder()
            .family(runId)
            .networkMode(NetworkMode.AWSVPC)
            .requiresCompatibilities(Compatibility.FARGATE)
            .cpu(property(ECS_CPU_PROPERTY, "2048"))
            .memory(property(ECS_MEMORY_PROPERTY, "4096"))
            .containerDefinitions(container.build());
        if (executionRoleArn.isBlank() == false) {
            request.executionRoleArn(executionRoleArn);
        }
        String effectiveTaskRoleArn = taskRoleArn;
        if (effectiveTaskRoleArn.isBlank()
            && Boolean.parseBoolean(property(ECS_ENABLE_EXECUTE_COMMAND_PROPERTY, "true"))
            && executionRoleArn.isBlank() == false) {
            effectiveTaskRoleArn = executionRoleArn;
        }
        if (effectiveTaskRoleArn.isBlank() == false) {
            request.taskRoleArn(effectiveTaskRoleArn);
        }
        return ecsClient.registerTaskDefinition(request.build()).taskDefinition().taskDefinitionArn();
    }

    private static String logGroupName(String runId) {
        return "/aws/ecs/" + runId;
    }

    private static void createLogGroup(CloudWatchLogsClient logsClient, String logGroup) {
        try {
            logsClient.createLogGroup(CreateLogGroupRequest.builder().logGroupName(logGroup).build());
        } catch (ResourceAlreadyExistsException ignored) {
            // Reused run ids are unlikely, but harmless.
        }
    }

    private static String resolveExecutionRoleArn(EcsClient ecsClient) {
        String configured = property(ECS_EXECUTION_ROLE_PROPERTY, "");
        if (configured.isBlank() == false) {
            return configured;
        }
        var taskDefinitions = ecsClient
            .listTaskDefinitions(
                ListTaskDefinitionsRequest.builder().status(TaskDefinitionStatus.ACTIVE).sort(SortOrder.DESC).maxResults(10).build()
            )
            .taskDefinitionArns();
        for (String taskDefinition : taskDefinitions) {
            var described = ecsClient
                .describeTaskDefinition(DescribeTaskDefinitionRequest.builder().taskDefinition(taskDefinition).build())
                .taskDefinition();
            if (described.executionRoleArn() != null && described.executionRoleArn().isBlank() == false) {
                return described.executionRoleArn();
            }
        }
        return "";
    }

    private static String resolveTaskRoleArn(EcsClient ecsClient) {
        String configured = property(ECS_TASK_ROLE_PROPERTY, "");
        if (configured.isBlank() == false) {
            return configured;
        }
        var taskDefinitions = ecsClient
            .listTaskDefinitions(
                ListTaskDefinitionsRequest.builder().status(TaskDefinitionStatus.ACTIVE).sort(SortOrder.DESC).maxResults(10).build()
            )
            .taskDefinitionArns();
        for (String taskDefinition : taskDefinitions) {
            var described = ecsClient
                .describeTaskDefinition(DescribeTaskDefinitionRequest.builder().taskDefinition(taskDefinition).build())
                .taskDefinition();
            if (described.taskRoleArn() != null && described.taskRoleArn().isBlank() == false) {
                return described.taskRoleArn();
            }
        }
        return "";
    }

    private static String startupCommand() {
        return String
            .join(
                "\n",
                "set -euo pipefail",
                "cd /usr/share/opensearch",
                "install_plugin() {",
                "  local plugin_dir=\"$1\"",
                "  local plugin_url=\"$2\"",
                "  if [ -d \"plugins/${plugin_dir}\" ]; then",
                "    echo \"Plugin ${plugin_dir} already installed; skipping\"",
                "    return 0",
                "  fi",
                "  bin/opensearch-plugin install --batch \"${plugin_url}\"",
                "}",
                "install_plugin opensearch-job-scheduler \"$JOB_SCHEDULER_PLUGIN_URL\"",
                "if [ -d plugins/opensearch-anomaly-detection ]; then",
                "  bin/opensearch-plugin remove opensearch-anomaly-detection",
                "fi",
                "bin/opensearch-plugin install --batch \"$AD_PLUGIN_URL\"",
                "if [ -x /usr/share/opensearch/opensearch-docker-entrypoint.sh ]; then exec /usr/share/opensearch/opensearch-docker-entrypoint.sh opensearch; fi",
                "if [ -x /usr/local/bin/docker-entrypoint.sh ]; then exec /usr/local/bin/docker-entrypoint.sh opensearch; fi",
                "exec /usr/share/opensearch/bin/opensearch"
            );
    }

    private static String configuredContainerName(EcsClient ecsClient, String taskDefinition) {
        String configured = property(ECS_CONTAINER_NAME_PROPERTY, "");
        if (configured.isBlank() == false) {
            return configured;
        }
        var response = ecsClient.describeTaskDefinition(DescribeTaskDefinitionRequest.builder().taskDefinition(taskDefinition).build());
        List<ContainerDefinition> definitions = response.taskDefinition().containerDefinitions();
        return definitions
            .stream()
            .filter(definition -> Boolean.TRUE.equals(definition.essential()))
            .findFirst()
            .or(() -> definitions.stream().findFirst())
            .map(ContainerDefinition::name)
            .orElseThrow(() -> new IllegalStateException("Task definition has no containers: " + taskDefinition));
    }

    private static NamespaceSummary resolveOrCreateNamespace(ServiceDiscoveryClient client, String namespaceName, String runId) {
        NamespaceSummary existing = findNamespace(client, namespaceName);
        if (existing != null) {
            return existing;
        }
        String operationId = client
            .createHttpNamespace(CreateHttpNamespaceRequest.builder().name(namespaceName).creatorRequestId(runId).build())
            .operationId();
        waitOperation(client, operationId);
        NamespaceSummary created = findNamespace(client, namespaceName);
        if (created == null) {
            throw new IllegalStateException("Created Cloud Map namespace but could not resolve it: " + namespaceName);
        }
        return created;
    }

    private static NamespaceSummary findNamespace(ServiceDiscoveryClient client, String namespaceName) {
        String nextToken = null;
        do {
            var response = client.listNamespaces(ListNamespacesRequest.builder().nextToken(nextToken).build());
            for (NamespaceSummary namespace : response.namespaces()) {
                if (namespaceName.equals(namespace.name())) {
                    return namespace;
                }
            }
            nextToken = response.nextToken();
        } while (nextToken != null && nextToken.isBlank() == false);
        return null;
    }

    private static ServiceResolution resolveOrCreateService(
        ServiceDiscoveryClient client,
        NamespaceSummary namespace,
        String serviceName,
        boolean createCloudMapService
    ) {
        ServiceSummary existing = findService(client, namespace.id(), serviceName);
        if (existing != null) {
            return new ServiceResolution(existing.id(), existing.arn(), false);
        }
        if (createCloudMapService == false) {
            throw new IllegalStateException("Cloud Map service not found: " + serviceName + " in namespace " + namespace.name());
        }

        CreateServiceRequest.Builder request = CreateServiceRequest
            .builder()
            .name(serviceName)
            .namespaceId(namespace.id())
            .healthCheckCustomConfig(HealthCheckCustomConfig.builder().failureThreshold(1).build());
        if (namespace.type() != NamespaceType.HTTP) {
            request
                .dnsConfig(
                    DnsConfig
                        .builder()
                        .namespaceId(namespace.id())
                        .routingPolicy(RoutingPolicy.MULTIVALUE)
                        .dnsRecords(DnsRecord.builder().ttl(30L).type(RecordType.A).build())
                        .build()
                );
        }

        var created = client.createService(request.build()).service();
        return new ServiceResolution(created.id(), created.arn(), true);
    }

    private static ServiceSummary findService(ServiceDiscoveryClient client, String namespaceId, String serviceName) {
        String nextToken = null;
        do {
            var response = client
                .listServices(
                    ListServicesRequest
                        .builder()
                        .nextToken(nextToken)
                        .filters(ServiceFilter.builder().name(ServiceFilterName.NAMESPACE_ID).values(namespaceId).build())
                        .build()
                );
            for (ServiceSummary service : response.services()) {
                if (serviceName.equals(service.name())) {
                    return service;
                }
            }
            nextToken = response.nextToken();
        } while (nextToken != null && nextToken.isBlank() == false);
        return null;
    }

    private static void waitOperation(ServiceDiscoveryClient client, String operationId) {
        Awaitility.await().atMost(Duration.ofMinutes(3)).pollInterval(Duration.ofSeconds(3)).untilAsserted(() -> {
            var operation = client.getOperation(GetOperationRequest.builder().operationId(operationId).build()).operation();
            assertFalse("Cloud Map operation failed: " + operation, operation.status() == OperationStatus.FAIL);
            assertEquals("Cloud Map operation did not complete: " + operation, OperationStatus.SUCCESS, operation.status());
        });
    }

    private static String required(String key) {
        String value = System.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required system property [" + key + "]");
        }
        return value;
    }

    private static String property(String key, String defaultValue) {
        String value = System.getProperty(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static List<String> splitCsv(String value) {
        List<String> values = new ArrayList<>();
        for (String candidate : value.split(",")) {
            String trimmed = candidate.trim();
            if (trimmed.isBlank() == false) {
                values.add(trimmed);
            }
        }
        return values;
    }

    private record ServiceResolution(String serviceId, String serviceArn, boolean created) {
    }

    private record NetworkResources(List<String> subnets, List<String> securityGroups) {
    }

    private record S3ObjectRef(String bucket, String key) {
    }

    private record PluginArtifacts(String adPluginUrl, String jobSchedulerPluginUrl, List<S3ObjectRef> uploadedObjects) {
    }

    private record SqsQueueResources(String queueName, String queueUrl, String dlqUrl, boolean created) {
    }

    private record ManagedTaskRole(String roleArn, String roleName, boolean created) {
    }

    private record ManagedEventBridgeRoleNames(String scheduleManagementRoleName, String sqsDeliveryRoleName) {
    }

    private record ManagedEventBridgeRoles(String scheduleManagementRoleName, String scheduleManagementRoleArn, String sqsDeliveryRoleName,
        String sqsDeliveryRoleArn) {
    }

    private record StartedTask(String role, String taskArn, String eniId, String privateIp, String publicIp, String endpoint) {
    }

    private record StartedService(String role, String serviceName, String taskDefinitionArn, String resourceId, List<String> policyNames) {
    }

    @FunctionalInterface
    interface ThrowingRunnable {
        void run() throws Exception;
    }
}
