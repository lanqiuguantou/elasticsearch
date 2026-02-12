/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Assertions;
import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.snapshots.status.TransportNodesSnapshotsStatus;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.action.search.SearchExecutionStatsCollector;
import org.elasticsearch.action.search.SearchPhaseController;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.metadata.AliasValidator;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexUpgradeService;
import org.elasticsearch.cluster.metadata.SystemIndexMetadataUpgradeService;
import org.elasticsearch.cluster.metadata.TemplateUpgradeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.BatchedRerouteService;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdMonitor;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.logging.NodeAndClusterIdStateListener;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.ConsistentSettingsService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.SettingUpgrader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeMetadata;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.gateway.GatewayMetaState;
import org.elasticsearch.gateway.GatewayModule;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.gateway.MetaStateService;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.recovery.PeerRecoverySourceService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.monitor.fs.FsHealthService;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.persistent.PersistentTasksClusterService;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksExecutorRegistry;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.CircuitBreakerPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.MetadataUpgrader;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.repositories.RepositoriesModule;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.support.AggregationUsageService;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.snapshots.InternalSnapshotsInfoService;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotShardsService;
import org.elasticsearch.snapshots.SnapshotsInfoService;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancellationService;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.watcher.ResourceWatcherService;

import javax.net.ssl.SNIHostName;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * A node represent a node within a cluster ({@code cluster.name}). The {@link #client()} can be used
 * in order to use a {@link Client} to perform actions/operations against the cluster.
 *
 * 【ES 节点核心类 — 所有组件的容器】
 * Node 是 Elasticsearch 中最核心的类，代表集群中的一个节点。
 * 它在构造函数中创建所有核心组件（PluginsService、ThreadPool、ClusterService、
 * IndicesService、TransportService、HttpServerTransport、DiscoveryModule 等），
 * 在 start() 方法中按严格顺序启动这些组件。
 *
 * 生命周期：
 *   构造函数 → 创建所有组件 → 打印 "initialized"
 *   start()  → 启动所有服务 → 打印 "starting ..." → "started"
 *   close()  → 停止所有服务 → 释放资源
 *
 * 对应日志：
 *   [o.e.n.Node] version[7.10.2-SNAPSHOT], pid[547318], build[...]  ← 构造函数开始
 *   [o.e.n.Node] initialized                                        ← 构造函数结束
 *   [o.e.n.Node] starting ...                                       ← start() 开始
 *   [o.e.n.Node] started                                            ← start() 结束
 */
public class Node implements Closeable {
    public static final Setting<Boolean> WRITE_PORTS_FILE_SETTING =
        Setting.boolSetting("node.portsfile", false, Property.NodeScope);
    private static final Setting<Boolean> NODE_DATA_SETTING =
        Setting.boolSetting("node.data", true, Property.Deprecated, Property.NodeScope);
    private static final Setting<Boolean> NODE_MASTER_SETTING =
        Setting.boolSetting("node.master", true, Property.Deprecated, Property.NodeScope);
    private static final Setting<Boolean> NODE_INGEST_SETTING =
        Setting.boolSetting("node.ingest", true, Property.Deprecated, Property.NodeScope);
    private static final Setting<Boolean> NODE_REMOTE_CLUSTER_CLIENT = Setting.boolSetting(
        "node.remote_cluster_client",
        RemoteClusterService.ENABLE_REMOTE_CLUSTERS,
        Property.Deprecated,
        Property.NodeScope);

    /**
    * controls whether the node is allowed to persist things like metadata to disk
    * Note that this does not control whether the node stores actual indices (see
    * {@link #NODE_DATA_SETTING}). However, if this is false, {@link #NODE_DATA_SETTING}
    * and {@link #NODE_MASTER_SETTING} must also be false.
    *
    */
    public static final Setting<Boolean> NODE_LOCAL_STORAGE_SETTING =
        Setting.boolSetting("node.local_storage", true, Property.Deprecated, Property.NodeScope);
    public static final Setting<String> NODE_NAME_SETTING = Setting.simpleString("node.name", Property.NodeScope);
    public static final Setting.AffixSetting<String> NODE_ATTRIBUTES = Setting.prefixKeySetting("node.attr.", (key) ->
        new Setting<>(key, "", (value) -> {
            if (value.length() > 0
                && (Character.isWhitespace(value.charAt(0)) || Character.isWhitespace(value.charAt(value.length() - 1)))) {
                throw new IllegalArgumentException(key + " cannot have leading or trailing whitespace " +
                    "[" + value + "]");
            }
            if (value.length() > 0 && "node.attr.server_name".equals(key)) {
                try {
                    new SNIHostName(value);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("invalid node.attr.server_name [" + value + "]", e );
                }
            }
            return value;
        }, Property.NodeScope));
    public static final Setting<String> BREAKER_TYPE_KEY = new Setting<>("indices.breaker.type", "hierarchy", (s) -> {
        switch (s) {
            case "hierarchy":
            case "none":
                return s;
            default:
                throw new IllegalArgumentException("indices.breaker.type must be one of [hierarchy, none] but was: " + s);
        }
    }, Setting.Property.NodeScope);

    private static final String CLIENT_TYPE = "node";

    private final Lifecycle lifecycle = new Lifecycle();

    /**
     * This logger instance is an instance field as opposed to a static field. This ensures that the field is not
     * initialized until an instance of Node is constructed, which is sure to happen after the logging infrastructure
     * has been initialized to include the hostname. If this field were static, then it would be initialized when the
     * class initializer runs. Alas, this happens too early, before logging is initialized as this class is referred to
     * in InternalSettingsPreparer#finalizeSettings, which runs when creating the Environment, before logging is
     * initialized.
     */
    private final Logger logger = LogManager.getLogger(Node.class);
    private final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(Node.class);
    private final Injector injector;
    private final Environment environment;
    private final NodeEnvironment nodeEnvironment;
    private final PluginsService pluginsService;
    private final NodeClient client;
    private final Collection<LifecycleComponent> pluginLifecycleComponents;
    private final LocalNodeFactory localNodeFactory;
    private final NodeService nodeService;

    public Node(Environment environment) {
        this(environment, Collections.emptyList(), true);
    }

    /**
     * 【Node 构造函数 — 创建所有核心组件】
     * 这是 ES 启动流程中最重量级的方法，按顺序创建所有核心组件。
     * 整个构造过程对应日志从 "version[7.10.2-SNAPSHOT]" 到 "initialized"。
     *
     * 组件创建顺序（与日志对应）：
     *   1. 打印版本/JVM/OS 信息 → 日志: version[...], pid[...], build[...]
     *   2. PluginsService — 加载 modules 和 plugins → 日志: loaded module [aggs-matrix-stats] ...
     *   3. NodeEnvironment — 数据目录、节点锁 → 日志: using [1] data paths, heap size [512mb]
     *   4. ThreadPool — 线程池（generic、search、write、management 等）
     *   5. ClusterService — 集群状态管理（MasterService + ClusterApplierService）
     *   6. IngestService — 数据预处理管道
     *   7. ScriptModule / AnalysisModule — 脚本引擎和分词器
     *   8. SettingsModule — 配置管理（集群级 + 索引级）
     *   9. ClusterModule / IndicesModule / SearchModule — 集群路由、索引管理、搜索
     *  10. CircuitBreakerService — 熔断器（防止 OOM）
     *  11. NetworkModule → Transport + HttpServerTransport → 日志: creating NettyAllocator
     *  12. DiscoveryModule — 节点发现和选举 → 日志: using discovery type [zen]
     *  13. RepositoriesModule / SnapshotsService — 快照和恢复
     *  14. Guice Injector — 依赖注入容器，绑定所有组件
     *  15. 初始化 REST handlers → 日志: initialized
     *
     * @param initialEnvironment         初始环境，会被插件补充额外配置
     * @param classpathPlugins           从 classpath 加载的插件类（测试用）
     * @param forbidPrivateIndexSettings 是否禁止私有索引设置（生产环境为 true，测试为 false）
     */
    protected Node(final Environment initialEnvironment,
                   Collection<Class<? extends Plugin>> classpathPlugins, boolean forbidPrivateIndexSettings) {
        final List<Closeable> resourcesToClose = new ArrayList<>(); // 错误时需要释放的资源列表
        boolean success = false;
        try {
            // 合并初始配置，设置 client.type=node
            Settings tmpSettings = Settings.builder().put(initialEnvironment.settings())
                .put(Client.CLIENT_TYPE_SETTING_S.getKey(), CLIENT_TYPE).build();

            // ==================== 【阶段1：打印节点基本信息】 ====================
            // 对应日志：[o.e.n.Node] version[7.10.2-SNAPSHOT], pid[547318], build[default/tar/...]
            final JvmInfo jvmInfo = JvmInfo.jvmInfo();
            logger.info(
                "version[{}], pid[{}], build[{}/{}/{}/{}], OS[{}/{}/{}], JVM[{}/{}/{}/{}]",
                Build.CURRENT.getQualifiedVersion(),
                jvmInfo.pid(),
                Build.CURRENT.flavor().displayName(),
                Build.CURRENT.type().displayName(),
                Build.CURRENT.hash(),
                Build.CURRENT.date(),
                Constants.OS_NAME,
                Constants.OS_VERSION,
                Constants.OS_ARCH,
                Constants.JVM_VENDOR,
                Constants.JVM_NAME,
                Constants.JAVA_VERSION,
                Constants.JVM_VERSION);
            // 对应日志：[o.e.n.Node] JVM home [/usr/lib/java/jdk-15.0.2], using bundled JDK [false]
            if (jvmInfo.getBundledJdk()) {
                logger.info("JVM home [{}], using bundled JDK [{}]", System.getProperty("java.home"), jvmInfo.getUsingBundledJdk());
            } else {
                logger.info("JVM home [{}]", System.getProperty("java.home"));
                deprecationLogger.deprecate(
                    "no-jdk",
                    "no-jdk distributions that do not bundle a JDK are deprecated and will be removed in a future release");
            }
            // 对应日志：[o.e.n.Node] JVM arguments [-Xshare:auto, -Des.networkaddress.cache.ttl=60, ...]
            logger.info("JVM arguments {}", Arrays.toString(jvmInfo.getInputArguments()));
            // 对应日志：[o.e.n.Node] version [7.10.2-SNAPSHOT] is a pre-release version ...
            if (Build.CURRENT.isProductionRelease() == false) {
                logger.warn(
                    "version [{}] is a pre-release version of Elasticsearch and is not suitable for production",
                    Build.CURRENT.getQualifiedVersion());
            }

            if (logger.isDebugEnabled()) {
                logger.debug("using config [{}], data [{}], logs [{}], plugins [{}]",
                    initialEnvironment.configFile(), Arrays.toString(initialEnvironment.dataFiles()),
                    initialEnvironment.logsFile(), initialEnvironment.pluginsFile());
            }

            // ==================== 【阶段2：加载插件和模块】 ====================
            // 对应日志：[o.e.p.PluginsService] loaded module [aggs-matrix-stats] ... loaded module [x-pack-watcher]
            //          [o.e.p.PluginsService] no plugins loaded
            // PluginsService 扫描 modules/ 和 plugins/ 目录，加载所有模块和插件
            // 模块是 ES 自带的功能扩展（如 reindex、transport-netty4、x-pack-*），插件是用户安装的
            this.pluginsService = new PluginsService(tmpSettings, initialEnvironment.configFile(), initialEnvironment.modulesFile(),
                initialEnvironment.pluginsFile(), classpathPlugins);
            // 插件可能会注册额外的配置项，这里获取合并后的最终配置
            final Settings settings = pluginsService.updatedSettings();

            // 收集插件注册的额外节点角色（如 x-pack 注册的 transform、ml 等角色）
            final Set<DiscoveryNodeRole> additionalRoles = pluginsService.filterPlugins(Plugin.class)
                .stream()
                .map(Plugin::getRoles)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
            DiscoveryNode.setAdditionalRoles(additionalRoles);

            // ==================== 【阶段3：创建 NodeEnvironment】 ====================
            // 对应日志：[o.e.e.NodeEnvironment] using [1] data paths, mounts [[/home (/dev/nvme1n1p6)]]
            //          [o.e.e.NodeEnvironment] heap size [512mb], compressed ordinary object pointers [true]
            // NodeEnvironment 管理数据目录、节点锁（防止多个实例使用同一数据目录）、节点 ID
            /*
             * 基于最终配置创建 Environment，确保所有组件获取到一致的配置值
             */
            this.environment = new Environment(settings, initialEnvironment.configFile(), Node.NODE_LOCAL_STORAGE_SETTING.get(settings));
            Environment.assertEquivalent(initialEnvironment, this.environment);
            nodeEnvironment = new NodeEnvironment(tmpSettings, environment);
            // 对应日志：[o.e.n.Node] node name [runTask-0], node ID [zzXl7znUSLOYhnSt4uutbA], cluster name [runTask], roles [...]
            logger.info("node name [{}], node ID [{}], cluster name [{}], roles {}",
                NODE_NAME_SETTING.get(tmpSettings), nodeEnvironment.nodeId(), ClusterName.CLUSTER_NAME_SETTING.get(tmpSettings).value(),
                DiscoveryNode.getRolesFromSettings(settings).stream()
                    .map(DiscoveryNodeRole::roleName)
                    .collect(Collectors.toCollection(LinkedHashSet::new)));
            resourcesToClose.add(nodeEnvironment);
            localNodeFactory = new LocalNodeFactory(settings, nodeEnvironment.nodeId());

            // ==================== 【阶段4：创建 ThreadPool】 ====================
            // ThreadPool 是 ES 的线程管理核心，包含多个命名线程池：
            //   generic（通用）、search（搜索）、write（写入）、management（管理）、
            //   get（获取）、analyze（分析）、snapshot（快照）等
            // 插件也可以注册自定义线程池（通过 ExecutorBuilder）
            final List<ExecutorBuilder<?>> executorBuilders = pluginsService.getExecutorBuilders(settings);

            final ThreadPool threadPool = new ThreadPool(settings, executorBuilders.toArray(new ExecutorBuilder[0]));
            resourcesToClose.add(() -> ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
            final ResourceWatcherService resourceWatcherService = new ResourceWatcherService(settings, threadPool);
            resourcesToClose.add(resourceWatcherService);
            // 将 ThreadContext 注入 DeprecationLogger，使废弃警告能携带请求上下文
            HeaderWarning.setThreadContext(threadPool.getThreadContext());
            resourcesToClose.add(() -> HeaderWarning.removeThreadContext(threadPool.getThreadContext()));

            // ==================== 【阶段5：创建核心服务】 ====================
            final List<Setting<?>> additionalSettings = new ArrayList<>();
            // 注册节点角色相关的配置项（node.data、node.ingest、node.master 等）
            additionalSettings.add(NODE_DATA_SETTING);
            additionalSettings.add(NODE_INGEST_SETTING);
            additionalSettings.add(NODE_MASTER_SETTING);
            additionalSettings.add(NODE_REMOTE_CLUSTER_CLIENT);
            additionalSettings.addAll(pluginsService.getPluginSettings());
            final List<String> additionalSettingsFilter = new ArrayList<>(pluginsService.getPluginSettingsFilter());
            for (final ExecutorBuilder<?> builder : threadPool.builders()) {
                additionalSettings.addAll(builder.getRegisteredSettings());
            }
            // NodeClient — 节点内部客户端，用于执行集群操作（如索引、搜索、管理等）
            client = new NodeClient(settings, threadPool);

            // ScriptModule — 脚本引擎（Painless、Expression、Mustache）
            final ScriptModule scriptModule = new ScriptModule(settings, pluginsService.filterPlugins(ScriptPlugin.class));
            final ScriptService scriptService = newScriptService(settings, scriptModule.engines, scriptModule.contexts);
            // AnalysisModule — 分词器和文本分析组件（standard、ik、pinyin 等）
            AnalysisModule analysisModule = new AnalysisModule(this.environment, pluginsService.filterPlugins(AnalysisPlugin.class));
            // this is as early as we can validate settings at this point. we already pass them to ScriptModule as well as ThreadPool
            // so we might be late here already

            final Set<SettingUpgrader<?>> settingsUpgraders = pluginsService.filterPlugins(Plugin.class)
                    .stream()
                    .map(Plugin::getSettingUpgraders)
                    .flatMap(List::stream)
                    .collect(Collectors.toSet());

            // SettingsModule — 配置管理，区分集群级配置（ClusterSettings）和索引级配置（IndexScopedSettings）
            final SettingsModule settingsModule =
                    new SettingsModule(settings, additionalSettings, additionalSettingsFilter, settingsUpgraders);
            scriptModule.registerClusterSettingsListeners(scriptService, settingsModule.getClusterSettings());
            // NetworkService — 网络服务，处理自定义 DNS 解析器（由 DiscoveryPlugin 提供）
            final NetworkService networkService = new NetworkService(
                getCustomNameResolvers(pluginsService.filterPlugins(DiscoveryPlugin.class)));

            // ==================== 【阶段6：创建 ClusterService】 ====================
            // ClusterService 是集群状态管理的核心，包含：
            //   - MasterService: 处理集群状态更新任务（仅 master 节点执行）
            //   - ClusterApplierService: 将集群状态变更应用到本地（所有节点执行）
            List<ClusterPlugin> clusterPlugins = pluginsService.filterPlugins(ClusterPlugin.class);
            final ClusterService clusterService = new ClusterService(settings, settingsModule.getClusterSettings(), threadPool);
            clusterService.addStateApplier(scriptService);  // 脚本服务监听集群状态变更
            resourcesToClose.add(clusterService);
            final Set<Setting<?>> consistentSettings = settingsModule.getConsistentSettings();
            if (consistentSettings.isEmpty() == false) {
                clusterService.addLocalNodeMasterListener(
                        new ConsistentSettingsService(settings, clusterService, consistentSettings).newHashPublisher());
            }
            // IngestService — 数据预处理管道（pipeline），支持 grok、geoip、user-agent 等处理器
            final IngestService ingestService = new IngestService(clusterService, threadPool, this.environment,
                scriptService, analysisModule.getAnalysisRegistry(),
                pluginsService.filterPlugins(IngestPlugin.class), client);
            final SetOnce<RepositoriesService> repositoriesServiceReference = new SetOnce<>();
            // ClusterInfoService — 收集集群磁盘使用信息，用于分片分配决策
            final ClusterInfoService clusterInfoService = newClusterInfoService(settings, clusterService, threadPool, client);
            // UsageService — 统计 REST API 使用情况
            final UsageService usageService = new UsageService();

            // ==================== 【阶段7：构建 Guice 模块】 ====================
            // ES 使用 Google Guice 作为依赖注入框架，这里按顺序添加各个模块
            ModulesBuilder modules = new ModulesBuilder();
            // 插件的 Guice 模块必须最先添加，否则可能出现注入错误
            for (Module pluginModule : pluginsService.createGuiceModules()) {
                modules.add(pluginModule);
            }
            // MonitorService — 系统监控（JVM、OS、进程、文件系统）
            final MonitorService monitorService = new MonitorService(settings, nodeEnvironment, threadPool);
            // FsHealthService — 文件系统健康检查（定期检测数据目录是否可写）
            final FsHealthService fsHealthService = new FsHealthService(settings, clusterService.getClusterSettings(), threadPool,
                nodeEnvironment);
            final SetOnce<RerouteService> rerouteServiceReference = new SetOnce<>();
            final InternalSnapshotsInfoService snapshotsInfoService = new InternalSnapshotsInfoService(settings, clusterService,
                repositoriesServiceReference::get, rerouteServiceReference::get);
            // ClusterModule — 集群路由和分片分配（AllocationService、ShardsAllocator）
            final ClusterModule clusterModule = new ClusterModule(settings, clusterService, clusterPlugins, clusterInfoService,
                snapshotsInfoService, threadPool.getThreadContext());
            modules.add(clusterModule);
            // IndicesModule — 索引管理（Mapper 注册、索引操作）
            IndicesModule indicesModule = new IndicesModule(pluginsService.filterPlugins(MapperPlugin.class));
            modules.add(indicesModule);

            // SearchModule — 搜索功能（聚合、查询构建器、排序、高亮、Suggest 等）
            SearchModule searchModule = new SearchModule(settings, false, pluginsService.filterPlugins(SearchPlugin.class));
            // ==================== 【阶段8：创建熔断器服务】 ====================
            // CircuitBreakerService — 内存熔断器，防止单个操作耗尽 JVM 堆内存导致 OOM
            // 包含多个熔断器：fielddata、request、in_flight_requests、accounting 等
            List<BreakerSettings> pluginCircuitBreakers = pluginsService.filterPlugins(CircuitBreakerPlugin.class)
                .stream()
                .map(plugin -> plugin.getCircuitBreaker(settings))
                .collect(Collectors.toList());
            final CircuitBreakerService circuitBreakerService = createCircuitBreakerService(settingsModule.getSettings(),
                pluginCircuitBreakers,
                settingsModule.getClusterSettings());
            pluginsService.filterPlugins(CircuitBreakerPlugin.class)
                .forEach(plugin -> {
                    CircuitBreaker breaker = circuitBreakerService.getBreaker(plugin.getCircuitBreaker(settings).getName());
                    plugin.setCircuitBreaker(breaker);
                });
            resourcesToClose.add(circuitBreakerService);
            modules.add(new GatewayModule());


            PageCacheRecycler pageCacheRecycler = createPageCacheRecycler(settings);
            BigArrays bigArrays = createBigArrays(pageCacheRecycler, circuitBreakerService);
            modules.add(settingsModule);
            List<NamedWriteableRegistry.Entry> namedWriteables = Stream.of(
                NetworkModule.getNamedWriteables().stream(),
                indicesModule.getNamedWriteables().stream(),
                searchModule.getNamedWriteables().stream(),
                pluginsService.filterPlugins(Plugin.class).stream()
                    .flatMap(p -> p.getNamedWriteables().stream()),
                ClusterModule.getNamedWriteables().stream())
                .flatMap(Function.identity()).collect(Collectors.toList());
            final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
            NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(Stream.of(
                NetworkModule.getNamedXContents().stream(),
                IndicesModule.getNamedXContents().stream(),
                searchModule.getNamedXContents().stream(),
                pluginsService.filterPlugins(Plugin.class).stream()
                    .flatMap(p -> p.getNamedXContent().stream()),
                ClusterModule.getNamedXWriteables().stream())
                .flatMap(Function.identity()).collect(toList()));
            final MetaStateService metaStateService = new MetaStateService(nodeEnvironment, xContentRegistry);
            final PersistedClusterStateService lucenePersistedStateFactory
                = new PersistedClusterStateService(nodeEnvironment, xContentRegistry, bigArrays, clusterService.getClusterSettings(),
                threadPool::relativeTimeInMillis);

            // collect engine factory providers from plugins
            final Collection<EnginePlugin> enginePlugins = pluginsService.filterPlugins(EnginePlugin.class);
            final Collection<Function<IndexSettings, Optional<EngineFactory>>> engineFactoryProviders =
                    enginePlugins.stream().map(plugin -> (Function<IndexSettings, Optional<EngineFactory>>)plugin::getEngineFactory)
                            .collect(Collectors.toList());


            final Map<String, IndexStorePlugin.DirectoryFactory> indexStoreFactories =
                    pluginsService.filterPlugins(IndexStorePlugin.class)
                            .stream()
                            .map(IndexStorePlugin::getDirectoryFactories)
                            .flatMap(m -> m.entrySet().stream())
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            final Map<String, IndexStorePlugin.RecoveryStateFactory> recoveryStateFactories =
                pluginsService.filterPlugins(IndexStorePlugin.class)
                    .stream()
                    .map(IndexStorePlugin::getRecoveryStateFactories)
                    .flatMap(m -> m.entrySet().stream())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            final Map<String, Collection<SystemIndexDescriptor>> systemIndexDescriptorMap = Collections.unmodifiableMap(pluginsService
                .filterPlugins(SystemIndexPlugin.class)
                .stream()
                .collect(Collectors.toMap(
                    plugin -> plugin.getClass().getSimpleName(),
                    plugin -> plugin.getSystemIndexDescriptors(settings))));
            final SystemIndices systemIndices = new SystemIndices(systemIndexDescriptorMap);

            final RerouteService rerouteService
                = new BatchedRerouteService(clusterService, clusterModule.getAllocationService()::reroute);
            rerouteServiceReference.set(rerouteService);
            clusterService.setRerouteService(rerouteService);

            final IndicesService indicesService =
                new IndicesService(settings, pluginsService, nodeEnvironment, xContentRegistry, analysisModule.getAnalysisRegistry(),
                    clusterModule.getIndexNameExpressionResolver(), indicesModule.getMapperRegistry(), namedWriteableRegistry,
                    threadPool, settingsModule.getIndexScopedSettings(), circuitBreakerService, bigArrays, scriptService,
                    clusterService, client, metaStateService, engineFactoryProviders, indexStoreFactories,
                    searchModule.getValuesSourceRegistry(), recoveryStateFactories);

            final AliasValidator aliasValidator = new AliasValidator();

            final ShardLimitValidator shardLimitValidator = new ShardLimitValidator(settings, clusterService);
            final MetadataCreateIndexService metadataCreateIndexService = new MetadataCreateIndexService(
                    settings,
                    clusterService,
                    indicesService,
                    clusterModule.getAllocationService(),
                    aliasValidator,
                shardLimitValidator,
                    environment,
                    settingsModule.getIndexScopedSettings(),
                    threadPool,
                    xContentRegistry,
                    systemIndices,
                    forbidPrivateIndexSettings
            );
            pluginsService.filterPlugins(Plugin.class)
                .forEach(p -> p.getAdditionalIndexSettingProviders()
                    .forEach(metadataCreateIndexService::addAdditionalIndexSettingProvider));

            final MetadataCreateDataStreamService metadataCreateDataStreamService =
                new MetadataCreateDataStreamService(threadPool, clusterService, metadataCreateIndexService);

            Collection<Object> pluginComponents = pluginsService.filterPlugins(Plugin.class).stream()
                .flatMap(p -> p.createComponents(client, clusterService, threadPool, resourceWatcherService,
                                                 scriptService, xContentRegistry, environment, nodeEnvironment,
                                                 namedWriteableRegistry, clusterModule.getIndexNameExpressionResolver(),
                                                 repositoriesServiceReference::get).stream())
                .collect(Collectors.toList());

            // ==================== 【阶段9：创建网络层】 ====================
            // ActionModule — 注册所有 Transport Action 和 REST Action
            // 每个 REST 端点（如 /_search、/_bulk）都对应一个 RestHandler 和 TransportAction
            ActionModule actionModule = new ActionModule(false, settings, clusterModule.getIndexNameExpressionResolver(),
                settingsModule.getIndexScopedSettings(), settingsModule.getClusterSettings(), settingsModule.getSettingsFilter(),
                threadPool, pluginsService.filterPlugins(ActionPlugin.class), client, circuitBreakerService, usageService, systemIndices);
            modules.add(actionModule);

            final RestController restController = actionModule.getRestController();
            // NetworkModule — 网络传输层，创建 Transport（节点间通信，默认 Netty4）和 HttpServerTransport（REST API）
            // 对应日志：[o.e.t.NettyAllocator] creating NettyAllocator with configs: [name=unpooled, ...]
            final NetworkModule networkModule = new NetworkModule(settings, false, pluginsService.filterPlugins(NetworkPlugin.class),
                threadPool, bigArrays, pageCacheRecycler, circuitBreakerService, namedWriteableRegistry, xContentRegistry,
                networkService, restController, clusterService.getClusterSettings());
            Collection<UnaryOperator<Map<String, IndexTemplateMetadata>>> indexTemplateMetadataUpgraders =
                pluginsService.filterPlugins(Plugin.class).stream()
                    .map(Plugin::getIndexTemplateMetadataUpgrader)
                    .collect(Collectors.toList());
            final MetadataUpgrader metadataUpgrader = new MetadataUpgrader(indexTemplateMetadataUpgraders);
            final MetadataIndexUpgradeService metadataIndexUpgradeService = new MetadataIndexUpgradeService(settings, xContentRegistry,
                indicesModule.getMapperRegistry(), settingsModule.getIndexScopedSettings(), systemIndices, scriptService);
            if (DiscoveryNode.isMasterNode(settings)) {
                clusterService.addListener(new SystemIndexMetadataUpgradeService(systemIndices, clusterService));
            }
            new TemplateUpgradeService(client, clusterService, threadPool, indexTemplateMetadataUpgraders);
            // Transport — 节点间通信的传输层（默认使用 Netty4，端口 9300）
            final Transport transport = networkModule.getTransportSupplier().get();
            // 收集所有插件注册的 task header（如 X-Opaque-Id，用于请求追踪）
            Set<String> taskHeaders = Stream.concat(
                pluginsService.filterPlugins(ActionPlugin.class).stream().flatMap(p -> p.getTaskHeaders().stream()),
                Stream.of(Task.X_OPAQUE_ID)
            ).collect(Collectors.toSet());
            // TransportService — 封装 Transport，提供请求/响应的高层 API
            final TransportService transportService = newTransportService(settings, transport, threadPool,
                networkModule.getTransportInterceptor(), localNodeFactory, settingsModule.getClusterSettings(), taskHeaders);
            // GatewayMetaState — 管理持久化的集群元数据（存储在 Lucene 索引中）
            final GatewayMetaState gatewayMetaState = new GatewayMetaState();
            final ResponseCollectorService responseCollectorService = new ResponseCollectorService(clusterService);
            final SearchTransportService searchTransportService =  new SearchTransportService(transportService,
                SearchExecutionStatsCollector.makeWrapper(responseCollectorService));
            final HttpServerTransport httpServerTransport = newHttpTransport(networkModule);
            final IndexingPressure indexingLimits = new IndexingPressure(settings);

            final RecoverySettings recoverySettings = new RecoverySettings(settings, settingsModule.getClusterSettings());
            RepositoriesModule repositoriesModule = new RepositoriesModule(this.environment,
                pluginsService.filterPlugins(RepositoryPlugin.class), transportService, clusterService, threadPool, xContentRegistry,
                recoverySettings);
            RepositoriesService repositoryService = repositoriesModule.getRepositoryService();
            repositoriesServiceReference.set(repositoryService);
            SnapshotsService snapshotsService = new SnapshotsService(settings, clusterService,
                    clusterModule.getIndexNameExpressionResolver(), repositoryService, transportService, actionModule.getActionFilters());
            SnapshotShardsService snapshotShardsService = new SnapshotShardsService(settings, clusterService, repositoryService,
                    transportService, indicesService);
            TransportNodesSnapshotsStatus nodesSnapshotsStatus = new TransportNodesSnapshotsStatus(threadPool, clusterService,
                transportService, snapshotShardsService, actionModule.getActionFilters());
            RestoreService restoreService = new RestoreService(clusterService, repositoryService, clusterModule.getAllocationService(),
                metadataCreateIndexService, metadataIndexUpgradeService, clusterService.getClusterSettings(), shardLimitValidator);

            final DiskThresholdMonitor diskThresholdMonitor = new DiskThresholdMonitor(settings, clusterService::state,
                clusterService.getClusterSettings(), client, threadPool::relativeTimeInMillis, rerouteService);
            clusterInfoService.addListener(diskThresholdMonitor::onNewInfo);

            // ==================== 【阶段10：创建 DiscoveryModule】 ====================
            // 对应日志：[o.e.d.DiscoveryModule] using discovery type [zen] and seed hosts providers [settings, file]
            // DiscoveryModule 负责节点发现和 master 选举（Zen2 协议）
            // 包含 Coordinator（选举协调器）、PeerFinder（节点发现）、LeaderChecker/FollowersChecker（心跳检测）
            final DiscoveryModule discoveryModule = new DiscoveryModule(settings, threadPool, transportService, namedWriteableRegistry,
                networkService, clusterService.getMasterService(), clusterService.getClusterApplierService(),
                clusterService.getClusterSettings(), pluginsService.filterPlugins(DiscoveryPlugin.class),
                clusterModule.getAllocationService(), environment.configFile(), gatewayMetaState, rerouteService,
                fsHealthService);
            this.nodeService = new NodeService(settings, threadPool, monitorService, discoveryModule.getDiscovery(),
                transportService, indicesService, pluginsService, circuitBreakerService, scriptService,
                httpServerTransport, ingestService, clusterService, settingsModule.getSettingsFilter(), responseCollectorService,
                searchTransportService, indexingLimits, searchModule.getValuesSourceRegistry().getUsageService());

            final SearchService searchService = newSearchService(clusterService, indicesService,
                threadPool, scriptService, bigArrays, searchModule.getFetchPhase(),
                responseCollectorService, circuitBreakerService);

            final List<PersistentTasksExecutor<?>> tasksExecutors = pluginsService
                .filterPlugins(PersistentTaskPlugin.class).stream()
                .map(p -> p.getPersistentTasksExecutor(clusterService, threadPool, client, settingsModule,
                    clusterModule.getIndexNameExpressionResolver()))
                .flatMap(List::stream)
                .collect(toList());

            final PersistentTasksExecutorRegistry registry = new PersistentTasksExecutorRegistry(tasksExecutors);
            final PersistentTasksClusterService persistentTasksClusterService =
                new PersistentTasksClusterService(settings, registry, clusterService, threadPool);
            resourcesToClose.add(persistentTasksClusterService);
            final PersistentTasksService persistentTasksService = new PersistentTasksService(clusterService, threadPool, client);

            // ==================== 【阶段11：创建 Guice Injector — 依赖注入容器】 ====================
            // 将所有核心组件绑定到 Guice Injector 中，后续通过 injector.getInstance() 获取
            // 这是 ES 依赖注入的核心，所有组件的生命周期都由 Injector 管理
            modules.add(b -> {
                    b.bind(Node.class).toInstance(this);
                    b.bind(NodeService.class).toInstance(nodeService);
                    b.bind(NamedXContentRegistry.class).toInstance(xContentRegistry);
                    b.bind(PluginsService.class).toInstance(pluginsService);
                    b.bind(Client.class).toInstance(client);
                    b.bind(NodeClient.class).toInstance(client);
                    b.bind(Environment.class).toInstance(this.environment);
                    b.bind(ThreadPool.class).toInstance(threadPool);
                    b.bind(NodeEnvironment.class).toInstance(nodeEnvironment);
                    b.bind(ResourceWatcherService.class).toInstance(resourceWatcherService);
                    b.bind(CircuitBreakerService.class).toInstance(circuitBreakerService);
                    b.bind(BigArrays.class).toInstance(bigArrays);
                    b.bind(PageCacheRecycler.class).toInstance(pageCacheRecycler);
                    b.bind(ScriptService.class).toInstance(scriptService);
                    b.bind(AnalysisRegistry.class).toInstance(analysisModule.getAnalysisRegistry());
                    b.bind(IngestService.class).toInstance(ingestService);
                    b.bind(IndexingPressure.class).toInstance(indexingLimits);
                    b.bind(UsageService.class).toInstance(usageService);
                    b.bind(AggregationUsageService.class).toInstance(searchModule.getValuesSourceRegistry().getUsageService());
                    b.bind(NamedWriteableRegistry.class).toInstance(namedWriteableRegistry);
                    b.bind(MetadataUpgrader.class).toInstance(metadataUpgrader);
                    b.bind(MetaStateService.class).toInstance(metaStateService);
                    b.bind(PersistedClusterStateService.class).toInstance(lucenePersistedStateFactory);
                    b.bind(IndicesService.class).toInstance(indicesService);
                    b.bind(AliasValidator.class).toInstance(aliasValidator);
                    b.bind(MetadataCreateIndexService.class).toInstance(metadataCreateIndexService);
                    b.bind(MetadataCreateDataStreamService.class).toInstance(metadataCreateDataStreamService);
                    b.bind(SearchService.class).toInstance(searchService);
                    b.bind(SearchTransportService.class).toInstance(searchTransportService);
                    b.bind(SearchPhaseController.class).toInstance(new SearchPhaseController(
                        namedWriteableRegistry, searchService::aggReduceContextBuilder));
                    b.bind(Transport.class).toInstance(transport);
                    b.bind(TransportService.class).toInstance(transportService);
                    b.bind(NetworkService.class).toInstance(networkService);
                    b.bind(UpdateHelper.class).toInstance(new UpdateHelper(scriptService));
                    b.bind(MetadataIndexUpgradeService.class).toInstance(metadataIndexUpgradeService);
                    b.bind(ClusterInfoService.class).toInstance(clusterInfoService);
                    b.bind(SnapshotsInfoService.class).toInstance(snapshotsInfoService);
                    b.bind(GatewayMetaState.class).toInstance(gatewayMetaState);
                    b.bind(Discovery.class).toInstance(discoveryModule.getDiscovery());
                    {
                        processRecoverySettings(settingsModule.getClusterSettings(), recoverySettings);
                        b.bind(PeerRecoverySourceService.class).toInstance(new PeerRecoverySourceService(transportService,
                                indicesService, recoverySettings));
                        b.bind(PeerRecoveryTargetService.class).toInstance(new PeerRecoveryTargetService(threadPool,
                                transportService, recoverySettings, clusterService));
                    }
                    b.bind(HttpServerTransport.class).toInstance(httpServerTransport);
                    pluginComponents.stream().forEach(p -> b.bind((Class) p.getClass()).toInstance(p));
                    b.bind(PersistentTasksService.class).toInstance(persistentTasksService);
                    b.bind(PersistentTasksClusterService.class).toInstance(persistentTasksClusterService);
                    b.bind(PersistentTasksExecutorRegistry.class).toInstance(registry);
                    b.bind(RepositoriesService.class).toInstance(repositoryService);
                    b.bind(SnapshotsService.class).toInstance(snapshotsService);
                    b.bind(SnapshotShardsService.class).toInstance(snapshotShardsService);
                    b.bind(TransportNodesSnapshotsStatus.class).toInstance(nodesSnapshotsStatus);
                    b.bind(RestoreService.class).toInstance(restoreService);
                    b.bind(RerouteService.class).toInstance(rerouteService);
                    b.bind(ShardLimitValidator.class).toInstance(shardLimitValidator);
                    b.bind(FsHealthService.class).toInstance(fsHealthService);
                    b.bind(SystemIndices.class).toInstance(systemIndices);
                }
            );
            // 创建 Guice Injector 实例，完成所有依赖绑定
            injector = modules.createInjector();

            // 解决循环依赖：分片分配服务需要 GatewayAllocator，而 GatewayAllocator 需要触发 reroute
            // 这里通过后置设置来关闭这个循环
            clusterModule.setExistingShardsAllocators(injector.getInstance(GatewayAllocator.class));

            List<LifecycleComponent> pluginLifecycleComponents = pluginComponents.stream()
                .filter(p -> p instanceof LifecycleComponent)
                .map(p -> (LifecycleComponent) p).collect(Collectors.toList());
            pluginLifecycleComponents.addAll(pluginsService.getGuiceServiceClasses().stream()
                .map(injector::getInstance).collect(Collectors.toList()));
            resourcesToClose.addAll(pluginLifecycleComponents);
            resourcesToClose.add(injector.getInstance(PeerRecoverySourceService.class));
            this.pluginLifecycleComponents = Collections.unmodifiableList(pluginLifecycleComponents);
            // 初始化 NodeClient — 将所有 TransportAction 注册到客户端，使其可以执行集群操作
            client.initialize(injector.getInstance(new Key<Map<ActionType, TransportAction>>() {}),
                    () -> clusterService.localNode().getId(), transportService.getRemoteClusterService(),
                    namedWriteableRegistry);
            // ==================== 【阶段12：初始化 REST handlers】 ====================
            // 注册所有 REST 端点处理器（如 RestSearchAction、RestIndexAction、RestBulkAction 等）
            // 每个 REST 端点对应一个 URL 路径和 HTTP 方法（GET/POST/PUT/DELETE）
            logger.debug("initializing HTTP handlers ...");
            actionModule.initRestHandlers(() -> clusterService.state().nodes());
            // 对应日志：[o.e.n.Node] [runTask-0] initialized
            // 至此，Node 构造完成，所有组件已创建但尚未启动
            logger.info("initialized");

            success = true;
        } catch (IOException ex) {
            throw new ElasticsearchException("failed to bind service", ex);
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(resourcesToClose);
            }
        }
    }

    protected TransportService newTransportService(Settings settings, Transport transport, ThreadPool threadPool,
                                                   TransportInterceptor interceptor,
                                                   Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
                                                   ClusterSettings clusterSettings, Set<String> taskHeaders) {
        return new TransportService(settings, transport, threadPool, interceptor, localNodeFactory, clusterSettings, taskHeaders);
    }

    protected void processRecoverySettings(ClusterSettings clusterSettings, RecoverySettings recoverySettings) {
        // Noop in production, overridden by tests
    }

    /**
     * The settings that are used by this node. Contains original settings as well as additional settings provided by plugins.
     */
    public Settings settings() {
        return this.environment.settings();
    }

    /**
     * A client that can be used to execute actions (operations) against the cluster.
     */
    public Client client() {
        return client;
    }

    /**
     * Returns the environment of the node
     */
    public Environment getEnvironment() {
        return environment;
    }

    /**
     * Returns the {@link NodeEnvironment} instance of this node
     */
    public NodeEnvironment getNodeEnvironment() {
        return nodeEnvironment;
    }


    /**
     * Start the node. If the node is already started, this method is no-op.
     *
     * 【启动节点 — 按严格顺序启动所有子服务】
     * 构造函数只是创建了组件，这里才真正启动它们。启动顺序非常重要，有严格的依赖关系。
     *
     * 启动顺序（与日志对应）：
     *   1. 插件生命周期组件
     *   2. IndicesService / SnapshotsService / SearchService 等核心服务
     *   3. GatewayService — 网关恢复服务
     *   4. TransportService — 节点间传输层 → 日志: publish_address {127.0.0.1:9300}
     *   5. GatewayMetaState — 加载磁盘上的集群元数据
     *   6. BootstrapChecks — 执行引导检查（堆大小、文件描述符等）
     *   7. Discovery.start() + ClusterService.start() — 启动发现和集群服务
     *   8. discovery.startInitialJoin() — 发起集群加入 → 日志: becoming CANDIDATE
     *   9. HttpServerTransport — 启动 HTTP 层 → 日志: publish_address {127.0.0.1:9200}
     *  10. 打印 "started"
     *
     * 注意：HTTP 在 Discovery 之后才启动，确保节点在对外提供服务前已开始加入集群。
     */
    public Node start() throws NodeValidationException {
        // 生命周期状态检查：如果已经启动过，直接返回（幂等）
        if (!lifecycle.moveToStarted()) {
            return this;
        }

        // 对应日志：[o.e.n.Node] [runTask-0] starting ...
        logger.info("starting ...");

        // ==================== 【步骤1：启动插件生命周期组件】 ====================
        // 启动所有插件创建的 LifecycleComponent（如 x-pack 的 SecurityLifecycleService 等）
        pluginLifecycleComponents.forEach(LifecycleComponent::start);

        // ==================== 【步骤2：启动核心服务】 ====================
        injector.getInstance(MappingUpdatedAction.class).setClient(client);
        // IndicesService — 索引管理服务（创建/删除/恢复索引）
        injector.getInstance(IndicesService.class).start();
        // IndicesClusterStateService — 监听集群状态变更，驱动本地索引的创建/删除/恢复
        injector.getInstance(IndicesClusterStateService.class).start();
        // SnapshotsService — 快照管理（创建/删除快照）
        injector.getInstance(SnapshotsService.class).start();
        // SnapshotShardsService — 分片级快照操作
        injector.getInstance(SnapshotShardsService.class).start();
        // RepositoriesService — 快照仓库管理（如 S3、HDFS、共享文件系统）
        injector.getInstance(RepositoriesService.class).start();
        // SearchService — 搜索服务（处理搜索请求的 query/fetch 阶段）
        injector.getInstance(SearchService.class).start();
        // FsHealthService — 文件系统健康检查
        injector.getInstance(FsHealthService.class).start();
        // MonitorService — 系统监控（JVM、OS、进程指标采集）
        nodeService.getMonitorService().start();

        final ClusterService clusterService = injector.getInstance(ClusterService.class);

        // NodeConnectionsService — 管理与集群中其他节点的连接
        final NodeConnectionsService nodeConnectionsService = injector.getInstance(NodeConnectionsService.class);
        nodeConnectionsService.start();
        clusterService.setNodeConnectionsService(nodeConnectionsService);

        // ==================== 【步骤3：启动 GatewayService】 ====================
        // GatewayService — 监听集群状态，当足够多的节点加入后触发索引恢复
        // 对应日志（master节点）：recovered [0] indices into cluster_state
        injector.getInstance(GatewayService.class).start();
        Discovery discovery = injector.getInstance(Discovery.class);
        // 将 Discovery 的 publish 方法设置为集群状态发布器
        clusterService.getMasterService().setClusterStatePublisher(discovery::publish);

        // ==================== 【步骤4：启动 TransportService】 ====================
        // 对应日志：[o.e.t.TransportService] publish_address {127.0.0.1:9300}, bound_addresses {[::1]:9300}, {127.0.0.1:9300}
        // 启动传输层后，本节点的 publish address 会被设置到 DiscoveryNode 中
        TransportService transportService = injector.getInstance(TransportService.class);
        transportService.getTaskManager().setTaskResultsService(injector.getInstance(TaskResultsService.class));
        transportService.getTaskManager().setTaskCancellationService(new TaskCancellationService(transportService));
        transportService.start();
        assert localNodeFactory.getNode() != null;
        assert transportService.getLocalNode().equals(localNodeFactory.getNode())
            : "transportService has a different local node than the factory provided";
        // PeerRecoverySourceService — 作为分片恢复的源端，向目标节点发送分片数据
        injector.getInstance(PeerRecoverySourceService.class).start();

        // ==================== 【步骤5：加载持久化的集群元数据】 ====================
        // 从磁盘加载（并可能升级）集群元数据，包括索引元数据、集群设置等
        // 元数据存储在 Lucene 索引中（_state 目录下）
        final GatewayMetaState gatewayMetaState = injector.getInstance(GatewayMetaState.class);
        gatewayMetaState.start(settings(), transportService, clusterService, injector.getInstance(MetaStateService.class),
            injector.getInstance(MetadataIndexUpgradeService.class), injector.getInstance(MetadataUpgrader.class),
            injector.getInstance(PersistedClusterStateService.class));
        if (Assertions.ENABLED) {
            try {
                if (DiscoveryModule.DISCOVERY_TYPE_SETTING.get(environment.settings()).equals(
                    DiscoveryModule.ZEN_DISCOVERY_TYPE) == false) {
                    assert injector.getInstance(MetaStateService.class).loadFullState().v1().isEmpty();
                }
                final NodeMetadata nodeMetadata = NodeMetadata.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY,
                    nodeEnvironment.nodeDataPaths());
                assert nodeMetadata != null;
                assert nodeMetadata.nodeVersion().equals(Version.CURRENT);
                assert nodeMetadata.nodeId().equals(localNodeFactory.getNode().getId());
            } catch (IOException e) {
                assert false : e;
            }
        }
        // ==================== 【步骤6：执行引导检查（Bootstrap Checks）】 ====================
        // 加载磁盘上持久化的集群状态元数据，传递给引导检查
        // 引导检查包括：堆大小检查、文件描述符数量、内存锁定状态、最大线程数、
        //              虚拟内存限制、系统调用过滤器等
        // 在生产模式下（绑定非回环地址时），任何检查失败都会阻止节点启动
        final Metadata onDiskMetadata = gatewayMetaState.getPersistedState().getLastAcceptedState().metadata();
        assert onDiskMetadata != null : "metadata is null but shouldn't"; // this is never null
        validateNodeBeforeAcceptingRequests(new BootstrapContext(environment, onDiskMetadata), transportService.boundAddress(),
            pluginsService.filterPlugins(Plugin.class).stream()
                .flatMap(p -> p.getBootstrapChecks().stream()).collect(Collectors.toList()));

        // ==================== 【步骤7：启动 Discovery 和 ClusterService】 ====================
        clusterService.addStateApplier(transportService.getTaskManager());
        // Discovery 必须在 ClusterService 之前启动，因为它需要在 ClusterApplierService 上设置初始状态
        discovery.start();
        // ClusterService 必须在 TransportService 之后启动，因为需要知道本地节点的 DiscoveryNode 信息
        clusterService.start();
        assert clusterService.localNode().equals(localNodeFactory.getNode())
            : "clusterService has a different local node than the factory provided";
        // 开始接受来自其他节点的传输层请求
        transportService.acceptIncomingRequests();

        // ==================== 【步骤8：发起集群加入（选举流程开始）】 ====================
        // 对应日志：[o.e.c.c.Coordinator] startInitialJoin: coordinator becoming CANDIDATE in term 0
        // 这一步触发 Zen2 选举协议：
        //   1. 节点变为 CANDIDATE 状态
        //   2. 通过 seed hosts 发现其他节点
        //   3. 发起 PreVote 预投票
        //   4. 收集到多数票后发起正式选举（StartJoinRequest）
        //   5. 赢得选举的节点成为 LEADER（master），其他节点成为 FOLLOWER
        // 对应日志中的选举过程：
        //   PreVoteCollector → requesting pre-votes → added PreVoteResponse → starting election
        //   Coordinator → starting election with StartJoinRequest{term=1}
        //   最终：runTask-1 赢得选举 → coordinator becoming LEADER in term 2
        //         runTask-0 成为 follower → coordinator becoming FOLLOWER of [runTask-1]
        discovery.startInitialJoin();
        final TimeValue initialStateTimeout = DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.get(settings());
        // 配置节点和集群 ID 的状态监听器（用于日志中显示集群 UUID）
        configureNodeAndClusterIdStateListener(clusterService);

        // ==================== 【步骤9：等待初始集群状态】 ====================
        // 如果配置了 discovery.initial_state_timeout（默认 30s），等待 master 选举完成
        // 如果超时仍未选出 master，节点会打印警告但继续运行
        if (initialStateTimeout.millis() > 0) {
            final ThreadPool thread = injector.getInstance(ThreadPool.class);
            ClusterState clusterState = clusterService.state();
            ClusterStateObserver observer =
                new ClusterStateObserver(clusterState, clusterService, null, logger, thread.getThreadContext());

            if (clusterState.nodes().getMasterNodeId() == null) {
                logger.debug("waiting to join the cluster. timeout [{}]", initialStateTimeout);
                final CountDownLatch latch = new CountDownLatch(1);
                observer.waitForNextChange(new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) { latch.countDown(); }

                    @Override
                    public void onClusterServiceClose() {
                        latch.countDown();
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        logger.warn("timed out while waiting for initial discovery state - timeout: {}",
                            initialStateTimeout);
                        latch.countDown();
                    }
                }, state -> state.nodes().getMasterNodeId() != null, initialStateTimeout);

                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new ElasticsearchTimeoutException("Interrupted while waiting for initial discovery state");
                }
            }
        }

        // ==================== 【步骤10：启动 HTTP 服务】 ====================
        // 对应日志：[o.e.h.AbstractHttpServerTransport] publish_address {127.0.0.1:9200}, bound_addresses {[::1]:9200}, {127.0.0.1:9200}
        // HTTP 层在 Discovery 之后才启动，这是有意为之——
        // 确保节点在对外提供 REST API 服务之前，已经开始加入集群流程
        injector.getInstance(HttpServerTransport.class).start();

        // 如果配置了 node.portsfile=true，将传输层和 HTTP 层的端口写入文件
        // 测试框架用这个来发现节点的端口
        if (WRITE_PORTS_FILE_SETTING.get(settings())) {
            TransportService transport = injector.getInstance(TransportService.class);
            writePortsFile("transport", transport.boundAddress());
            HttpServerTransport http = injector.getInstance(HttpServerTransport.class);
            writePortsFile("http", http.boundAddress());
        }

        // 对应日志：[o.e.n.Node] [runTask-0] started
        // 至此，节点启动完成，所有服务已就绪
        // 后续的集群选举、集群状态同步等操作在后台异步进行
        logger.info("started");

        // 通知所有 ClusterPlugin 节点已启动（插件可以在此执行启动后的初始化逻辑）
        pluginsService.filterPlugins(ClusterPlugin.class).forEach(ClusterPlugin::onNodeStarted);

        return this;
    }

    protected void configureNodeAndClusterIdStateListener(ClusterService clusterService) {
        NodeAndClusterIdStateListener.getAndSetNodeIdAndClusterId(clusterService,
            injector.getInstance(ThreadPool.class).getThreadContext());
    }

    private Node stop() {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        logger.info("stopping ...");

        injector.getInstance(ResourceWatcherService.class).close();
        injector.getInstance(HttpServerTransport.class).stop();

        injector.getInstance(SnapshotsService.class).stop();
        injector.getInstance(SnapshotShardsService.class).stop();
        injector.getInstance(RepositoriesService.class).stop();
        // stop any changes happening as a result of cluster state changes
        injector.getInstance(IndicesClusterStateService.class).stop();
        // close discovery early to not react to pings anymore.
        // This can confuse other nodes and delay things - mostly if we're the master and we're running tests.
        injector.getInstance(Discovery.class).stop();
        // we close indices first, so operations won't be allowed on it
        injector.getInstance(ClusterService.class).stop();
        injector.getInstance(NodeConnectionsService.class).stop();
        injector.getInstance(FsHealthService.class).stop();
        nodeService.getMonitorService().stop();
        injector.getInstance(GatewayService.class).stop();
        injector.getInstance(SearchService.class).stop();
        injector.getInstance(TransportService.class).stop();

        pluginLifecycleComponents.forEach(LifecycleComponent::stop);
        // we should stop this last since it waits for resources to get released
        // if we had scroll searchers etc or recovery going on we wait for to finish.
        injector.getInstance(IndicesService.class).stop();
        logger.info("stopped");

        return this;
    }

    // During concurrent close() calls we want to make sure that all of them return after the node has completed it's shutdown cycle.
    // If not, the hook that is added in Bootstrap#setup() will be useless:
    // close() might not be executed, in case another (for example api) call to close() has already set some lifecycles to stopped.
    // In this case the process will be terminated even if the first call to close() has not finished yet.
    @Override
    public synchronized void close() throws IOException {
        synchronized (lifecycle) {
            if (lifecycle.started()) {
                stop();
            }
            if (!lifecycle.moveToClosed()) {
                return;
            }
        }

        logger.info("closing ...");
        List<Closeable> toClose = new ArrayList<>();
        StopWatch stopWatch = new StopWatch("node_close");
        toClose.add(() -> stopWatch.start("node_service"));
        toClose.add(nodeService);
        toClose.add(() -> stopWatch.stop().start("http"));
        toClose.add(injector.getInstance(HttpServerTransport.class));
        toClose.add(() -> stopWatch.stop().start("snapshot_service"));
        toClose.add(injector.getInstance(SnapshotsService.class));
        toClose.add(injector.getInstance(SnapshotShardsService.class));
        toClose.add(injector.getInstance(RepositoriesService.class));
        toClose.add(() -> stopWatch.stop().start("client"));
        Releasables.close(injector.getInstance(Client.class));
        toClose.add(() -> stopWatch.stop().start("indices_cluster"));
        toClose.add(injector.getInstance(IndicesClusterStateService.class));
        toClose.add(() -> stopWatch.stop().start("indices"));
        toClose.add(injector.getInstance(IndicesService.class));
        // close filter/fielddata caches after indices
        toClose.add(injector.getInstance(IndicesStore.class));
        toClose.add(injector.getInstance(PeerRecoverySourceService.class));
        toClose.add(() -> stopWatch.stop().start("cluster"));
        toClose.add(injector.getInstance(ClusterService.class));
        toClose.add(() -> stopWatch.stop().start("node_connections_service"));
        toClose.add(injector.getInstance(NodeConnectionsService.class));
        toClose.add(() -> stopWatch.stop().start("discovery"));
        toClose.add(injector.getInstance(Discovery.class));
        toClose.add(() -> stopWatch.stop().start("monitor"));
        toClose.add(nodeService.getMonitorService());
        toClose.add(() -> stopWatch.stop().start("fsHealth"));
        toClose.add(injector.getInstance(FsHealthService.class));
        toClose.add(() -> stopWatch.stop().start("gateway"));
        toClose.add(injector.getInstance(GatewayService.class));
        toClose.add(() -> stopWatch.stop().start("search"));
        toClose.add(injector.getInstance(SearchService.class));
        toClose.add(() -> stopWatch.stop().start("transport"));
        toClose.add(injector.getInstance(TransportService.class));

        for (LifecycleComponent plugin : pluginLifecycleComponents) {
            toClose.add(() -> stopWatch.stop().start("plugin(" + plugin.getClass().getName() + ")"));
            toClose.add(plugin);
        }
        toClose.addAll(pluginsService.filterPlugins(Plugin.class));

        toClose.add(() -> stopWatch.stop().start("script"));
        toClose.add(injector.getInstance(ScriptService.class));

        toClose.add(() -> stopWatch.stop().start("thread_pool"));
        toClose.add(() -> injector.getInstance(ThreadPool.class).shutdown());
        // Don't call shutdownNow here, it might break ongoing operations on Lucene indices.
        // See https://issues.apache.org/jira/browse/LUCENE-7248. We call shutdownNow in
        // awaitClose if the node doesn't finish closing within the specified time.

        toClose.add(() -> stopWatch.stop().start("gateway_meta_state"));
        toClose.add(injector.getInstance(GatewayMetaState.class));

        toClose.add(() -> stopWatch.stop().start("node_environment"));
        toClose.add(injector.getInstance(NodeEnvironment.class));
        toClose.add(stopWatch::stop);

        if (logger.isTraceEnabled()) {
            toClose.add(() -> logger.trace("Close times for each service:\n{}", stopWatch.prettyPrint()));
        }
        IOUtils.close(toClose);
        logger.info("closed");
    }

    /**
     * Wait for this node to be effectively closed.
     */
    // synchronized to prevent running concurrently with close()
    public synchronized boolean awaitClose(long timeout, TimeUnit timeUnit) throws InterruptedException {
        if (lifecycle.closed() == false) {
            // We don't want to shutdown the threadpool or interrupt threads on a node that is not
            // closed yet.
            throw new IllegalStateException("Call close() first");
        }


        ThreadPool threadPool = injector.getInstance(ThreadPool.class);
        final boolean terminated = ThreadPool.terminate(threadPool, timeout, timeUnit);
        if (terminated) {
            // All threads terminated successfully. Because search, recovery and all other operations
            // that run on shards run in the threadpool, indices should be effectively closed by now.
            if (nodeService.awaitClose(0, TimeUnit.MILLISECONDS) == false) {
                throw new IllegalStateException("Some shards are still open after the threadpool terminated. " +
                        "Something is leaking index readers or store references.");
            }
        }
        return terminated;
    }

    /**
     * Returns {@code true} if the node is closed.
     */
    public boolean isClosed() {
        return lifecycle.closed();
    }

    public Injector injector() {
        return this.injector;
    }

    /**
     * Hook for validating the node after network
     * services are started but before the cluster service is started
     * and before the network service starts accepting incoming network
     * requests.
     *
     * @param context               the bootstrap context for this node
     * @param boundTransportAddress the network addresses the node is
     *                              bound and publishing to
     */
    @SuppressWarnings("unused")
    protected void validateNodeBeforeAcceptingRequests(
        final BootstrapContext context,
        final BoundTransportAddress boundTransportAddress, List<BootstrapCheck> bootstrapChecks) throws NodeValidationException {
    }

    /** Writes a file to the logs dir containing the ports for the given transport type */
    private void writePortsFile(String type, BoundTransportAddress boundAddress) {
        Path tmpPortsFile = environment.logsFile().resolve(type + ".ports.tmp");
        try (BufferedWriter writer = Files.newBufferedWriter(tmpPortsFile, Charset.forName("UTF-8"))) {
            for (TransportAddress address : boundAddress.boundAddresses()) {
                InetAddress inetAddress = InetAddress.getByName(address.getAddress());
                writer.write(NetworkAddress.format(new InetSocketAddress(inetAddress, address.getPort())) + "\n");
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write ports file", e);
        }
        Path portsFile = environment.logsFile().resolve(type + ".ports");
        try {
            Files.move(tmpPortsFile, portsFile, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new RuntimeException("Failed to rename ports file", e);
        }
    }

    /**
     * The {@link PluginsService} used to build this node's components.
     */
    protected PluginsService getPluginsService() {
        return pluginsService;
    }

    /**
     * Creates a new {@link CircuitBreakerService} based on the settings provided.
     * @see #BREAKER_TYPE_KEY
     */
    public static CircuitBreakerService createCircuitBreakerService(Settings settings,
                                                                    List<BreakerSettings> breakerSettings,
                                                                    ClusterSettings clusterSettings) {
        String type = BREAKER_TYPE_KEY.get(settings);
        if (type.equals("hierarchy")) {
            return new HierarchyCircuitBreakerService(settings, breakerSettings, clusterSettings);
        } else if (type.equals("none")) {
            return new NoneCircuitBreakerService();
        } else {
            throw new IllegalArgumentException("Unknown circuit breaker type [" + type + "]");
        }
    }

    /**
     * Creates a new {@link BigArrays} instance used for this node.
     * This method can be overwritten by subclasses to change their {@link BigArrays} implementation for instance for testing
     */
    BigArrays createBigArrays(PageCacheRecycler pageCacheRecycler, CircuitBreakerService circuitBreakerService) {
        return new BigArrays(pageCacheRecycler, circuitBreakerService, CircuitBreaker.REQUEST);
    }

    /**
     * Creates a new {@link BigArrays} instance used for this node.
     * This method can be overwritten by subclasses to change their {@link BigArrays} implementation for instance for testing
     */
    PageCacheRecycler createPageCacheRecycler(Settings settings) {
        return new PageCacheRecycler(settings);
    }

    /**
     * Creates a new the SearchService. This method can be overwritten by tests to inject mock implementations.
     */
    protected SearchService newSearchService(ClusterService clusterService, IndicesService indicesService,
                                             ThreadPool threadPool, ScriptService scriptService, BigArrays bigArrays,
                                             FetchPhase fetchPhase, ResponseCollectorService responseCollectorService,
                                             CircuitBreakerService circuitBreakerService) {
        return new SearchService(clusterService, indicesService, threadPool,
            scriptService, bigArrays, fetchPhase, responseCollectorService, circuitBreakerService);
    }

    /**
     * Creates a new the ScriptService. This method can be overwritten by tests to inject mock implementations.
     */
    protected ScriptService newScriptService(Settings settings, Map<String, ScriptEngine> engines, Map<String, ScriptContext<?>> contexts) {
        return new ScriptService(settings, engines, contexts);
    }

    /**
     * Get Custom Name Resolvers list based on a Discovery Plugins list
     * @param discoveryPlugins Discovery plugins list
     */
    private List<NetworkService.CustomNameResolver> getCustomNameResolvers(List<DiscoveryPlugin> discoveryPlugins) {
        List<NetworkService.CustomNameResolver> customNameResolvers = new ArrayList<>();
        for (DiscoveryPlugin discoveryPlugin : discoveryPlugins) {
            NetworkService.CustomNameResolver customNameResolver = discoveryPlugin.getCustomNameResolver(settings());
            if (customNameResolver != null) {
                customNameResolvers.add(customNameResolver);
            }
        }
        return customNameResolvers;
    }

    /** Constructs a ClusterInfoService which may be mocked for tests. */
    protected ClusterInfoService newClusterInfoService(Settings settings, ClusterService clusterService,
                                                       ThreadPool threadPool, NodeClient client) {
        final InternalClusterInfoService service = new InternalClusterInfoService(settings, clusterService, threadPool, client);
        if (DiscoveryNode.isMasterNode(settings)) {
            // listen for state changes (this node starts/stops being the elected master, or new nodes are added)
            clusterService.addListener(service);
        }
        return service;
    }

    /** Constructs a {@link org.elasticsearch.http.HttpServerTransport} which may be mocked for tests. */
    protected HttpServerTransport newHttpTransport(NetworkModule networkModule) {
        return networkModule.getHttpServerTransportSupplier().get();
    }

    private static class LocalNodeFactory implements Function<BoundTransportAddress, DiscoveryNode> {
        private final SetOnce<DiscoveryNode> localNode = new SetOnce<>();
        private final String persistentNodeId;
        private final Settings settings;

        private LocalNodeFactory(Settings settings, String persistentNodeId) {
            this.persistentNodeId = persistentNodeId;
            this.settings = settings;
        }

        @Override
        public DiscoveryNode apply(BoundTransportAddress boundTransportAddress) {
            localNode.set(DiscoveryNode.createLocal(settings, boundTransportAddress.publishAddress(), persistentNodeId));
            return localNode.get();
        }

        DiscoveryNode getNode() {
            assert localNode.get() != null;
            return localNode.get();
        }
    }
}
