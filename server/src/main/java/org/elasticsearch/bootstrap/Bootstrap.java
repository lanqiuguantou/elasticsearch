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

package org.elasticsearch.bootstrap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cli.KeyStoreAwareCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.PidFile;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.inject.CreationException;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.IfConfig;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.monitor.process.ProcessProbe;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Internal startup code.
 *
 * 【ES引导启动核心类】
 * Bootstrap 是 Elasticsearch 启动的核心编排者，由 Elasticsearch.init() 调用 Bootstrap.init() 进入。
 * 职责：
 *   1. 创建 keepAlive 线程（非守护线程），通过 CountDownLatch 阻塞防止 JVM 退出
 *   2. 加载 keystore（安全配置存储）
 *   3. 构建最终的 Environment（合并 elasticsearch.yml + 命令行参数 + keystore）
 *   4. 配置 Log4j2 日志系统
 *   5. setup(): 启动原生控制器、初始化本地资源、安装安全管理器、构造 Node 对象
 *   6. start(): 调用 Node.start() 启动所有服务，然后启动 keepAlive 线程
 *
 * 整体调用链：Bootstrap.init() → setup() → new Node() → start() → Node.start()
 */
final class Bootstrap {

    private static volatile Bootstrap INSTANCE;  // 全局单例，整个 JVM 只有一个 Bootstrap 实例
    private volatile Node node;                   // ES 节点实例，在 setup() 中创建
    private final CountDownLatch keepAliveLatch = new CountDownLatch(1);  // 用于阻塞 keepAlive 线程，防止 JVM 退出
    private final Thread keepAliveThread;         // 非守护线程，通过 latch 阻塞来保持 JVM 存活
    private final Spawner spawner = new Spawner(); // 原生进程启动器，用于启动 ML controller 等原生进程

    /**
     * 【构造函数 — 创建 keepAlive 机制】
     * 创建一个非守护线程 keepAliveThread，它通过 CountDownLatch.await() 永久阻塞。
     * 只要这个线程存活，JVM 就不会退出（因为它不是守护线程）。
     * 同时注册 JVM shutdown hook，在 JVM 关闭时释放 latch，让 keepAlive 线程正常结束。
     */
    Bootstrap() {
        keepAliveThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    keepAliveLatch.await();  // 永久阻塞，直到 shutdown hook 释放 latch
                } catch (InterruptedException e) {
                    // bail out
                }
            }
        }, "elasticsearch[keepAlive/" + Version.CURRENT + "]");
        keepAliveThread.setDaemon(false);  // 非守护线程，保持 JVM 存活
        // 注册 shutdown hook：JVM 关闭时释放 latch，让 keepAlive 线程退出
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                keepAliveLatch.countDown();
            }
        });
    }

    /**
     * 【初始化本地/原生资源】
     * 在安装正式 SecurityManager 之前调用，因为这些操作需要较高的系统权限。
     * 执行顺序：
     *   1. 检查是否以 root 用户运行（禁止，直接抛异常）
     *   2. 安装系统调用过滤器（Linux 上是 seccomp，macOS 上是 seatbelt），
     *      限制 ES 进程能执行的系统调用，防止被利用执行恶意操作
     *   3. 如果配置了 bootstrap.memory_lock=true，执行 mlockall（Linux）或 VirtualLock（Windows），
     *      将 JVM 堆内存锁定在物理内存中，防止被交换到磁盘（对性能至关重要）
     *   4. 注册 Windows 控制台关闭事件处理器（仅 Windows）
     *   5. 设置最大线程数、最大虚拟内存、最大文件大小等系统限制
     *   6. 初始化 Lucene 随机种子（使用 /dev/urandom）
     */
    public static void initializeNatives(Path tmpFile, boolean mlockAll, boolean systemCallFilter, boolean ctrlHandler) {
        final Logger logger = LogManager.getLogger(Bootstrap.class);

        // check if the user is running as root, and bail
        if (Natives.definitelyRunningAsRoot()) {
            throw new RuntimeException("can not run elasticsearch as root");
        }

        // enable system call filter
        if (systemCallFilter) {
            Natives.tryInstallSystemCallFilter(tmpFile);
        }

        // mlockall if requested
        if (mlockAll) {
            if (Constants.WINDOWS) {
               Natives.tryVirtualLock();
            } else {
               Natives.tryMlockall();
            }
        }

        // listener for windows close event
        if (ctrlHandler) {
            Natives.addConsoleCtrlHandler(new ConsoleCtrlHandler() {
                @Override
                public boolean handle(int code) {
                    if (CTRL_CLOSE_EVENT == code) {
                        logger.info("running graceful exit on windows");
                        try {
                            Bootstrap.stop();
                        } catch (IOException e) {
                            throw new ElasticsearchException("failed to stop node", e);
                        }
                        return true;
                    }
                    return false;
                }
            });
        }

        // force remainder of JNA to be loaded (if available).
        try {
            JNAKernel32Library.getInstance();
        } catch (Exception ignored) {
            // we've already logged this.
        }

        Natives.trySetMaxNumberOfThreads();
        Natives.trySetMaxSizeVirtualMemory();
        Natives.trySetMaxFileSize();

        // init lucene random seed. it will use /dev/urandom where available:
        StringHelper.randomId();
    }

    static void initializeProbes() {
        // Force probes to be loaded
        ProcessProbe.getInstance();
        OsProbe.getInstance();
        JvmInfo.jvmInfo();
    }

    /**
     * 【核心 setup 方法 — 构造 Node 对象】
     * 这是启动流程中最重要的方法之一，负责在启动 Node 之前完成所有准备工作。
     * 执行顺序：
     *   1. spawnNativeControllers() — 启动原生控制器进程（如 ML 的 C++ controller）
     *      对应日志：[controller/547752] controller (64 bit): Version 7.10.2-SNAPSHOT
     *   2. initializeNatives() — 初始化本地资源（seccomp、mlockall 等）
     *   3. initializeProbes() — 强制加载系统探针（ProcessProbe、OsProbe、JvmInfo）
     *   4. 注册 shutdown hook — JVM 关闭时优雅停止 Node 和 Spawner
     *   5. JarHell.checkJarHell() — 检查 classpath 中是否有重复的 jar 包
     *   6. IfConfig.logIfNecessary() — 记录网络接口信息（在安装 SM 之前，因为之后没权限读取）
     *   7. Security.configure() — 安装正式的 SecurityManager（替换 main() 中的临时 SM）
     *      基于 elasticsearch.policy 文件配置权限，限制 ES 进程的能力
     *   8. new Node(environment) — 【最重量级的步骤】构造 Node 对象，创建所有核心组件
     *      （PluginsService、NodeEnvironment、ThreadPool、ClusterService、TransportService 等）
     *      对应日志从 "version[7.10.2-SNAPSHOT]" 到 "initialized"
     */
    private void setup(boolean addShutdownHook, Environment environment) throws BootstrapException {
        Settings settings = environment.settings();

        try {
            // 【步骤1】启动原生控制器进程（如 x-pack-ml 的 C++ autodetect/controller 进程）
            // 对应日志：[o.e.x.m.p.l.CppLogMessageHandler] [controller/547752] controller (64 bit): Version 7.10.2-SNAPSHOT
            spawner.spawnNativeControllers(environment, true);
        } catch (IOException e) {
            throw new BootstrapException(e);
        }

        // 【步骤2】初始化本地资源：root检查、seccomp、mlockall、系统限制、Lucene随机种子
        initializeNatives(
                environment.tmpFile(),
                BootstrapSettings.MEMORY_LOCK_SETTING.get(settings),    // bootstrap.memory_lock
                BootstrapSettings.SYSTEM_CALL_FILTER_SETTING.get(settings), // bootstrap.system_call_filter
                BootstrapSettings.CTRLHANDLER_SETTING.get(settings));       // bootstrap.ctrlhandler (Windows)

        // 【步骤3】强制加载系统探针（必须在安装 SecurityManager 之前，因为之后可能没权限）
        initializeProbes();

        // 【步骤4】注册 JVM shutdown hook — 优雅关闭 Node 和原生进程
        if (addShutdownHook) {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        // 关闭 Node（停止所有服务）和 Spawner（杀死原生控制器进程）
                        IOUtils.close(node, spawner);
                        LoggerContext context = (LoggerContext) LogManager.getContext(false);
                        Configurator.shutdown(context);  // 关闭 Log4j2
                        // 等待 Node 完全关闭，最多等 10 秒
                        if (node != null && node.awaitClose(10, TimeUnit.SECONDS) == false) {
                            throw new IllegalStateException("Node didn't stop within 10 seconds. " +
                                    "Any outstanding requests or tasks might get killed.");
                        }
                    } catch (IOException ex) {
                        throw new ElasticsearchException("failed to stop node", ex);
                    } catch (InterruptedException e) {
                        LogManager.getLogger(Bootstrap.class).warn("Thread got interrupted while waiting for the node to shutdown.");
                        Thread.currentThread().interrupt();
                    }
                }
            });
        }

        try {
            // 【步骤5】Jar Hell 检查 — 扫描 classpath，检测是否有重复的 jar 或类
            // 如果有冲突会抛异常，防止运行时出现难以排查的 ClassCastException 等问题
            final Logger logger = LogManager.getLogger(JarHell.class);
            JarHell.checkJarHell(logger::debug);
        } catch (IOException | URISyntaxException e) {
            throw new BootstrapException(e);
        }

        // 【步骤6】记录网络接口信息（必须在安装 SecurityManager 之前）
        IfConfig.logIfNecessary();

        // 【步骤7】安装正式的 SecurityManager（替换 main() 中安装的临时全权限 SM）
        // 基于 elasticsearch.policy 文件和插件的权限声明，构建严格的权限策略
        // 安装后，ES 进程的能力被严格限制（如不能执行任意系统命令、不能访问任意文件等）
        try {
            Security.configure(environment, BootstrapSettings.SECURITY_FILTER_BAD_DEFAULTS_SETTING.get(settings));
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new BootstrapException(e);
        }

        // 【步骤8 — 最重量级】构造 Node 对象
        // 这一步会创建 ES 的所有核心组件：PluginsService、NodeEnvironment、ThreadPool、
        // ClusterService、IndicesService、TransportService、HttpServerTransport、DiscoveryModule 等
        // 对应日志从 "version[7.10.2-SNAPSHOT], pid[547318]" 一直到 "initialized"
        // 注意：这里重写了 validateNodeBeforeAcceptingRequests()，在其中执行 BootstrapChecks
        node = new Node(environment) {
            @Override
            protected void validateNodeBeforeAcceptingRequests(
                final BootstrapContext context,
                final BoundTransportAddress boundTransportAddress, List<BootstrapCheck> checks) throws NodeValidationException {
                // 执行引导检查（如堆大小、文件描述符数量、内存锁定状态等）
                // 在生产模式下（绑定非回环地址时），任何检查失败都会阻止节点启动
                BootstrapChecks.check(context, boundTransportAddress, checks);
            }
        };
    }

    /**
     * 【加载安全配置存储（keystore）】
     * 从 config 目录加载 elasticsearch.keystore 文件，其中存储了敏感配置（如密码、API key 等）。
     * 如果 keystore 不存在，会自动创建一个空的 keystore。
     * 如果 keystore 有密码保护，会从 stdin 读取密码进行解密。
     * 对应日志：[BUILD] Creating elasticsearch keystore with password set to []
     */
    static SecureSettings loadSecureSettings(Environment initialEnv) throws BootstrapException {
        final KeyStoreWrapper keystore;
        try {
            keystore = KeyStoreWrapper.load(initialEnv.configFile());
        } catch (IOException e) {
            throw new BootstrapException(e);
        }

        SecureString password;
        try {
            if (keystore != null && keystore.hasPassword()) {
                password = readPassphrase(System.in, KeyStoreAwareCommand.MAX_PASSPHRASE_LENGTH);
            } else {
                password = new SecureString(new char[0]);
            }
        } catch (IOException e) {
            throw new BootstrapException(e);
        }

        try{
            if (keystore == null) {
                final KeyStoreWrapper keyStoreWrapper = KeyStoreWrapper.create();
                keyStoreWrapper.save(initialEnv.configFile(), new char[0]);
                return keyStoreWrapper;
            } else {
                keystore.decrypt(password.getChars());
                KeyStoreWrapper.upgrade(keystore, initialEnv.configFile(), password.getChars());
            }
        } catch (Exception e) {
            throw new BootstrapException(e);
        } finally {
            password.close();
        }
        return keystore;
    }

    // visible for tests
    /**
     * Read from an InputStream up to the first carriage return or newline,
     * returning no more than maxLength characters.
     */
    static SecureString readPassphrase(InputStream stream, int maxLength) throws IOException {
        SecureString passphrase;

        try(InputStreamReader reader = new InputStreamReader(stream, StandardCharsets.UTF_8)) {
            passphrase = new SecureString(Terminal.readLineToCharArray(reader, maxLength));
        } catch (RuntimeException e) {
            if (e.getMessage().startsWith("Input exceeded maximum length")) {
                throw new IllegalStateException("Password exceeded maximum length of " + maxLength, e);
            }
            throw e;
        }

        if (passphrase.length() == 0) {
            passphrase.close();
            throw new IllegalStateException("Keystore passphrase required but none provided.");
        }

        return passphrase;
    }

    /**
     * 【构建最终的 Environment 对象】
     * 合并以下配置来源，构建 ES 运行所需的完整环境：
     *   - initialSettings: 从 elasticsearch.yml 和命令行参数解析出的初始配置
     *   - secureSettings: 从 keystore 加载的安全配置
     *   - pidFile: PID 文件路径（如果指定了 -p 参数）
     *   - configPath: 配置文件目录路径
     * 通过 InternalSettingsPreparer.prepareEnvironment() 完成最终的配置合并和校验。
     * HOSTNAME 环境变量由 elasticsearch-env 脚本设置，用作默认节点名。
     */
    private static Environment createEnvironment(
            final Path pidFile,
            final SecureSettings secureSettings,
            final Settings initialSettings,
            final Path configPath) {
        Settings.Builder builder = Settings.builder();
        if (pidFile != null) {
            builder.put(Environment.NODE_PIDFILE_SETTING.getKey(), pidFile);
        }
        builder.put(initialSettings);
        if (secureSettings != null) {
            builder.setSecureSettings(secureSettings);
        }
        return InternalSettingsPreparer.prepareEnvironment(builder.build(), Collections.emptyMap(), configPath,
                // HOSTNAME is set by elasticsearch-env and elasticsearch-env.bat so it is always available
                () -> System.getenv("HOSTNAME"));
    }

    /**
     * 【启动节点】
     * 调用 Node.start() 启动所有子服务（Transport、HTTP、Discovery、ClusterService 等），
     * 然后启动 keepAlive 线程，阻塞 JVM 防止退出。
     * 对应日志：
     *   "starting ..." → 各服务启动 → "publish_address {127.0.0.1:9300}" → "started"
     */
    private void start() throws NodeValidationException {
        node.start();            // 启动 ES 节点的所有服务
        keepAliveThread.start(); // 启动 keepAlive 线程，阻塞 JVM
    }

    static void stop() throws IOException {
        try {
            IOUtils.close(INSTANCE.node, INSTANCE.spawner);
            if (INSTANCE.node != null && INSTANCE.node.awaitClose(10, TimeUnit.SECONDS) == false) {
                throw new IllegalStateException("Node didn't stop within 10 seconds. Any outstanding requests or tasks might get killed.");
            }
        } catch (InterruptedException e) {
            LogManager.getLogger(Bootstrap.class).warn("Thread got interrupted while waiting for the node to shutdown.");
            Thread.currentThread().interrupt();
        } finally {
            INSTANCE.keepAliveLatch.countDown();
        }
    }

    /**
     * 【Bootstrap 核心入口 — 由 Elasticsearch.init() 调用】
     * 这是整个 ES 启动流程的总编排方法，按顺序完成以下步骤：
     *   1. BootstrapInfo.init() — 初始化引导信息（必须在安装 SecurityManager 之前）
     *   2. new Bootstrap() — 创建 Bootstrap 实例（含 keepAlive 线程和 shutdown hook）
     *   3. loadSecureSettings() — 加载 keystore
     *   4. createEnvironment() — 合并所有配置源，构建最终 Environment
     *   5. LogConfigurator.configure() — 配置 Log4j2 日志系统
     *   6. 创建 PID 文件（如果指定了 -p 参数）
     *   7. checkLucene() — 校验 Lucene 版本是否匹配
     *   8. setup() — 启动原生控制器、初始化本地资源、安装 SM、构造 Node
     *   9. start() — 调用 Node.start() 启动所有服务
     *
     * @param foreground 是否前台运行（true=前台，false=守护进程模式）
     * @param pidFile    PID 文件路径（可为 null）
     * @param quiet      是否静默模式（关闭控制台输出）
     * @param initialEnv 初始环境（从 elasticsearch.yml 和命令行参数构建）
     */
    static void init(
            final boolean foreground,
            final Path pidFile,
            final boolean quiet,
            final Environment initialEnv) throws BootstrapException, NodeValidationException, UserException {
        // 【步骤1】强制 BootstrapInfo 类初始化，必须在安装 SecurityManager 之前完成
        BootstrapInfo.init();

        // 【步骤2】创建全局唯一的 Bootstrap 实例（含 keepAlive 线程）
        INSTANCE = new Bootstrap();

        // 【步骤3】加载 keystore（安全配置存储，存放密码、API key 等敏感信息）
        final SecureSettings keystore = loadSecureSettings(initialEnv);
        // 【步骤4】合并所有配置源（elasticsearch.yml + 命令行参数 + keystore），构建最终 Environment
        final Environment environment = createEnvironment(pidFile, keystore, initialEnv.settings(), initialEnv.configFile());

        // 【步骤5】配置 Log4j2 日志系统（设置节点名用于日志前缀）
        LogConfigurator.setNodeName(Node.NODE_NAME_SETTING.get(environment.settings()));
        try {
            LogConfigurator.configure(environment);  // 根据 log4j2.properties 配置日志
        } catch (IOException e) {
            throw new BootstrapException(e);
        }
        // Java 11 版本检查（低于 11 的版本会打印废弃警告）
        if (JavaVersion.current().compareTo(JavaVersion.parse("11")) < 0) {
            final String message = String.format(
                            Locale.ROOT,
                            "future versions of Elasticsearch will require Java 11; " +
                                    "your Java version from [%s] does not meet this requirement",
                            System.getProperty("java.home"));
            DeprecationLogger.getLogger(Bootstrap.class).deprecate("java_version_11_required", message);
        }
        // 创建 PID 文件（如果指定了 -p 参数）
        if (environment.pidFile() != null) {
            try {
                PidFile.create(environment.pidFile(), true);
            } catch (IOException e) {
                throw new BootstrapException(e);
            }
        }

        // 如果是后台运行或静默模式，关闭控制台日志输出
        final boolean closeStandardStreams = (foreground == false) || quiet;
        try {
            if (closeStandardStreams) {
                final Logger rootLogger = LogManager.getRootLogger();
                final Appender maybeConsoleAppender = Loggers.findAppender(rootLogger, ConsoleAppender.class);
                if (maybeConsoleAppender != null) {
                    Loggers.removeAppender(rootLogger, maybeConsoleAppender);
                }
                closeSystOut();
            }

            // 【步骤6】校验 Lucene 版本 — 确保当前 ES 版本要求的 Lucene 版本与实际加载的一致
            checkLucene();

            // 设置全局未捕获异常处理器（必须在安装 SecurityManager 之前，因为之后没有 setDefaultUncaughtExceptionHandler 权限）
            Thread.setDefaultUncaughtExceptionHandler(new ElasticsearchUncaughtExceptionHandler());

            // 【步骤7 — 核心】调用 setup()：启动原生控制器、初始化本地资源、安装 SM、构造 Node
            INSTANCE.setup(true, environment);

            try {
                // keystore 中的安全配置必须在 Node 构造期间读取完毕，之后关闭 keystore
                IOUtils.close(keystore);
            } catch (IOException e) {
                throw new BootstrapException(e);
            }

            // 【步骤8 — 启动】调用 Node.start() 启动所有服务，然后启动 keepAlive 线程
            INSTANCE.start();

            // We don't close stderr if `--quiet` is passed, because that
            // hides fatal startup errors. For example, if Elasticsearch is
            // running via systemd, the init script only specifies
            // `--quiet`, not `-d`, so we want users to be able to see
            // startup errors via journalctl.
            if (foreground == false) {
                closeSysError();
            }
        } catch (NodeValidationException | RuntimeException e) {
            // disable console logging, so user does not see the exception twice (jvm will show it already)
            final Logger rootLogger = LogManager.getRootLogger();
            final Appender maybeConsoleAppender = Loggers.findAppender(rootLogger, ConsoleAppender.class);
            if (foreground && maybeConsoleAppender != null) {
                Loggers.removeAppender(rootLogger, maybeConsoleAppender);
            }
            Logger logger = LogManager.getLogger(Bootstrap.class);
            // HACK, it sucks to do this, but we will run users out of disk space otherwise
            if (e instanceof CreationException) {
                // guice: log the shortened exc to the log file
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                PrintStream ps = null;
                try {
                    ps = new PrintStream(os, false, "UTF-8");
                } catch (UnsupportedEncodingException uee) {
                    assert false;
                    e.addSuppressed(uee);
                }
                new StartupException(e).printStackTrace(ps);
                ps.flush();
                try {
                    logger.error("Guice Exception: {}", os.toString("UTF-8"));
                } catch (UnsupportedEncodingException uee) {
                    assert false;
                    e.addSuppressed(uee);
                }
            } else if (e instanceof NodeValidationException) {
                logger.error("node validation exception\n{}", e.getMessage());
            } else {
                // full exception
                logger.error("Exception", e);
            }
            // re-enable it if appropriate, so they can see any logging during the shutdown process
            if (foreground && maybeConsoleAppender != null) {
                Loggers.addAppender(rootLogger, maybeConsoleAppender);
            }

            throw e;
        }
    }

    @SuppressForbidden(reason = "System#out")
    private static void closeSystOut() {
        System.out.close();
    }

    @SuppressForbidden(reason = "System#err")
    private static void closeSysError() {
        System.err.close();
    }

    private static void checkLucene() {
        if (Version.CURRENT.luceneVersion.equals(org.apache.lucene.util.Version.LATEST) == false) {
            throw new AssertionError("Lucene version mismatch this version of Elasticsearch requires lucene version ["
                + Version.CURRENT.luceneVersion + "]  but the current lucene version is [" + org.apache.lucene.util.Version.LATEST + "]");
        }
    }

}
