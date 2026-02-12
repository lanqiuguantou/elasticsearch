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

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;
import joptsimple.util.PathConverter;
import org.elasticsearch.Build;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.env.Environment;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.node.NodeValidationException;

import java.io.IOException;
import java.nio.file.Path;
import java.security.Permission;
import java.security.Security;
import java.util.Arrays;
import java.util.Locale;

/**
 * This class starts elasticsearch.
 *
 * 【ES启动入口类】
 * 这是 Elasticsearch 的 JVM 入口类，继承自 EnvironmentAwareCommand（命令行框架）。
 * 整体调用链：main() → execute() → init() → Bootstrap.init()
 * 负责：解析命令行参数(-d/-p/-q/-V)、覆盖DNS缓存策略、安装临时SecurityManager、
 *       然后将控制权交给 Bootstrap 完成真正的节点初始化和启动。
 */
class Elasticsearch extends EnvironmentAwareCommand {

    private final OptionSpecBuilder versionOption;
    private final OptionSpecBuilder daemonizeOption;
    private final OptionSpec<Path> pidfileOption;
    private final OptionSpecBuilder quietOption;

    // visible for testing
    Elasticsearch() {
        super("Starts Elasticsearch", () -> {}); // we configure logging later so we override the base class from configuring logging
        versionOption = parser.acceptsAll(Arrays.asList("V", "version"),
            "Prints Elasticsearch version information and exits");
        daemonizeOption = parser.acceptsAll(Arrays.asList("d", "daemonize"),
            "Starts Elasticsearch in the background")
            .availableUnless(versionOption);
        pidfileOption = parser.acceptsAll(Arrays.asList("p", "pidfile"),
            "Creates a pid file in the specified path on start")
            .availableUnless(versionOption)
            .withRequiredArg()
            .withValuesConvertedBy(new PathConverter());
        quietOption = parser.acceptsAll(Arrays.asList("q", "quiet"),
            "Turns off standard output/error streams logging in console")
            .availableUnless(versionOption)
            .availableUnless(daemonizeOption);
    }

    /**
     * Main entry point for starting elasticsearch
     *
     * 【JVM main 入口 — 启动流程第一步】
     * 这是整个 Elasticsearch 进程的入口方法，JVM 启动后首先执行这里。
     * 执行顺序：
     *   1. overrideDnsCachePolicyProperties() — 覆盖 JVM 的 DNS 缓存策略
     *      读取 -Des.networkaddress.cache.ttl=60 和 -Des.networkaddress.cache.negative.ttl=10，
     *      设置到 java.security 属性中，控制 DNS 解析结果的缓存时间。
     *   2. 安装临时 SecurityManager — 这个 SM 放行所有权限（checkPermission 为空实现），
     *      目的是让 JVM 内部策略（如 DNS 缓存策略）按"有安全管理器"的路径执行，
     *      后续在 Bootstrap.setup() 中会替换为真正的、有权限限制的 SecurityManager。
     *   3. LogConfigurator.registerErrorListener() — 注册日志错误监听器
     *   4. 创建 Elasticsearch 命令行实例，解析参数并执行 execute() 方法
     *   5. 如果启动失败（status != OK），打印错误信息并退出
     */
    public static void main(final String[] args) throws Exception {
        // 【步骤1】覆盖 DNS 缓存策略，确保 ES 能及时感知 DNS 变更
        overrideDnsCachePolicyProperties();
        /*
         * 【步骤2】安装临时的全权限 SecurityManager
         * 我们希望 JVM 认为已经安装了安全管理器，这样 JVM 内部基于安全管理器存在与否的策略决策
         * （例如 DNS 缓存策略）会按照"有安全管理器"的路径执行。
         * 这个临时 SM 放行所有权限，后续会在 Bootstrap.setup() 中被替换为正式的 SM。
         */
        System.setSecurityManager(new SecurityManager() {

            @Override
            public void checkPermission(Permission perm) {
                // 放行所有权限，以便后续可以设置真正的安全管理器
            }

        });
        // 【步骤3】注册 Log4j2 错误监听器，捕获日志系统自身的错误
        LogConfigurator.registerErrorListener();
        // 【步骤4】创建命令行实例并执行（会调用 execute() → init() → Bootstrap.init()）
        final Elasticsearch elasticsearch = new Elasticsearch();
        int status = main(args, elasticsearch, Terminal.DEFAULT);
        // 【步骤5】启动失败时打印错误信息并退出 JVM
        if (status != ExitCodes.OK) {
            final String basePath = System.getProperty("es.logs.base_path");
            // 如果日志还没配置好，就没必要提示用户去看日志文件了
            if (basePath != null) {
                Terminal.DEFAULT.errorPrintln(
                    "ERROR: Elasticsearch did not exit normally - check the logs at "
                        + basePath
                        + System.getProperty("file.separator")
                        + System.getProperty("es.logs.cluster_name") + ".log"
                );
            }
            exit(status);
        }
    }

    /**
     * 【覆盖 DNS 缓存策略】
     * 读取 JVM 系统属性 -Des.networkaddress.cache.ttl 和 -Des.networkaddress.cache.negative.ttl，
     * 将其设置到 java.security 的对应属性中。
     * 这样可以控制 JVM 对 DNS 解析结果的正向/负向缓存时间（单位：秒）。
     * 在日志中可以看到 JVM 参数：-Des.networkaddress.cache.ttl=60, -Des.networkaddress.cache.negative.ttl=10
     * 即 DNS 正向缓存 60 秒，负向缓存（解析失败）10 秒。
     */
    private static void overrideDnsCachePolicyProperties() {
        for (final String property : new String[] {"networkaddress.cache.ttl", "networkaddress.cache.negative.ttl" }) {
            final String overrideProperty = "es." + property;
            final String overrideValue = System.getProperty(overrideProperty);
            if (overrideValue != null) {
                try {
                    // 先转为整数再转回字符串，确保值是合法的数字
                    Security.setProperty(property, Integer.toString(Integer.valueOf(overrideValue)));
                } catch (final NumberFormatException e) {
                    throw new IllegalArgumentException(
                            "failed to parse [" + overrideProperty + "] with value [" + overrideValue + "]", e);
                }
            }
        }
    }

    static int main(final String[] args, final Elasticsearch elasticsearch, final Terminal terminal) throws Exception {
        return elasticsearch.main(args, terminal);
    }

    /**
     * 【命令行执行入口 — 由 EnvironmentAwareCommand 框架回调】
     * main() 解析完命令行参数后，框架会回调此方法。
     * 这里处理 -V(版本)、-d(后台运行)、-p(pid文件)、-q(静默模式) 等选项，
     * 校验临时目录后，调用 init() 进入真正的启动流程。
     */
    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws UserException {
        if (options.nonOptionArguments().isEmpty() == false) {
            throw new UserException(ExitCodes.USAGE, "Positional arguments not allowed, found " + options.nonOptionArguments());
        }
        // 如果指定了 -V/--version，打印版本信息后直接返回，不启动节点
        if (options.has(versionOption)) {
            final String versionOutput = String.format(
                Locale.ROOT,
                "Version: %s, Build: %s/%s/%s/%s, JVM: %s",
                Build.CURRENT.getQualifiedVersion(),
                Build.CURRENT.flavor().displayName(),
                Build.CURRENT.type().displayName(),
                Build.CURRENT.hash(),
                Build.CURRENT.date(),
                JvmInfo.jvmInfo().version()
            );
            terminal.println(versionOutput);
            return;
        }

        final boolean daemonize = options.has(daemonizeOption);  // -d: 是否以守护进程方式运行
        final Path pidFile = pidfileOption.value(options);       // -p: PID 文件路径
        final boolean quiet = options.has(quietOption);          // -q: 是否关闭控制台输出

        // 校验 java.io.tmpdir 临时目录是否可用，配置错误会导致后续难以诊断的问题
        try {
            env.validateTmpFile();
        } catch (IOException e) {
            throw new UserException(ExitCodes.CONFIG, e.getMessage());
        }

        // 【关键调用】进入 init() → Bootstrap.init()，开始真正的节点初始化和启动
        try {
            init(daemonize, pidFile, quiet, env);
        } catch (NodeValidationException e) {
            throw new UserException(ExitCodes.CONFIG, e.getMessage());
        }
    }

    /**
     * 【桥接方法 — 连接命令行框架与 Bootstrap】
     * 将 daemonize 取反为 foreground 参数，调用 Bootstrap.init() 进入核心启动流程。
     * 如果 Bootstrap 抛出异常，包装为 StartupException 以避免 Guice 等框架产生巨大的堆栈输出。
     */
    void init(final boolean daemonize, final Path pidFile, final boolean quiet, Environment initialEnv)
        throws NodeValidationException, UserException {
        try {
            // !daemonize = foreground，即前台运行时 foreground=true
            Bootstrap.init(!daemonize, pidFile, quiet, initialEnv);
        } catch (BootstrapException | RuntimeException e) {
            // 格式化异常输出，避免 Guice 等框架产生 2MB 的堆栈信息
            throw new StartupException(e);
        }
    }

    /**
     * Required method that's called by Apache Commons procrun when
     * running as a service on Windows, when the service is stopped.
     *
     * http://commons.apache.org/proper/commons-daemon/procrun.html
     *
     * NOTE: If this method is renamed and/or moved, make sure to
     * update elasticsearch-service.bat!
     */
    static void close(String[] args) throws IOException {
        Bootstrap.stop();
    }

}
