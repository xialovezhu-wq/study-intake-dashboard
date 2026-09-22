# 三科快速入库看板

这是一个只绑定本机回环地址的实时看板，固定按 408 → 数学 → 英语显示截止日内全部未正式入库项目。页面只读取安全投影；刷新不授权也不执行三科正式写入。

启动

双击 `scripts/start_realtime_dashboard.command`，或在终端运行：

```bash
cd '/Users/YOUR_USER/Documents/ChatGPT/三科快速入库看板'
PYTHONDONTWRITEBYTECODE=1 python3 scripts/realtime_server.py serve --host 127.0.0.1 --port 8767 --poll-seconds 1
```

然后打开 `http://127.0.0.1:8767/`。如果直接以 `file:` 打开生成的 `index.html`，页面会自动跳转到该本机地址。

登录后自动启动

用户级 LaunchAgent 安装在：

`/Users/YOUR_USER/Library/LaunchAgents/com.xiazhibin.three-subject-quick-intake-dashboard.plist`

其项目内权威副本位于：

`launchd/com.xiazhibin.three-subject-quick-intake-dashboard.plist`

它只启动本机回环只读 viewer，不自动打开浏览器，不调用三科正式 writer，也不访问或写入 T9。登录时由 `RunAtLoad` 启动；异常退出时由 `KeepAlive.SuccessfulExit=false` 恢复。

查看状态：

```bash
launchctl print gui/$(id -u)/com.xiazhibin.three-subject-quick-intake-dashboard
curl -fsS http://127.0.0.1:8767/healthz
```

受控重启：

```bash
launchctl kickstart -k gui/$(id -u)/com.xiazhibin.three-subject-quick-intake-dashboard
```

停用并卸载：

```bash
launchctl bootout gui/$(id -u)/com.xiazhibin.three-subject-quick-intake-dashboard
```

实时合同

- `GET /api/snapshot` 首先返回当前完整安全快照。
- `GET /api/stream` 通过 SSE 发送后续完整快照。
- `POST /api/refresh` 立即重建安全投影并在响应中返回快照。
- 服务每秒只检查权威源的轻量文件签名；签名未变化时不解析账本、不重新加载科目脚本，也不重建快照。
- 源变化经过短防抖后只重建发生变化的科目；多个 SSE 订阅者共用同一个检查与重建调度器。
- 单科投影失败时保留该科 last-known-good，其他科目继续更新；同一失败按 1、2、4、8、15、30 秒退避，新的源变化可立即解除退避。

交互与隐私

- 更新时保留已展开卡片和滚动位置；内容未变时不重建 DOM。
- 卡片同时以文字显示处理状态和残留状态，不依赖颜色传达含义。
- 页面只消费安全短标题、ID、hash、状态、时间和附件计数；不展示完整题干、解析、答案、英语全文、原始作答或私密对话。
- 44 px 最小操作高度、可见键盘焦点和 `prefers-reduced-motion` 适配均由模板保证。

验证

```bash
cd '/Users/YOUR_USER/Documents/ChatGPT/三科快速入库看板'
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v tests/test_live_ui_contract.py
```

安全边界

- 服务只允许 `127.0.0.1` 或 `localhost`，拒绝非本机 Host 和 Origin。
- 看板不修改三科正式账本、题卡、英语 bank 或 T9。
- 归档数据只通过已验证 pointer/locator 的精确路径重开，不扫描整个 T9。

正式入库自动化互斥

两小时正式入库自动化使用独立的任务级互斥工具：

```bash
python3 scripts/formal_intake_mutex.py acquire \
  --thread-id THREAD_ID --run-id RUN_ID --phase preflight
```

互斥状态位于 `runtime/three-subject-formal-intake.lock/`。成功获取后，调用方必须读取并保存回执顶层的 `owner_token`；`owner` 对象本身不包含令牌。在阶段切换时发送 heartbeat，并在本轮精确复验完成后用同一 token 释放：

```bash
python3 scripts/formal_intake_mutex.py heartbeat \
  --owner-token OWNER_TOKEN --phase math
python3 scripts/formal_intake_mutex.py release \
  --owner-token OWNER_TOKEN
```

已有有效锁且 owner 属于另一个任务时，新的自动化轮次只返回 `busy` 并退出，不运行任何 subject planner 或 writer。同一专用会话开始了新 run、但上一 run 的锁仍未释放时返回 `stale_lock`；缺失或损坏 owner 也返回 `stale_lock`。任何锁都不会按时间自动删除；必须先核实没有活动任务和未完成正式事务，再显式处理。状态输出不会暴露已有 owner token。

该工具只管理任务级互斥，不替代数学、408、英语各自的账本锁、RepoLock、WAL、CAS 或归档恢复合同，也不会自行启动正式入库。

验证：

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v tests/test_formal_intake_mutex.py
```
