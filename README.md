# 三科快速入库实时看板

读取数学、408、英语的 Capture 和流程状态，将尚未正式处理的资料展示为同一张本地看板。服务提供快照、健康检查与 SSE 更新，正式写入由学科入口负责。

## 结构与入口

- `scripts/realtime_server.py`：本地 HTTP 与 SSE 服务。
- `scripts/project_quick_intake_today.py`：将三科源状态映射为看板数据。
- `scripts/formal_intake_mutex.py`：相关操作互斥辅助。
- `index.html` 与 `templates/`：显示界面。

```sh
python3 scripts/realtime_server.py --help
```

请先配置代码中约定的本机学科路径，再按 `serve --help` 启动服务；公开版没有个人看板数据。原有流程细节保留在 `README.upstream.md`，其中日期和本机状态是历史背景，不代表公开副本已部署。

## 公开范围

这是从本机工作源码整理的公开副本。只包含代码、运行逻辑和必要结构，不包含真实学习记录、完整对话、学习画像、健康记录、原始教材、凭据、浏览器数据或运行数据库。路径中的 `YOUR_USER` 必须按本机环境配置。源码检查不代表已在另一台电脑部署成功；本次发布不启动服务、不写入真实学习库。
