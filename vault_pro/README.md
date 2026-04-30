# 吕梁 Vault PRO

一个本地化的二手房信息工作台：

- **前端**：单文件 `Vault PRO.html`，用 IndexedDB 持久化，纯本地，无需联网即可浏览/筛选/导出。
- **后端（可选启动）**：本地 Python FastAPI + Playwright，用于一键从安居客 / 58 同城吕梁站采集 50–100 条最新二手房信息，前端校对后批量入库。

仅供个人低频自用，请遵守各平台的服务条款与相关法律法规。

---

## 目录结构

```
vault_pro/
├── Vault PRO.html              # 前端（双击即可打开使用，离线可用）
├── backend/
│   ├── server.py               # FastAPI 入口
│   ├── tasks.py                # 异步任务状态机
│   ├── normalize.py            # 字段清洗 / 单价回算 / 去重 hash
│   └── collectors/
│       ├── base.py             # 采集器基类（Playwright 上下文 / cookie 持久化 / 节流）
│       ├── anjuke.py           # 安居客 lvliang.anjuke.com
│       └── tongcheng58.py      # 58 同城 lvliang.58.com
├── requirements.txt            # 见 backend/requirements.txt
├── start.bat                   # Windows 一键启动后端
└── start.sh                    # macOS / Linux 一键启动后端
```

---

## 快速开始

### A. 仅使用前端（不采集）

直接双击 `Vault PRO.html` 即可。已有的 5 条样例数据会自动从 localStorage 迁移进 IndexedDB。

### B. 启用「自动采集」

1. 安装 Python 3.10+，并确保命令行能 `python --version`。
2. **Windows**：双击 `start.bat`。  
   **macOS / Linux**：终端运行 `bash start.sh`。  
   首次启动会自动建虚拟环境、装依赖、装 Playwright 的 Chromium（一两百 MB，请耐心）。
3. 看到 `Uvicorn running on http://127.0.0.1:8765` 即后端就绪。
4. 打开 `Vault PRO.html`，点顶栏「自动采集」按钮。

### C. 首次采集需要人工过一次滑块/登录

安居客/58 近年都有较强风控。第一次采集时，会**弹出真实 Chromium 窗口**，你需要：

- 用手机扫码登录一次（或滑块验证一次）。
- 登录成功后窗口会自动进入列表页继续抓取。
- 登录态写入 `backend/.user_data/<platform>/`，下次直接静默运行。

### D. 多电脑共享使用（局域网）

如果你希望其它电脑也能打开并正常操作：

1. 在一台主机上启动后端（Windows 直接双击 `start.bat`）。现在后端会监听 `0.0.0.0:8765`。  
2. 在主机上再开一个终端，进入项目目录后运行：

   ```bash
   python -m http.server 8080
   ```

3. 在主机防火墙放行 `8080`（前端页面）和 `8765`（后端 API）入站访问。  
4. 其它电脑访问：`http://<主机IP>:8080/Vault%20PRO.html`。  
   前端会自动把后端地址识别为 `http://<主机IP>:8765`，可直接采集与操作。

> 说明：当前“房源主数据”仍保存在每台电脑浏览器自己的 IndexedDB（前端本地库），不是集中式数据库。  
> 因此网页文件能同步更新，但房源数据默认不会自动跨电脑实时同步。

---

## 法律与伦理边界（重要）

- 本项目的采集器**仅限个人低频自用**：每次最多 100 条、两次任务间隔不少于 5 分钟（前端硬编码节流）。
- 不要把数据二次传播、商用、批量下载平台数据。
- 各平台的反爬策略与页面 DOM 经常更新，采集器**不保证持续可用**；如失败请改用「手动新增」入库。
- 出现风控（持续滑块、频繁失败）请立即停止，等 24 小时后再试。

---

## 常见问题

- **后端启动时 pip 装不上**：检查 Python 版本是否 ≥ 3.10；公司网络可能要换源（阿里 / 清华）。
- **Playwright 装不上 Chromium**：手动 `python -m playwright install chromium`。
- **采集 0 条**：通常是登录态失效或选择器失效。删 `backend/.user_data/<platform>/` 再跑；仍不行则等更新。
- **安居客采到 0 条**：多半是验证码或风控拦截，请在弹窗浏览器先完成验证后重试。

---

## 已知灯下黑影（务必看一眼）

下面是开发时刻意没有自动化、需要你心里有数的点：

1. **首次采集建议人工过一次登录/滑块**。Playwright 启动的是真实 Chromium 窗口（`headful` 模式），安居客/58 常会要求验证后才能稳定看列表。通过之后 cookie 会持久化到 `backend/.user_data/<platform>/`，之后不必每次都验证。
   - 如果不希望弹出窗口，可设环境变量 `VAULT_PRO_HEADLESS=1`，但首次跑大概率会拿到 0 条。

2. **平台 DOM 改版会让选择器失效**。本项目把选择器写死在：
   - 安居客：`.property` / `.list-item` / `.house-list-item` 等多模板并行
   - 58：`.property`（最新模板）/ `.house-list-wrap > li`（中代）/ `.listUl > li`（旧）三套并列
   一旦平台改版，相应文件 `backend/collectors/anjuke.py`、`tongcheng58.py` 里的 CSS 选择器需要同步更新。

4. **节流是前端硬编码的**：两次「自动采集」之间至少 5 分钟，存在 `localStorage['vault_pro_cooldown_until']` 里。需要紧急再采可以在浏览器控制台 `localStorage.removeItem('vault_pro_cooldown_until')`。

5. **去重 hash 在前后端略有差异**：后端用 MD5 取前 10 位，前端用 32-bit FNV-1a。不影响功能（前端只用自己的 hash 在自己的 IndexedDB 里查重），但要知道这一点。

6. **「今日无更新」逻辑**：当后端拿到 0 条（要么平台真的没新房源、要么全被现有库去重命中），会在前端弹一个「是否扩大到全量翻页再采一次」的二次确认。点确认会自动切到 `不限时间` 重跑。

7. **数据备份建议**：IndexedDB 在浏览器清缓存时会丢，请定期点侧边栏的 **导出 JSON 备份**。后续若想做更稳健的导入恢复，可以参照 `db.addMany` 自行扩展。

8. **本工具仅供个人低频自用**。任何把数据二次发布、商用、提供给第三方的行为都可能违反平台 ToS 与相关法律法规，与本项目无关，请自行评估。
