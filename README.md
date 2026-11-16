# Paired FASTQ Re-pairing Service

测序湖入湖工具：接收顺序被打乱的双端未压缩 FASTQ（R1/R2），按读段号重新
配对，按 R1 顺序交付两份文件，原文逐字节保留。FastAPI + SQLite 实现，单文件
上限 256 MiB，进程内存上限 256 MiB，可处理总量超过内存的输入。

## 配对规则

- 读段号取头行首个空白前的字段，去掉开头 `@`，再去掉末尾的 `/1` 或 `/2`
  （例如 `@read-7/1 sample=x` → `read-7`）。
- 输出按 **R1 的原始顺序**排列；R2 按相同读段号对齐到同一位置。
- 四行记录原文（含头行描述、换行风格）原样写入结果，不做任何改写。

## 失败即整次失败，不开放部分结果

任一情况发生，任务整体标记为 `failed`/`cancelled`，已写入的暂存全部删除，
下载接口返回 409：

- 同一文件内读段号重复（含 `/1`、`/2` 归一化后重复）
- 孤端（某读段号只出现在一侧）
- 格式错误：非四行式、头行不以 `@` 开头、第三行不以 `+` 开头、空文件
- 序列与质量字符串长度不等

## 内存与大输入

- 上传经 `SpooledTemporaryFile` 落盘（超过 1 MiB 即写临时文件），分块复制，
  单文件流式校验 256 MiB 上限，超限返回 413。
- 解析按 512 条/批写入 SQLite（WAL、小 page cache、临时表走文件
  `temp_store=FILE`、关闭 mmap），任意时刻内存里只有一个批次。
- 配对发布使用 `JOIN ... ORDER BY a.ord` 流式游标，分批 `fetchmany` 写出。
- 因此输入总量可以远超 256 MiB；自测以约 393 MiB 总量（两份各 196.5 MiB、
  各 200 万条）验证，峰值 RSS 约 19 MiB（见下文自测结果）。

## 任务与取消语义

- `POST /tasks` 上传后立即返回 `202`，后台守护线程处理。
- `GET /tasks/{id}` 查询状态、阶段（upload/parse/validate/publish/done）与
  0–1 进度。查询使用短超时 + 有界重试，永远不会等满 SQLite busy timeout。
- `POST /tasks/{id}/cancel`：**在内存中先受理**（不需要任何数据库写锁），随后
  用短超时事务写 `cancelling`，因此即使另一条 SQLite 连接正持有写锁，取消也会
  在释锁前被受理并立即返回，响应与查询都会显示 `cancelling`。工作线程在下一
  个批次边界中止；取消与完成之间用条件 UPDATE + 每任务决策锁仲裁：
  - 取消先被受理 → 任务最终 `cancelled`，结果永不发布，暂存目录删除；
  - 完成先到 → 取消请求得到 `409`，明确告知任务已完成、不可取消，结果保留。
- 迟到的进度/成功写库带条件保护（`status NOT IN (...)`），不可能把任务从
  `cancelling`/终态改回 `processing` 或 `succeeded`。
- `cancelled`/`failed` 只在以下全部完成后才对外可见：工作线程已停止、暂存
  目录已删除、解析记录已删除。若写锁被占用导致收尾无法落库，任务暂时显示为
  `cancelling`（取消）或 `cleanup_pending`（失败），后台 reaper 每 0.5 秒重试，
  **释锁后自动完成**，不会留下无人处理的活动任务。超时的等待不会被“续期”成
  30 秒阻塞。
- 已成功的任务与下载不受影响：WAL 读在写锁被持有期间仍可用，`/download/r1`
  与 `/download/r2` 照常返回 200 与逐字节一致的内容。
- 取消后可立刻创建新任务；任务目录以 UUID 隔离，清理只删除本任务目录，
  不会误删新任务文件。
- 服务重启（启动钩子）：所有非终态任务收尾为 `failed`（若已请求取消则为
  `cancelled`），清理暂存与记录；`succeeded` 任务的结果文件保留，仍可下载。
  启动时若写锁被外部占用，也只会短暂等待，剩余收尾交给 reaper。

### 写锁争用下的行为（本次修复重点）

| 场景 | 修复前 | 修复后 |
| --- | --- | --- |
| 外部连接持有 SQLite 写锁时取消 | 等约 30 秒后报错 | 立即受理并返回 `cancelling` |
| 后台进度写入 | 等满 busy timeout 后抛错退出 | 短超时，跳过本次进度，继续处理 |
| 失败清理 | 写库超时，任务卡在 `processing` | 暂存立即删除，状态暂显示 `cleanup_pending`，释锁后自动 `failed` |
| 释锁后无人收尾 | 活动任务残留 | reaper 自动完成取消/失败收尾 |
| 已成功结果下载 | 受写锁影响 | 始终可用 |

实现要点：不靠延长 SQLite 等待来规避，而是把等待拆成“短超时 + 可取消的
有界重试”，并在内存中保存已受理的取消请求（`worker._pending_cancel_writes`
/`_deferred`）与按任务决策锁，最后交给后台 reaper 落库。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/tasks` | multipart 字段 `r1`、`r2`，返回任务对象（202） |
| GET  | `/tasks/{id}` | 状态/进度/消息/下载链接 |
| POST | `/tasks/{id}/cancel` | 请求取消（立即返回） |
| GET  | `/tasks/{id}/download/r1` | 下载配对后的 R1 |
| GET  | `/tasks/{id}/download/r2` | 下载配对后的 R2 |
| GET  | `/health` | 健康检查 |
| GET  | `/console` | 自带的操作页（离线可用，不依赖 CDN） |

启动后有两种可操作界面：

- <http://localhost:8000/console>：单文件自带的上传/查询/取消/双端下载操作页，
  JavaScript 全内联，**离线容器内也可用**；
- <http://localhost:8000/docs>：FastAPI 生成的 Swagger UI，可直接演示上传、
  取消、下载；它的前端资源来自 CDN，需要浏览器能访问外网。
  OpenAPI 描述本身在 <http://localhost:8000/openapi.json>。

## 运行（Docker Compose，内存上限 256 MiB）

```bash
docker compose up --build
# 打开 http://localhost:8000/console （离线可用）
# 或   http://localhost:8000/docs     （需 CDN）
```

`docker-compose.yml` 中 `mem_limit: 256m` 即交付要求的内存上限；数据保存在
命名卷 `fastq-data`（容器内 `/data`，含 SQLite 与各任务目录），重启后成功
结果仍可下载。

## 本地直接运行（开发/自测）

```bash
python3 -m venv --without-pip .venv
.venv/bin/python tools/get-pip.py
.venv/bin/pip install -r requirements.txt
FASTQ_DATA_DIR=./data .venv/bin/uvicorn app.main:app --port 8000
```

## 演示脚本

```bash
./scripts/demo.sh                 # 需要一个运行在 localhost:8000 的服务
BASE_URL=http://localhost:8000 ./scripts/demo.sh
```

脚本会生成一对乱序 FASTQ，演示上传 → 轮询进度 → 下载校验，并额外演示一次
取消与一次错误输入。

## 自测

```bash
# 短测试（功能/取消/重启语义 + 仲裁 + 写锁争用），共 81 项检查
bash scripts/run_tests.sh
# 或分别运行
.venv/bin/python -I tests/test_api.py              # 30 项：配对、整次失败、取消、重启恢复
.venv/bin/python -I tests/test_cancel_race.py      #  9 项：取消/完成条件 UPDATE 仲裁
.venv/bin/python -I tests/test_lock_contention.py  # 42 项：外部写锁下的取消/收尾/查询/下载

# 内存压力测试：两份各 196.5 MiB（合计约 393 MiB，超过 256 MiB 内存上限），
# 子进程实测峰值 RSS 约 19 MiB，并逐条校验配对顺序与原文
.venv/bin/python -I tests/test_memory.py 2000000
```

`tests/test_lock_contention.py` 用一条独立连接真正持有 SQLite 写锁，验证：
取消立即受理并显示 `cancelling`、查询与已成功任务下载仍可用、迟到的写库不能
把任务改回 `processing`/`succeeded`、释锁后自动收尾为 `cancelled`/`failed`、
暂存与读段明细清理完毕、启动恢复在锁被占用时也不会阻塞。

另有一个针对真实 HTTP 服务的验证脚本：

```bash
# 需要一个运行中的服务；DATA_DIR 指向该服务的数据目录
BASE_URL=http://localhost:8000 DATA_DIR=/tmp/fastq-live \
  bash scripts/live_lock_test.sh
```

## 目录结构

```
app/
  config.py    # 上限、路径、批大小、短超时/reaper 间隔等配置（环境变量可覆盖）
  db.py        # SQLite schema、短超时连接、条件 UPDATE、终态事务
  fastq.py     # 流式解析、校验、JOIN 配对发布（批写入可取消重试）
  worker.py    # 后台线程、决定锁仲裁、延迟收尾 reaper、启动恢复、目录清理
  main.py      # FastAPI 路由与有界流式上传
  static/console.html  # 自带操作页（离线可用）
tests/         # 功能自测、仲裁测试、写锁争用测试、内存压力测试
scripts/demo.sh, scripts/live_lock_test.sh
Dockerfile, docker-compose.yml, requirements.txt
```

## 遗留项 / 边界说明

- 仅接受未压缩 FASTQ；`.gz` 等压缩格式不在范围内。
- 服务为单实例 + 单 SQLite 文件设计；多实例横向扩展需要换用共享存储/队列。
- 没有内置任务 TTL 清理，成功结果会一直保留在数据卷中，需外部运维定期清理。
- 写锁被外部进程长期占用时，取消请求依然会被受理；任务在其间显示
  `cancelling`/`cleanup_pending`，暂存已在内存中标记清理、文件目录此时即删除，
  但终态行与记录删除要等 reaper 拿到写锁后落库。这是“先保证不发布、再保证
  可查询、最后落库”的取舍：释锁后自动完成，无需人工干预。
- 若进程被 `kill -9`，进行中的任务由下次启动恢复兜底（非终态 → `failed`，
  已请求取消 → `cancelled`），重启前 RSS/内存上限按 256 MiB 设计。
- 开发沙箱内未安装 Docker，`docker compose up` 未实跑；Dockerfile/compose
  已按可实际启动编写（`python:3.12-slim`、非 root 用户、健康检查、
  `mem_limit: 256m`、命名卷 `/data`）。已在隔离 venv 中严格按
  `requirements.txt` 安装并启动应用验证（`/console`、`/docs`、上传、查询、
  取消、双端下载、外部写锁下沉着取消、重启恢复、2M 条/393 MiB 内存压力）。
  在装有 Docker 的环境执行 `docker compose up --build` 即可，打开
  <http://localhost:8000/console>；`mem_limit: 256m` 即内存上限。
