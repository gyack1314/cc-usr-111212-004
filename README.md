# 告警会话窗的晚到合并与撤回拆分

本项目保存湖仓治理的合成资料与原始配置。`lake_data.io`读取JSON清单和资产身份，`lake_data.domain`检查基本字段并给数量概况，`lake_data.sessions`在资料之上提供可更新的事件时间会话聚合与可重放窗口变更日志，`lake_data.cli`输出原资料摘要或驱动会话重放。字段、数据类型、时间和版本口径见[资料约定](docs/data-contract.md)。数据中的设备、班组、主体和电量均为虚构，不连接电网、生产数仓或人员系统，也不调用任何告警设备或真实处置接口。

Python 3.11及以上，无第三方Python依赖。在项目根目录运行：

```sh
python -m unittest discover -s tests -v
python -m compileall -q lake_data tests
python -m lake_data.cli inspect data/normal.json
python -m lake_data.cli replay data/normal.json
python -m lake_data.cli replay --check data/bridge-move-device.json
python -m lake_data.cli replay --store build/session-store data/normal.json
python -m lake_data.cli replay --resume --store build/session-store data/normal.json
```

`inspect`只显示原字段概况，不修改源资料。`replay`按清单顺序驱动会话引擎，原子产出每个输入的旧会话`retract`、新会话`upsert`与封存`seal`变更（顺序与全局变更序号稳定），并输出访问统计与检查点；遇到首个按批前状态判定应拒绝的修改即停止，退出码1，拒绝不消耗输入位置、不发变更。`invalid-*.json`是基本配置错误，其余JSON为有效清单但可含需要领域操作判断的边界资料。null与缺字段应按各字段约定区分，原文件摘要绑定实际字节。

## 会话聚合口径（`lake_data.sessions`）

* 事件由`event_id`标识；upsert携带设备、整数`event_time`与数值，delete只撤回该ID的已知版本。每ID revision严格递增：同版本同内容幂等、同版本异内容冲突。
* 每设备相邻事件时间差`<= gap`链式连通为同一会话，重复时刻是不同事件；会话区间`[start, last+gap)`，ID由设备与成员中最小event_id稳定编码，聚合`count/sum/min/max`并保留成员引用。桥事件删除或改设备会拆分旧会话，不是只支持不断合并。
* watermark由调用者显式单调推进，`end+allowed_lateness<=watermark`的会话封存；upsert/delete的接收判定统一按批前状态，凡触及已封存会话、或新事件自身`event_time+gap+lateness<=watermark`，整次修改拒绝，不能借新事件重开已封存会话。已封存事件保留身份与修订墓碑，旧ID不会被当作全新事件重新接收。
* `rebuild_device_sessions`与`SessionEngine.verify_rebuild`可独立从已接受事件重建每设备会话，用于对照在线索引。
* 访问统计说明局部性：事件修改只重算批前受影响会话覆盖的设备区间，统计项`rebuild_event_accesses`为实际访问事件数；watermark封存只走会话end小顶堆（`seal_heap_accesses`仅计弹出项），`event_history_scans`与`watermark_event_scans`恒为0，不扫描全部历史事件。

## 落盘与重放（`--store`）

`PersistentSessionEngine`（CLI的`--store`）把源事件、会话索引、变更日志与检查点作为同一事务持久化：新变更先追加`changes.jsonl`并fsync，再把含完整状态与日志字节偏移的`checkpoint.json`原子替换。崩溃在两步之间时，`resume`按检查点偏移截断未确认的日志尾部（含半行），重启不重复发变更；拒绝路径不做任何写入。`--resume`跳过检查点之前的输入位置续跑。

Dockerfile固定Linux基础镜像。`docker build -t lake-material .`构建，`docker run --rm lake-material`读取正常清单；`docker run --rm lake-material python -m unittest discover -s tests -v`执行已有检查。所有资料本地可读，无需账号和在线来源。
