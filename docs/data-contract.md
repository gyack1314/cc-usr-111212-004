# 会话状态时间口径

time和watermark为有符号整数，value为有符号整数且无null，gap与allowed_lateness为非负整数（gap=0表示只有同刻事件连通）。ID按Unicode排序，会话ID由device加成员中最小event_id稳定编码（`lake_data.sessions.make_session_id`，JSON数组编码无分隔符碰撞）；end=last+gap，尽管区间右开，相邻差恰为gap仍在同一连通会话。初始无事件，watermark=-100。delete只接受已有活跃版本，删除后记修订墓碑；同ID未来较高revision可重新upsert但仍受封存身份限制，不能复活已封存ID。

## 版本与幂等

* 每ID revision严格递增。upsert：低于已见revision拒绝（stale-revision）；等于已见revision时内容完全相同则幂等（不发变更、不增加变更序号），内容不同拒绝（revision-conflict）；高于已见revision按新版本接收，可能改时间、改数值或改设备。
* delete活跃事件：revision低于当前拒绝（stale-revision），等于或高于当前均可撤回，墓碑记录delete携带的revision；对墓碑只接受同revision的delete重放（幂等），其余revision拒绝（tombstone-revision）。未知ID的delete拒绝（unknown-event）。
* 任何拒绝都不推进输入位置与revision，不写任何状态。

## 封存与拒绝（统一按批前状态）

影响会话由批前旧位置所在会话、移除后受影响组件以及新事件与原会话的gap连接确定；若任一所触及会话已满足`end+allowed_lateness<=watermark`（sealed-session-affected/sealed-event），整次修改拒绝。新事件自身若`event_time+gap+allowed_lateness<=watermark`（event-too-late）同样拒绝，即使它本会成为孤立会话；不能借新事件把已封存会话重新打开。删除/改设备后拆出的片段可能在同一次处理内到期封存。watermark操作只推进逻辑时间（必须严格变大）并按`end+allowed_lateness<=watermark`封存，不产生新业务事件。相同会话ID但成员/聚合改变仍发retract再upsert，业务口径完全相同不发。每次输入有独立operation序号用于幂等，同revision重放不增加变更序号。

## 变更日志与检查点

正式日志先按旧会话ID排序retract，再按新ID排序upsert，封存标记最后按ID；同一输入内变更序号连续，全局单调。检查点记录输入操作位置（op_position）、watermark、全部事件（含active墓碑与sealed身份）、全部会话、统计与变更日志字节偏移。对已封存会话不允许任何后续修订，即使修改会令其更晚结束。

## 局部重算与统计

事件修改只抽取批前受影响会话的成员集合，在对应设备的时间区间内重算（stats.rebuild_event_accesses为实际访问事件数，集合外会话不触碰）；watermark封存只访问按会话end维护的小顶堆弹出项（stats.seal_heap_accesses），stats.event_history_scans与stats.watermark_event_scans恒为0。`rebuild_device_sessions`与`SessionEngine.verify_rebuild`只依赖已接受事件即可独立重建每设备会话，与在线索引逐设备对照。
