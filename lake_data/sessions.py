"""可更新的事件时间会话聚合与可重放窗口变更日志。

口径见 docs/data-contract.md：

* 每设备相邻事件时间差 ``<= gap`` 链式连通为同一会话，重复时刻是不同事件；
  会话区间为 ``[start, last+gap)``，ID 由设备与成员中最小 event_id 稳定编码，
  聚合 count/sum/min/max 并保留成员引用。
* upsert/delete 携带每 ID 严格递增的 revision：同版本同内容幂等、异内容冲突；
  delete 只撤回该 ID 当前已知版本，删除后保留修订墓碑。
* watermark 由调用者显式单调推进，``end+allowed_lateness<=watermark`` 的会话
  封存。upsert/delete 的接收判定统一按批前状态：凡触及已封存会话、或新事件
  自身 ``event_time+gap+lateness<=watermark``，整次修改拒绝且不留任何痕迹。
* 每次输入原子产出旧会话 retract、新会话 upsert 与封存标记，变更序号全局稳定；
  失败不推进 revision 与输入位置。

访问局部性：事件修改只抽取批前受影响会话的成员，在对应设备的时间区间内重算；
watermark 封存按会话 end 维护小顶堆，只弹出到期项，不扫描历史事件
（stats 中 event_history_scans 恒为 0）。
"""
from __future__ import annotations

import heapq
import json
import os
from bisect import bisect_left, bisect_right, insort
from dataclasses import dataclass
from pathlib import Path

from .io import integer

RETRACT = 'retract'
UPSERT = 'upsert'
SEAL = 'seal'

_CHECKPOINT = 'checkpoint.json'
_CHANGELOG = 'changes.jsonl'


class RejectedOperation(ValueError):
    """按批前状态判定必须整体拒绝的输入；拒绝不改变任何状态与 revision。"""

    def __init__(self, reason, **context):
        super().__init__(reason)
        self.reason = reason
        self.context = context


class _Min:
    def __lt__(self, other):
        return True

    def __gt__(self, other):
        return False


class _Max:
    def __lt__(self, other):
        return False

    def __gt__(self, other):
        return True


_MIN = _Min()
_MAX = _Max()


def _signed_int(value, name):
    # bool 是 int 子类，按资料约定一并拒绝
    if type(value) is not int:
        raise ValueError(f'{name}应为整数')
    return value


def _non_empty_str(value, name):
    if not isinstance(value, str) or not value:
        raise ValueError(f'{name}缺失或非字符串')
    return value


def make_session_id(device, min_event_id):
    """会话 ID：设备 + 最小事件 ID 的 JSON 数组编码，无分隔符碰撞，按 Unicode 序排序。"""
    return json.dumps([device, min_event_id], ensure_ascii=False,
                      separators=(',', ':'), allow_nan=False)


@dataclass
class EventRecord:
    """事件身份与当前版本；active=False 为修订墓碑，sealed 身份永久冻结。"""

    event_id: str
    revision: int
    device: str
    time: int
    value: int
    active: bool = True
    sealed: bool = False

    def to_dict(self):
        return {
            'event_id': self.event_id,
            'revision': self.revision,
            'device': self.device,
            'time': self.time,
            'value': self.value,
            'active': self.active,
            'sealed': self.sealed,
        }

    @classmethod
    def from_dict(cls, d):
        return cls(d['event_id'], d['revision'], d['device'], d['time'],
                   d['value'], d['active'], d['sealed'])


@dataclass
class Session:
    session_id: str
    device: str
    members: list  # 按 (time, event_id) 排序的成员引用
    start: int
    last: int
    end: int  # last + gap，区间右开
    count: int
    sum: int
    min: int
    max: int
    sealed: bool = False

    @classmethod
    def from_records(cls, records, gap):
        """从同一设备、已按时间连通的一组活跃事件构造会话。"""
        rows = sorted(records, key=lambda r: (r.time, r.event_id))
        device = rows[0].device
        members = [r.event_id for r in rows]
        times = [r.time for r in rows]
        values = [r.value for r in rows]
        last = times[-1]
        return cls(
            session_id=make_session_id(device, min(members)),
            device=device,
            members=members,
            start=times[0],
            last=last,
            end=last + gap,
            count=len(rows),
            sum=sum(values),
            min=min(values),
            max=max(values),
        )

    def business_key(self):
        """除封存标记外的完整业务口径，用于判断 retract/upsert 是否完全相同。"""
        return (self.device, self.start, self.last, self.end,
                tuple(self.members), self.count, self.sum, self.min, self.max)

    def to_dict(self):
        return {
            'session_id': self.session_id,
            'device': self.device,
            'members': list(self.members),
            'start': self.start,
            'last': self.last,
            'end': self.end,
            'count': self.count,
            'sum': self.sum,
            'min': self.min,
            'max': self.max,
            'sealed': self.sealed,
        }

    @classmethod
    def from_dict(cls, d):
        return cls(d['session_id'], d['device'], list(d['members']),
                   d['start'], d['last'], d['end'], d['count'], d['sum'],
                   d['min'], d['max'], d['sealed'])


def componentize(records, gap):
    """把同设备活跃事件按相邻时间差 <= gap 链式切分为会话（纯函数）。"""
    rows = sorted(records, key=lambda r: (r.time, r.event_id))
    sessions = []
    current = []
    for rec in rows:
        if current and rec.time - current[-1].time > gap:
            sessions.append(Session.from_records(current, gap))
            current = []
        current.append(rec)
    if current:
        sessions.append(Session.from_records(current, gap))
    return sessions


def rebuild_device_sessions(active_events, gap):
    """独立从已接受事件重建每设备会话，可用于对照在线索引。

    只接受 active 事件（墓碑与封存历史由调用方过滤），返回
    ``{device: [Session, ...]}``，不依赖任何在线状态。
    """
    by_device = {}
    for rec in active_events:
        if not rec.active:
            raise ValueError('重建只接受已接受（active）事件')
        by_device.setdefault(rec.device, []).append(rec)
    return {
        device: componentize(rows, integer(gap, 0))
        for device, rows in sorted(by_device.items())
    }


def _zero_stats():
    # event_history_scans / watermark_event_scans 恒为 0：口径承诺，保留在统计中明示
    return {
        'operations_consumed': 0,       # 成功接收（含幂等重放）的输入数
        'operations_applied': 0,        # 产生状态处理的输入数
        'operations_idempotent': 0,     # 同版本同内容重放、未发变更
        'operations_rejected': 0,       # 被拒绝（拒绝不消耗输入位置）
        'rebuild_event_accesses': 0,    # 局部重算实际访问的事件数（仅受影响设备区间）
        'sessions_retracted': 0,
        'sessions_upserted': 0,
        'sessions_sealed': 0,
        'seal_heap_accesses': 0,        # 封存扫描只访问 end 堆弹出项
        'event_history_scans': 0,
        'watermark_event_scans': 0,
    }


class SessionEngine:
    """内存态事件时间会话聚合引擎，单线程、确定性。"""

    def __init__(self, gap, allowed_lateness, initial_watermark=-100,
                 max_active_events=10000):
        self.gap = integer(gap, 0)
        self.allowed_lateness = integer(allowed_lateness, 0)
        self.watermark = _signed_int(initial_watermark, 'initial_watermark')
        self.max_active_events = integer(max_active_events, 1)

        self.events = {}          # event_id -> EventRecord（含墓碑与封存身份）
        self.sessions = {}        # session_id -> Session
        self.eid_sid = {}         # active event_id -> session_id
        self.device_order = {}    # device -> [(time, event_id)] 有序
        self._seal_heap = []      # (end+lateness, session_id)，惰性校验

        self.change_seq = 0
        self.op_position = 0
        self.change_log = []
        self.stats = _zero_stats()

    # ------------------------------------------------------------------ 基础索引

    def _active_count(self):
        return sum(1 for rec in self.events.values() if rec.active)

    def _order_insert(self, rec):
        rows = self.device_order.setdefault(rec.device, [])
        insort(rows, (rec.time, rec.event_id))

    def _order_remove(self, rec):
        rows = self.device_order[rec.device]
        rows.pop(bisect_left(rows, (rec.time, rec.event_id)))
        if not rows:
            del self.device_order[rec.device]

    def _near_sessions(self, device, time):
        """批前状态下，新位置在 [time-gap, time+gap] 内能连通的全部会话。"""
        rows = self.device_order.get(device)
        if not rows:
            return {}
        lo = bisect_left(rows, (time - self.gap, _MIN))
        hi = bisect_right(rows, (time + self.gap, _MAX))
        found = {}
        for _, event_id in rows[lo:hi]:
            sid = self.eid_sid[event_id]
            found[sid] = self.sessions[sid]
        return found

    def _register(self, session):
        self.sessions[session.session_id] = session
        for event_id in session.members:
            self.eid_sid[event_id] = session.session_id
        heapq.heappush(
            self._seal_heap,
            (session.end + self.allowed_lateness, session.session_id),
        )

    def _unregister(self, session):
        self.sessions.pop(session.session_id, None)
        for event_id in session.members:
            self.eid_sid.pop(event_id, None)

    def _rebuild_sets(self, sets):
        """仅在给定设备的受影响事件集合内重算会话，不触碰集合外任何事件。"""
        new_sessions = []
        for device, event_ids in sets.items():
            # 去重按设备独立：同一事件改设备后只应在新设备集合内参与重算
            rows = [
                self.events[eid]
                for eid in set(event_ids)
                if self.events[eid].active and self.events[eid].device == device
            ]
            self.stats['rebuild_event_accesses'] += len(rows)
            for session in componentize(rows, self.gap):
                self._register(session)
                new_sessions.append(session)
        return new_sessions

    # ------------------------------------------------------------------ 封存

    def _sweep_sealed(self):
        """弹出所有 end+lateness<=watermark 的开放会话并封存，返回封存 ID 列表。"""
        sealed = []
        while self._seal_heap and self._seal_heap[0][0] <= self.watermark:
            seal_at, sid = heapq.heappop(self._seal_heap)
            self.stats['seal_heap_accesses'] += 1
            session = self.sessions.get(sid)
            # 会话已被撤回替换、或 end 已变化时堆项失效
            if session is None or session.sealed:
                continue
            if session.end + self.allowed_lateness != seal_at:
                continue
            session.sealed = True
            for event_id in session.members:
                self.events[event_id].sealed = True
            sealed.append(sid)
            self.stats['sessions_sealed'] += 1
        return sealed

    def _append_entries(self, entries):
        for entry in entries:
            self.change_seq += 1
            entry['seq'] = self.change_seq
            entry['op_index'] = self.op_position
            self.change_log.append(entry)

    def _emit(self, retracted, upserted):
        """一个输入的原子变更：retract（ID 序）→ upsert（ID 序）→ seal（ID 序）。"""
        r_map = {s.session_id: s for s in retracted}
        u_map = {s.session_id: s for s in upserted}
        # 同 ID 且业务口径完全相同：撤回与新建相互抵消，不发变更
        for sid in list(r_map):
            if sid in u_map and r_map[sid].business_key() == u_map[sid].business_key():
                del r_map[sid]
                del u_map[sid]
        entries = []
        for sid in sorted(r_map):
            entries.append({'kind': RETRACT, 'session_id': sid,
                            'session': r_map[sid].to_dict()})
        for sid in sorted(u_map):
            entries.append({'kind': UPSERT, 'session_id': sid,
                            'session': u_map[sid].to_dict()})
        self.stats['sessions_retracted'] += len(r_map)
        self.stats['sessions_upserted'] += len(u_map)
        for sid in sorted(self._sweep_sealed()):
            entries.append({'kind': SEAL, 'session_id': sid,
                            'watermark': self.watermark})
        self._append_entries(entries)
        return entries

    def _idempotent(self, touched):
        self.op_position += 1
        self.stats['operations_consumed'] += 1
        self.stats['operations_idempotent'] += 1
        return {'status': 'idempotent', 'changes': [],
                'touched_devices': touched}

    # ------------------------------------------------------------------ 输入

    def apply_upsert(self, event_id, revision, device, event_time, value):
        _non_empty_str(event_id, 'event_id')
        integer(revision, 1)
        _non_empty_str(device, 'device')
        time = _signed_int(event_time, 'time')
        _signed_int(value, 'value')

        rec = self.events.get(event_id)
        old = None
        if rec is not None:
            if rec.sealed:
                self.stats['operations_rejected'] += 1
                raise RejectedOperation('sealed-event', event_id=event_id)
            if revision < rec.revision:
                self.stats['operations_rejected'] += 1
                raise RejectedOperation('stale-revision', event_id=event_id,
                                        seen=rec.revision, given=revision)
            if revision == rec.revision:
                same = (rec.active and rec.device == device
                        and rec.time == time and rec.value == value)
                if same:
                    return self._idempotent([])
                self.stats['operations_rejected'] += 1
                raise RejectedOperation('revision-conflict', event_id=event_id,
                                        revision=revision)
            if rec.active:
                old = rec  # 活跃版本更新（可能改时间/改设备）
            elif self._active_count() >= self.max_active_events:
                self.stats['operations_rejected'] += 1
                raise RejectedOperation('active-limit',
                                        limit=self.max_active_events)
        elif self._active_count() >= self.max_active_events:
            self.stats['operations_rejected'] += 1
            raise RejectedOperation('active-limit',
                                    limit=self.max_active_events)

        # 新事件自身的晚到边界，按当前 watermark（批前状态）判定
        if time + self.gap + self.allowed_lateness <= self.watermark:
            self.stats['operations_rejected'] += 1
            raise RejectedOperation('event-too-late', event_id=event_id,
                                    time=time, watermark=self.watermark)

        # 批前受影响会话：旧位置会话 + 新位置 gap 连通会话；任一封存即整体拒绝
        affected = {}
        if old is not None:
            old_sid = self.eid_sid[event_id]
            affected[old_sid] = self.sessions[old_sid]
        for sid, session in self._near_sessions(device, time).items():
            affected[sid] = session
        if any(session.sealed for session in affected.values()):
            self.stats['operations_rejected'] += 1
            raise RejectedOperation('sealed-session-affected',
                                    event_id=event_id)

        # 受影响设备区间 = 批前受影响会话的成员集合，集合外会话保持不动
        rebuild_sets = {}
        for session in affected.values():
            rebuild_sets.setdefault(session.device, set()).update(session.members)
        rebuild_sets.setdefault(device, set()).add(event_id)
        retracted = list(affected.values())

        # 通过全部判定后才落地修改，保证失败不留痕迹
        if rec is None:
            rec = EventRecord(event_id, revision, device, time, value)
            self.events[event_id] = rec
        else:
            if old is not None:
                self._order_remove(rec)
            rec.revision = revision
            rec.device = device
            rec.time = time
            rec.value = value
            rec.active = True
        self._order_insert(rec)

        for session in retracted:
            self._unregister(session)
        upserted = self._rebuild_sets(rebuild_sets)
        entries = self._emit(retracted, upserted)

        self.op_position += 1
        self.stats['operations_consumed'] += 1
        self.stats['operations_applied'] += 1
        return {'status': 'applied', 'changes': entries,
                'touched_devices': sorted(rebuild_sets)}

    def apply_delete(self, event_id, revision):
        _non_empty_str(event_id, 'event_id')
        integer(revision, 1)

        rec = self.events.get(event_id)
        if rec is None:
            self.stats['operations_rejected'] += 1
            raise RejectedOperation('unknown-event', event_id=event_id)
        if rec.sealed:
            self.stats['operations_rejected'] += 1
            raise RejectedOperation('sealed-event', event_id=event_id)
        if not rec.active:
            # 修订墓碑：只接受同版本 delete 的幂等重放，其余任何 revision 拒绝
            if revision == rec.revision:
                return self._idempotent([])
            self.stats['operations_rejected'] += 1
            raise RejectedOperation('tombstone-revision', event_id=event_id,
                                    seen=rec.revision, given=revision)
        if revision < rec.revision:
            # 每 ID revision 严格递增：回退版本一律拒绝
            self.stats['operations_rejected'] += 1
            raise RejectedOperation('stale-revision', event_id=event_id,
                                    seen=rec.revision, given=revision)

        old_session = self.sessions[self.eid_sid[event_id]]
        if old_session.sealed:
            self.stats['operations_rejected'] += 1
            raise RejectedOperation('sealed-session-affected',
                                    event_id=event_id)
        rebuild_set = set(old_session.members)

        self._unregister(old_session)
        self._order_remove(rec)
        rec.active = False  # 保留身份与 revision 的修订墓碑
        rec.revision = revision  # delete 自带严格递增的 revision
        upserted = self._rebuild_sets({old_session.device: rebuild_set})
        entries = self._emit([old_session], upserted)

        self.op_position += 1
        self.stats['operations_consumed'] += 1
        self.stats['operations_applied'] += 1
        return {'status': 'applied', 'changes': entries,
                'touched_devices': [old_session.device]}

    def apply_watermark(self, value):
        watermark = _signed_int(value, 'watermark')
        if watermark <= self.watermark:
            self.stats['operations_rejected'] += 1
            raise RejectedOperation('watermark-not-advanced',
                                    seen=self.watermark, given=watermark)
        self.watermark = watermark
        # 只走 end 小顶堆：不扫描事件、不扫描全部会话
        entries = []
        for sid in sorted(self._sweep_sealed()):
            entries.append({'kind': SEAL, 'session_id': sid,
                            'watermark': watermark})
        self._append_entries(entries)

        self.op_position += 1
        self.stats['operations_consumed'] += 1
        self.stats['operations_applied'] += 1
        return {'status': 'applied', 'changes': entries,
                'touched_devices': []}

    # ------------------------------------------------------------------ 重建对照

    def verify_rebuild(self):
        """从全部已接受事件独立重建，逐设备对照在线会话索引，返回差异列表。"""
        rebuilt = rebuild_device_sessions(
            [r for r in self.events.values() if r.active], self.gap)
        current = {}
        for session in self.sessions.values():
            current.setdefault(session.device, set()).add(session.business_key())
        diffs = []
        for device in sorted(set(current) | set(rebuilt)):
            left = current.get(device, set())
            right = {s.business_key() for s in rebuilt.get(device, [])}
            if left != right:
                diffs.append({'device': device,
                              'missing_online': [
                                  {'members': list(key[3]), 'start': key[1],
                                   'end': key[2], 'count': key[4],
                                   'sum': key[5], 'min': key[6], 'max': key[7]}
                                  for key in sorted(right - left)],
                              'extra_online': [
                                  {'members': list(key[3]), 'start': key[1],
                                   'end': key[2], 'count': key[4],
                                   'sum': key[5], 'min': key[6], 'max': key[7]}
                                  for key in sorted(left - right)]})
        return diffs

    # ------------------------------------------------------------------ 快照

    def snapshot(self):
        return {
            'format': 'lake-session-checkpoint/1',
            'config': {
                'gap': self.gap,
                'allowed_lateness': self.allowed_lateness,
                'initial_watermark': self.watermark,
                'max_active_events': self.max_active_events,
            },
            'watermark': self.watermark,
            'op_position': self.op_position,
            'change_seq': self.change_seq,
            'stats': dict(self.stats),
            'events': [rec.to_dict() for rec in sorted(
                self.events.values(), key=lambda r: r.event_id)],
            'sessions': [s.to_dict() for s in sorted(
                self.sessions.values(), key=lambda s: s.session_id)],
        }

    @classmethod
    def from_snapshot(cls, snap):
        cfg = snap['config']
        engine = cls(cfg['gap'], cfg['allowed_lateness'],
                     snap['watermark'], cfg['max_active_events'])
        for d in snap['events']:
            rec = EventRecord.from_dict(d)
            engine.events[rec.event_id] = rec
            if rec.active:
                engine._order_insert(rec)
        for d in snap['sessions']:
            session = Session.from_dict(d)
            engine.sessions[session.session_id] = session
            for event_id in session.members:
                engine.eid_sid[event_id] = session.session_id
            if not session.sealed:
                heapq.heappush(
                    engine._seal_heap,
                    (session.end + engine.allowed_lateness, session.session_id),
                )
        engine.change_seq = snap['change_seq']
        engine.op_position = snap['op_position']
        engine.stats = dict(snap['stats'])
        return engine


def run_operations(engine, operations):
    """按清单顺序驱动引擎，遇到首个拒绝即停止；返回 (每输入结果, 拒绝或 None)。"""
    records = []
    for index, op in enumerate(operations):
        if index < engine.op_position:
            continue  # 检查点之后续跑
        kind = op.get('kind')
        try:
            if kind == 'upsert':
                result = engine.apply_upsert(
                    op['event_id'], op['revision'], op['device'],
                    op['time'], op['value'])
            elif kind == 'delete':
                result = engine.apply_delete(op['event_id'], op['revision'])
            elif kind == 'watermark':
                result = engine.apply_watermark(op['value'])
            else:
                raise ValueError('源操作未知')
        except RejectedOperation as exc:
            records.append({
                'op_index': index,
                'kind': kind,
                'status': 'rejected',
                'reason': exc.reason,
                'context': exc.context,
                'changes': [],
                'touched_devices': [],
            })
            return records, exc
        records.append({'op_index': index, 'kind': kind, **result})
    return records, None


class PersistentSessionEngine:
    """源事件、会话索引、变更日志与检查点的同事务落盘包装。

    提交流程（崩溃可恢复）：
      1. 新变更以 JSON Lines 追加到 changes.jsonl 并 fsync；
      2. 含完整状态与 change_log_bytes 的检查点写临时文件后原子 rename。
    崩溃在 1 之后 2 之前：重启按检查点偏移截断日志尾部，状态不变、不重发；
    崩溃在 2 之后：全部状态已持久。拒绝路径不做任何写入。
    """

    def __init__(self, directory, engine):
        self.directory = Path(directory)
        self.engine = engine
        self._log_path = self.directory / _CHANGELOG
        self._persisted_seq = engine.change_seq
        self._persisted_position = engine.op_position

    @classmethod
    def create(cls, directory, gap, allowed_lateness,
               initial_watermark=-100, max_active_events=10000):
        path = Path(directory)
        path.mkdir(parents=True, exist_ok=True)
        engine = SessionEngine(gap, allowed_lateness, initial_watermark,
                               max_active_events)
        handle = cls(path, engine)
        handle._commit()
        return handle

    @classmethod
    def resume(cls, directory):
        path = Path(directory)
        snap = json.loads((path / _CHECKPOINT).read_text(encoding='utf8'))
        engine = SessionEngine.from_snapshot(snap)
        offset = snap['change_log_bytes']
        log = path / _CHANGELOG
        if log.exists():
            size = log.stat().st_size
            if size < offset:
                raise IOError('变更日志短于检查点记录，状态不可信')
            if size > offset:
                # 撕掉已追加但检查点未确认的尾部（含半行）
                with open(log, 'r+b') as f:
                    f.truncate(offset)
            entries = []
            with open(log, encoding='utf8') as f:
                for line in f:
                    entries.append(json.loads(line))
        elif offset:
            raise IOError('检查点引用的变更日志缺失')
        else:
            entries = []
        # 日志序号必须与检查点状态连续一致，否则状态不可信
        if [entry['seq'] for entry in entries] != list(range(1, len(entries) + 1)):
            raise IOError('变更日志序号不连续，状态不可信')
        if len(entries) != engine.change_seq:
            raise IOError('变更日志条数与检查点 change_seq 不符')
        engine.change_log = entries
        handle = cls(path, engine)
        handle._persisted_seq = engine.change_seq
        handle._persisted_position = engine.op_position
        return handle

    def _commit(self):
        new_entries = self.engine.change_log[self._persisted_seq:]
        if new_entries:
            with open(self._log_path, 'a', encoding='utf8') as f:
                for entry in new_entries:
                    f.write(json.dumps(entry, ensure_ascii=False,
                                       allow_nan=False) + '\n')
                f.flush()
                os.fsync(f.fileno())
        offset = self._log_path.stat().st_size if self._log_path.exists() else 0
        snap = self.engine.snapshot()
        snap['change_log_bytes'] = offset
        tmp = self.directory / (_CHECKPOINT + '.tmp')
        with open(tmp, 'w', encoding='utf8') as f:
            json.dump(snap, f, ensure_ascii=False, allow_nan=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.directory / _CHECKPOINT)
        self._persisted_seq = len(self.engine.change_log)
        self._persisted_position = self.engine.op_position

    def apply_upsert(self, *args):
        result = self.engine.apply_upsert(*args)
        self._commit()
        return result

    def apply_delete(self, *args):
        result = self.engine.apply_delete(*args)
        self._commit()
        return result

    def apply_watermark(self, value):
        result = self.engine.apply_watermark(value)
        self._commit()
        return result

    @property
    def op_position(self):
        return self.engine.op_position

    @property
    def watermark(self):
        return self.engine.watermark

    @property
    def sessions(self):
        return self.engine.sessions

    @property
    def events(self):
        return self.engine.events

    @property
    def change_seq(self):
        return self.engine.change_seq

    @property
    def change_log(self):
        return self.engine.change_log

    @property
    def stats(self):
        return self.engine.stats

    def verify_rebuild(self):
        return self.engine.verify_rebuild()

    def snapshot(self):
        snap = self.engine.snapshot()
        snap['change_log_bytes'] = (
            self._log_path.stat().st_size if self._log_path.exists() else 0)
        return snap
