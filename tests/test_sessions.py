"""可更新事件时间会话聚合：桥接、撤回拆分、改设备、封存边界与重放日志。"""
import json
import tempfile
import unittest
from pathlib import Path

from lake_data.sessions import (
    EventRecord,
    PersistentSessionEngine,
    RejectedOperation,
    RETRACT,
    UPSERT,
    SEAL,
    SessionEngine,
    make_session_id,
    rebuild_device_sessions,
)


def sids(result):
    return [(e['kind'], e['session_id']) for e in result['changes']]


def engine(gap=10, lateness=5, wm=-100, limit=10000):
    return SessionEngine(gap, lateness, wm, limit)


class SessionShapeTests(unittest.TestCase):
    def test_session_id_stable_and_sorted(self):
        sid = make_session_id('d1', 'a')
        self.assertEqual(json.loads(sid), ['d1', 'a'])
        self.assertEqual(sid, make_session_id('d1', 'a'))
        self.assertLess(make_session_id('d1', 'a'), make_session_id('d1', 'b'))

    def test_chain_connectivity_and_boundary_gap_equal(self):
        e = engine(gap=10)
        e.apply_upsert('a', 1, 'd1', 0, 1)
        e.apply_upsert('b', 1, 'd1', 10, 2)   # 差恰为 gap，连通
        e.apply_upsert('c', 1, 'd1', 21, 4)   # 差 11 > gap，另起会话
        self.assertEqual(set(e.sessions), {
            make_session_id('d1', 'a'), make_session_id('d1', 'c')})
        merged = e.sessions[make_session_id('d1', 'a')]
        self.assertEqual((merged.start, merged.last, merged.end), (0, 10, 20))
        self.assertEqual((merged.count, merged.sum, merged.min, merged.max),
                         (2, 3, 1, 2))
        self.assertEqual(merged.members, ['a', 'b'])

    def test_equal_time_distinct_events(self):
        e = engine(gap=0)
        e.apply_upsert('x', 1, 'd1', 5, 2)
        e.apply_upsert('y', 1, 'd1', 5, 3)
        e.apply_upsert('z', 1, 'd1', 6, 4)  # gap=0 下差 1 不连通
        sid = make_session_id('d1', 'x')
        self.assertEqual(e.sessions[sid].members, ['x', 'y'])
        self.assertEqual(e.sessions[sid].count, 2)
        self.assertEqual(set(e.sessions), {make_session_id('d1', 'x'),
                                           make_session_id('d1', 'z')})


class BridgeAndMoveTests(unittest.TestCase):
    def _two_islands(self):
        e = engine()
        e.apply_upsert('a', 1, 'd1', 0, 1)
        e.apply_upsert('b', 1, 'd1', 20, 2)
        return e

    def test_late_heartbeat_merges_then_delete_splits_back(self):
        e = self._two_islands()
        before = set(e.sessions)
        self.assertEqual(len(before), 2)
        # 迟到心跳在中间，把两段接成一个长会话
        r = e.apply_upsert('bridge', 1, 'd1', 10, 3)
        self.assertEqual([x[0] for x in sids(r)].count(RETRACT), 2)
        self.assertEqual(sids(r).count((UPSERT, make_session_id('d1', 'a'))), 1)
        self.assertEqual(len(e.sessions), 1)
        long_session = e.sessions[make_session_id('d1', 'a')]
        self.assertEqual(long_session.members, ['a', 'bridge', 'b'])
        self.assertEqual((long_session.sum, long_session.min, long_session.max),
                         (6, 1, 3))
        # 撤回误报：桥删除后必须拆回两段，而不是只支持不断合并
        r2 = e.apply_delete('bridge', 2)
        self.assertEqual(sids(r2)[0], (RETRACT, make_session_id('d1', 'a')))
        self.assertEqual(set(s[0] for s in sids(r2)[1:]), {UPSERT})
        self.assertEqual(set(e.sessions), before)
        sa = e.sessions[make_session_id('d1', 'a')]
        sb = e.sessions[make_session_id('d1', 'b')]
        self.assertEqual(sa.members, ['a'])
        self.assertEqual(sb.members, ['b'])
        # 墓碑保留身份与 delete 自带的严格递增 revision
        self.assertFalse(e.events['bridge'].active)
        self.assertEqual(e.events['bridge'].revision, 2)
        self.assertNotIn('bridge', e.eid_sid)

    def test_bridge_move_to_another_device_splits_old_and_seeds_new(self):
        from lake_data.sessions import _zero_stats
        e = self._two_islands()
        e.apply_upsert('bridge', 1, 'd1', 10, 3)
        e.stats = _zero_stats()  # 只统计本次改设备的重算量
        r = e.apply_upsert('bridge', 2, 'd2', 10, 3)
        kinds = sids(r)
        # d1 长会话撤回，d1 拆成两段，d2 新增一段
        self.assertIn((RETRACT, make_session_id('d1', 'a')), kinds)
        self.assertIn((UPSERT, make_session_id('d1', 'a')), kinds)
        self.assertIn((UPSERT, make_session_id('d1', 'b')), kinds)
        self.assertIn((UPSERT, make_session_id('d2', 'bridge')), kinds)
        self.assertEqual(set(e.sessions), {
            make_session_id('d1', 'a'), make_session_id('d1', 'b'),
            make_session_id('d2', 'bridge')})
        self.assertEqual(e.events['bridge'].device, 'd2')
        # 仅受影响设备区间：d1 残留 a、b 两个成员（桥已移走）+ d2 新桥
        self.assertEqual(e.stats['rebuild_event_accesses'], 2 + 1)

    def test_unrelated_device_untouched_by_rebuild(self):
        e = engine()
        e.apply_upsert('a', 1, 'd1', 0, 1)
        e.apply_upsert('z', 1, 'd9', 0, 9)
        e.apply_upsert('b', 1, 'd1', 10, 2)
        e.apply_upsert('m', 1, 'd1', 5, 3)
        e.apply_upsert('m2', 1, 'd2', 5, 3)  # 新事件落到另一设备，不占用旧 ID
        z_sid = make_session_id('d9', 'z')
        # d1 的桥合并不重算 d9
        self.assertTrue(z_sid in e.sessions)
        self.assertEqual(e.sessions[z_sid].members, ['z'])
        self.assertTrue(make_session_id('d2', 'm2') in e.sessions)


class RevisionTests(unittest.TestCase):
    def test_same_revision_same_content_idempotent_no_new_change(self):
        e = engine()
        r1 = e.apply_upsert('a', 1, 'd1', 0, 1)
        seq_after = e.change_seq
        r2 = e.apply_upsert('a', 1, 'd1', 0, 1)
        self.assertEqual(r2['status'], 'idempotent')
        self.assertEqual(e.change_seq, seq_after)
        self.assertEqual(e.op_position, 2)
        self.assertEqual(len(e.change_log), 1)
        # 墓碑同版本 delete 重放幂等（当前活跃版本 revision=1）
        e.apply_delete('a', 1)
        e.apply_delete('a', 1)
        self.assertFalse(e.events['a'].active)
        self.assertEqual(e.stats['operations_idempotent'], 2)

    def test_same_revision_different_content_conflict(self):
        e = engine()
        e.apply_upsert('a', 1, 'd1', 0, 1)
        with self.assertRaises(RejectedOperation) as ctx:
            e.apply_upsert('a', 1, 'd1', 1, 1)
        self.assertEqual(ctx.exception.reason, 'revision-conflict')
        # 失败不推进 revision，原状态不变
        self.assertEqual(e.events['a'].time, 0)
        self.assertEqual(len(e.change_log), 1)

    def test_revision_must_increase_and_delete_known_version_only(self):
        e = engine()
        with self.assertRaises(RejectedOperation) as ctx:
            e.apply_delete('a', 1)
        self.assertEqual(ctx.exception.reason, 'unknown-event')
        e.apply_upsert('a', 3, 'd1', 0, 1)
        with self.assertRaises(RejectedOperation) as ctx:
            e.apply_upsert('a', 2, 'd1', 0, 1)
        self.assertEqual(ctx.exception.reason, 'stale-revision')
        with self.assertRaises(RejectedOperation) as ctx:
            e.apply_delete('a', 2)
        self.assertEqual(ctx.exception.reason, 'stale-revision')
        # delete 携带下一个严格递增 revision 撤回已知版本，墓碑记录该 revision
        e.apply_delete('a', 4)
        self.assertEqual(e.events['a'].revision, 4)
        # 墓碑只接受同版本重放；其他任何 revision 都由墓碑分支拒绝
        with self.assertRaises(RejectedOperation) as ctx:
            e.apply_delete('a', 3)
        self.assertEqual(ctx.exception.reason, 'tombstone-revision')
        with self.assertRaises(RejectedOperation) as ctx:
            e.apply_delete('a', 5)
        self.assertEqual(ctx.exception.reason, 'tombstone-revision')
        e.apply_delete('a', 4)  # 同版本重放幂等
        # 墓碑后更高 revision 可重新 upsert（链条严格递增）
        r = e.apply_upsert('a', 5, 'd1', 0, 1)
        self.assertEqual(r['status'], 'applied')
        self.assertTrue(e.events['a'].active)
        self.assertEqual(e.events['a'].revision, 5)

    def test_failed_modification_advances_nothing(self):
        e = engine()
        e.apply_upsert('a', 1, 'd1', 0, 1)
        with self.assertRaises(RejectedOperation):
            e.apply_upsert('a', 1, 'd1', 0, 2)
        self.assertEqual(e.op_position, 1)
        self.assertEqual(e.stats['operations_rejected'], 1)


class SealingTests(unittest.TestCase):
    def test_equality_seal_boundary(self):
        # end=0+10=10, end+lateness=15：wm=14 未封存，wm=15 恰封存
        e = engine(wm=-100)
        e.apply_upsert('a', 1, 'd1', 0, 1)
        e.apply_watermark(14)
        self.assertFalse(e.sessions[make_session_id('d1', 'a')].sealed)
        e.apply_watermark(15)
        self.assertTrue(e.sessions[make_session_id('d1', 'a')].sealed)
        self.assertTrue(e.events['a'].sealed)

    def test_delete_of_sealed_rejected(self):
        e = engine()
        e.apply_upsert('a', 1, 'd1', 0, 1)
        e.apply_watermark(15)
        with self.assertRaises(RejectedOperation) as ctx:
            e.apply_delete('a', 2)
        self.assertEqual(ctx.exception.reason, 'sealed-event')
        self.assertTrue(e.events['a'].active)

    def test_touching_sealed_session_via_new_bridge_rejected(self):
        e = engine()
        e.apply_upsert('a', 1, 'd1', 0, 1)
        e.apply_upsert('b', 1, 'd1', 30, 2)
        e.apply_watermark(15)  # 只封存 a
        # 新桥若接收会重开/影响已封存 a：按批前状态整体拒绝
        with self.assertRaises(RejectedOperation) as ctx:
            e.apply_upsert('bridge', 1, 'd1', 10, 3)
        self.assertEqual(ctx.exception.reason, 'sealed-session-affected')
        self.assertNotIn('bridge', e.events)
        self.assertEqual(e.sessions[make_session_id('d1', 'a')].sealed, True)

    def test_event_too_late_boundary(self):
        e = engine()
        e.apply_watermark(16)
        # time+gap+lateness=15 < 16：已过晚（<= watermark）
        with self.assertRaises(RejectedOperation) as ctx:
            e.apply_upsert('late', 1, 'd1', 0, 1)
        self.assertEqual(ctx.exception.reason, 'event-too-late')
        # time=1 -> 1+10+5=16 恰等于 watermark 同样过晚
        with self.assertRaises(RejectedOperation) as ctx:
            e.apply_upsert('late2', 1, 'd1', 1, 1)
        self.assertEqual(ctx.exception.reason, 'event-too-late')
        # 差 1 仍可接收
        r = e.apply_upsert('ok', 1, 'd1', 2, 1)
        self.assertEqual(r['status'], 'applied')

    def test_open_sibling_session_remains_mutable(self):
        e = engine()
        e.apply_upsert('a', 1, 'd1', 0, 1)
        e.apply_upsert('b', 1, 'd1', 30, 2)
        e.apply_watermark(15)
        # b 开放，可在其附近桥接新事件（不触及 a）
        r = e.apply_upsert('c', 1, 'd1', 22, 3)
        self.assertEqual(r['status'], 'applied')
        self.assertEqual(
            e.sessions[make_session_id('d1', 'b')].members, ['c', 'b'])

    def test_split_components_can_seal_in_same_batch(self):
        # 删除桥后拆出的早期片段已到封存边界：同一输入内发出 upsert 再 seal
        e = engine()
        e.apply_upsert('a', 1, 'd1', 0, 1)
        e.apply_upsert('bridge', 1, 'd1', 10, 3)
        e.apply_upsert('b', 1, 'd1', 20, 2)
        e.apply_watermark(14)  # 长会话 end=30 -> 35>14 未封存
        # 拆后 a 段 end=10 -> 10+5=15 > 14，仍开放；推进到 15 后再拆
        e.apply_watermark(15)
        self.assertFalse(e.sessions[make_session_id('d1', 'a')].sealed)
        entries = e.apply_delete('bridge', 2)['changes']
        kinds = [(x['kind'], x['session_id']) for x in entries]
        self.assertEqual(kinds[-1], (SEAL, make_session_id('d1', 'a')))
        self.assertTrue(e.sessions[make_session_id('d1', 'a')].sealed)
        self.assertFalse(e.sessions[make_session_id('d1', 'b')].sealed)
        # 封存片段内事件身份冻结，再删 a 被拒
        with self.assertRaises(RejectedOperation) as ctx:
            e.apply_delete('a', 2)
        self.assertEqual(ctx.exception.reason, 'sealed-event')

    def test_watermark_monotonic(self):
        e = engine()
        e.apply_watermark(0)
        with self.assertRaises(RejectedOperation):
            e.apply_watermark(0)
        with self.assertRaises(RejectedOperation):
            e.apply_watermark(-1)

    def test_sealed_identity_never_reborn_as_fresh(self):
        e = engine()
        e.apply_upsert('a', 1, 'd1', 0, 1)
        e.apply_watermark(15)
        with self.assertRaises(RejectedOperation) as ctx:
            e.apply_upsert('a', 2, 'd1', 0, 9)
        self.assertEqual(ctx.exception.reason, 'sealed-event')
        self.assertEqual(e.events['a'].revision, 1)


class ChangeLogOrderTests(unittest.TestCase):
    def test_retract_then_upsert_then_seal_and_seq_stable(self):
        e = engine()
        e.apply_upsert('b', 1, 'd1', 20, 2)
        e.apply_upsert('a', 1, 'd1', 0, 1)
        r = e.apply_upsert('m', 1, 'd1', 10, 3)
        entries = r['changes']
        self.assertTrue(all(entries[i]['seq'] < entries[i + 1]['seq']
                            for i in range(len(entries) - 1)))
        positions = {'retract': [], 'upsert': [], 'seal': []}
        for entry in entries:
            positions[entry['kind']].append(entry['seq'])
        self.assertTrue(all(u > max(positions['retract'], default=0)
                            for u in positions['upsert']))
        self.assertTrue(all(s > max(positions['upsert'], default=0)
                            for s in positions['seal']))
        self.assertEqual([x['session_id'] for x in entries
                          if x['kind'] == RETRACT],
                         sorted(x['session_id'] for x in entries
                                if x['kind'] == RETRACT))

    def test_identical_session_emits_nothing(self):
        e = engine()
        e.apply_upsert('a', 1, 'd1', 0, 1)
        # 更高 revision 但内容完全一致：业务口径无变化，不发窗口变更
        r = e.apply_upsert('a', 2, 'd1', 0, 1)
        self.assertEqual(r['changes'], [])
        self.assertEqual(e.stats['sessions_retracted'], 0)
        self.assertEqual(e.events['a'].revision, 2)

    def test_watermark_without_sealing_advances_position_only(self):
        e = engine()
        e.apply_upsert('a', 1, 'd1', 100, 1)
        seq = e.change_seq
        r = e.apply_watermark(0)  # 远早于 end+lateness，无会话到期
        self.assertEqual(r['changes'], [])
        self.assertEqual(e.change_seq, seq)
        self.assertEqual(e.op_position, 2)
        self.assertEqual(e.watermark, 0)
        self.assertEqual(e.stats['sessions_sealed'], 0)

    def test_watermark_change_entries_only_seal_markers(self):
        e = engine()
        e.apply_upsert('a', 1, 'd1', 0, 1)
        r = e.apply_watermark(15)
        self.assertTrue(all(x['kind'] == SEAL for x in r['changes']))
        self.assertEqual(r['changes'][0]['watermark'], 15)


class RebuildTests(unittest.TestCase):
    def test_pure_rebuild_matches_online_index_after_chaos(self):
        e = engine(gap=7, lateness=3)
        ops = [
            ('u', 'a', 1, 'd1', 0, 1), ('u', 'b', 1, 'd1', 8, 2),
            ('u', 'c', 1, 'd2', 5, 4), ('u', 'm', 1, 'd1', 6, 9),
            ('u', 'm', 2, 'd2', 6, 9), ('d', 'b', 2),
            ('u', 'd', 1, 'd1', 9, 7), ('u', 'e', 1, 'd2', 20, 1),
            ('u', 'f', 1, 'd3', 100, -5), ('d', 'c', 2),
            ('u', 'g', 1, 'd1', 9, 2),
        ]
        for op in ops:
            if op[0] == 'u':
                e.apply_upsert(op[1], op[2], op[3], op[4], op[5])
            else:
                e.apply_delete(op[1], op[2])
            self.assertEqual(e.verify_rebuild(), [])
        rebuilt = rebuild_device_sessions(
            [r for r in e.events.values() if r.active], 7)
        online = {}
        for s in e.sessions.values():
            online.setdefault(s.device, set()).add(s.business_key())
        for device, sessions in rebuilt.items():
            self.assertEqual({s.business_key() for s in sessions},
                             online[device])

    def test_rebuild_refuses_tombstones(self):
        with self.assertRaises(ValueError):
            rebuild_device_sessions(
                [EventRecord('a', 1, 'd1', 0, 1, active=False)], 10)


class StatsTests(unittest.TestCase):
    def test_watermark_never_scans_event_history(self):
        e = engine()
        for i in range(50):
            e.apply_upsert(f'e{i:02d}', 1, 'd1', i * 100, i)
        e.apply_watermark(5_000)
        self.assertEqual(e.stats['event_history_scans'], 0)
        self.assertEqual(e.stats['watermark_event_scans'], 0)
        # 堆访问只触及实际弹出的封存会话
        self.assertLessEqual(e.stats['seal_heap_accesses'],
                             e.stats['sessions_sealed'] + 50)
        # 局部重算：单点 upsert 只访问受影响会话的少量成员
        self.assertGreater(e.stats['rebuild_event_accesses'], 0)
        self.assertLess(e.stats['rebuild_event_accesses'], 50 * 50)

    def test_active_limit(self):
        e = engine(limit=2)
        e.apply_upsert('a', 1, 'd1', 0, 1)
        e.apply_upsert('b', 1, 'd2', 0, 1)
        with self.assertRaises(RejectedOperation) as ctx:
            e.apply_upsert('c', 1, 'd3', 0, 1)
        self.assertEqual(ctx.exception.reason, 'active-limit')
        # 更新与删除不占用新名额（delete 用下一个严格递增 revision）
        e.apply_upsert('a', 2, 'd1', 0, 8)
        e.apply_delete('b', 2)
        e.apply_upsert('c', 1, 'd3', 0, 1)


class PersistenceTests(unittest.TestCase):
    def test_commit_and_resume_replays_without_duplicate_changes(self):
        with tempfile.TemporaryDirectory() as d:
            store = PersistentSessionEngine.create(d, 10, 5)
            store.apply_upsert('a', 1, 'd1', 0, 1)
            store.apply_upsert('b', 1, 'd1', 20, 2)
            store.apply_upsert('bridge', 1, 'd1', 10, 3)
            store.apply_watermark(0)
            seq = store.change_seq
            position = store.op_position
            log_bytes = (Path(d) / 'changes.jsonl').stat().st_size

            resumed = PersistentSessionEngine.resume(d)
            self.assertEqual(resumed.change_seq, seq)
            self.assertEqual(resumed.op_position, position)
            self.assertEqual(set(resumed.engine.sessions), set(store.engine.sessions))
            self.assertEqual(resumed.change_log, store.change_log)
            self.assertEqual(resumed.verify_rebuild(), [])
            # 续跑产生新序号，不重复旧变更
            resumed.apply_delete('bridge', 2)
            self.assertEqual(resumed.change_seq, seq + 3)  # retract + 2 upsert
            self.assertEqual(resumed.op_position, position + 1)
            self.assertEqual((Path(d) / 'changes.jsonl').stat().st_size, log_bytes
                             + sum(len(json.dumps(x, ensure_ascii=False)) + 1
                                   for x in resumed.change_log[seq:]))

    def test_resume_then_continue_business_flow(self):
        with tempfile.TemporaryDirectory() as d:
            store = PersistentSessionEngine.create(d, 10, 5)
            store.apply_upsert('a', 1, 'd1', 0, 1)
            store.apply_upsert('b', 1, 'd1', 20, 2)
            position = store.op_position
            resumed = PersistentSessionEngine.resume(d)
            self.assertEqual(resumed.op_position, position)
            # 续跑桥接合并，检查点与日志继续追加，序号不重复
            r = resumed.apply_upsert('bridge', 1, 'd1', 10, 3)
            self.assertTrue(any(x['kind'] == UPSERT for x in r['changes']))
            self.assertEqual(len(resumed.sessions), 1)
            again = PersistentSessionEngine.resume(d)
            self.assertEqual(again.change_log, resumed.change_log)
            self.assertEqual(again.verify_rebuild(), [])

    def test_torn_tail_after_crash_is_trimmed_on_resume(self):
        with tempfile.TemporaryDirectory() as d:
            store = PersistentSessionEngine.create(d, 10, 5)
            store.apply_upsert('a', 1, 'd1', 0, 1)
            log = Path(d) / 'changes.jsonl'
            good_bytes = log.stat().st_size
            # 模拟“追加日志后、检查点原子替换前”崩溃：追加一条脏尾
            with open(log, 'a', encoding='utf8') as f:
                f.write('{"kind":"seal",半行')
            resumed = PersistentSessionEngine.resume(d)
            self.assertEqual(log.stat().st_size, good_bytes)
            self.assertEqual(resumed.op_position, 1)
            self.assertEqual(resumed.change_seq, 1)

    def test_rejected_operation_writes_nothing(self):
        with tempfile.TemporaryDirectory() as d:
            store = PersistentSessionEngine.create(d, 10, 5)
            store.apply_upsert('a', 1, 'd1', 0, 1)
            cp_before = (Path(d) / 'checkpoint.json').read_bytes()
            log_before = (Path(d) / 'changes.jsonl').read_bytes()
            with self.assertRaises(RejectedOperation):
                store.apply_upsert('a', 1, 'd1', 0, 2)
            self.assertEqual((Path(d) / 'checkpoint.json').read_bytes(), cp_before)
            self.assertEqual((Path(d) / 'changes.jsonl').read_bytes(), log_before)

    def test_checkpoint_carries_full_state(self):
        with tempfile.TemporaryDirectory() as d:
            store = PersistentSessionEngine.create(d, 10, 5)
            store.apply_upsert('a', 1, 'd1', 0, 1)
            store.apply_delete('a', 2)
            store.apply_watermark(15)
            snap = json.loads((Path(d) / 'checkpoint.json').read_text(encoding='utf8'))
            self.assertEqual(snap['op_position'], 3)
            # 墓碑身份与封存标记都在检查点中
            self.assertEqual(snap['events'][0]['active'], False)
            self.assertIn('stats', snap)


class FixtureReplayTests(unittest.TestCase):
    def test_normal_fixture_full_trace(self):
        e = engine()
        from lake_data.io import load
        v, _ = load('data/normal.json')
        results, rejection = _run_all(e, v['operations'])
        # 最后一条 late：time=5 -> 5+10+5=20 <= wm=30，按口径拒绝且不留状态
        self.assertIsNotNone(rejection)
        self.assertEqual(rejection.reason, 'event-too-late')
        # a 段已封存、b 段开放
        self.assertTrue(e.sessions[make_session_id('d1', 'a')].sealed)
        self.assertFalse(e.sessions[make_session_id('d1', 'b')].sealed)
        self.assertNotIn('late', e.events)
        self.assertTrue(e.events['bridge'].active is False)
        self.assertEqual(e.verify_rebuild(), [])
        # 变更日志序号连续且全部带输入序号
        self.assertEqual([x['seq'] for x in e.change_log],
                         list(range(1, len(e.change_log) + 1)))
        self.assertTrue(all('op_index' in x for x in e.change_log))
        # 拒绝不消耗输入位置：停在 late（索引 6）之前
        self.assertEqual(e.op_position, 6)

    def test_sealed_delete_fixture_rejects(self):
        from lake_data.io import load
        v, _ = load('data/sealed-delete-reject.json')
        e = SessionEngine(v['gap'], v['allowed_lateness'])
        _, rejection = _run_all(e, v['operations'])
        self.assertIsNotNone(rejection)
        self.assertEqual(rejection.reason, 'sealed-event')
        # 拒绝后输入位置停在 delete 之前
        self.assertEqual(e.op_position, 2)
        self.assertTrue(e.events['a'].active)

    def test_bridge_move_fixture(self):
        from lake_data.io import load
        v, _ = load('data/bridge-move-device.json')
        e = SessionEngine(v['gap'], v['allowed_lateness'])
        _, rejection = _run_all(e, v['operations'])
        self.assertIsNone(rejection)
        self.assertEqual(e.events['bridge'].device, 'd2')
        self.assertEqual(set(e.sessions), {
            make_session_id('d1', 'a'), make_session_id('d1', 'b'),
            make_session_id('d2', 'bridge')})
        self.assertEqual(e.verify_rebuild(), [])


def _run_all(engine, operations):
    """与 sessions.run_operations 等价的本地驱动，失败即停。"""
    from lake_data.sessions import run_operations
    return run_operations(engine, operations)


class CliReplayTests(unittest.TestCase):
    def test_replay_command_outputs_and_persistent_store(self):
        import json as _json
        import subprocess
        import sys
        with tempfile.TemporaryDirectory() as d:
            store = str(Path(d) / 'store')
            proc = subprocess.run(
                [sys.executable, '-m', 'lake_data.cli', 'replay', '--check',
                 '--store', store, 'data/bridge-move-device.json'],
                cwd='/workspace', capture_output=True, text=True)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            payload = _json.loads(proc.stdout)
            self.assertEqual(payload['rejection'], None)
            self.assertEqual(payload['rebuild_diff'], [])
            self.assertTrue((Path(store) / 'checkpoint.json').exists())
            self.assertTrue((Path(store) / 'changes.jsonl').exists())

    def test_replay_command_exit_nonzero_on_rejection(self):
        import subprocess
        import sys
        proc = subprocess.run(
            [sys.executable, '-m', 'lake_data.cli', 'replay',
             'data/sealed-delete-reject.json'],
            cwd='/workspace', capture_output=True, text=True)
        self.assertEqual(proc.returncode, 1)
        import json as _json
        self.assertEqual(_json.loads(proc.stdout)['rejection']['reason'],
                         'sealed-event')


if __name__ == '__main__':
    unittest.main()
