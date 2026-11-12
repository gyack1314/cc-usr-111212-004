"""显示原始湖仓资料配置，或驱动事件时间会话聚合重放。"""
import argparse
import json
from pathlib import Path

from .io import load
from .domain import summarize
from .sessions import (
    PersistentSessionEngine,
    SessionEngine,
    RejectedOperation,
    run_operations,
)


def _print(payload):
    print(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False))


def _build_engine(v, store, resume):
    gap = v['gap']
    lateness = v['allowed_lateness']
    initial = v.get('initial_watermark', -100)
    limit = v.get('max_active_events', 10000)
    if store is None:
        return SessionEngine(gap, lateness, initial, limit)
    if resume:
        return PersistentSessionEngine.resume(store)
    return PersistentSessionEngine.create(
        store, gap, lateness, initial, limit)


def _replay(v, digest, store, resume, check):
    engine = _build_engine(v, store, resume)
    # 拒绝即停止；拒绝不消耗输入位置、不发任何变更、不落盘
    records, rejection = run_operations(engine, v['operations'])
    payload = {
        'source_sha256': digest,
        'watermark': engine.watermark,
        'op_position': engine.op_position,
        'change_seq': engine.change_seq,
        'stats': dict(engine.stats),
        'results': records,
        'change_log': list(engine.change_log),
        'rejection': None,
        'rebuild_diff': None,
        'checkpoint': engine.snapshot() if store is not None else None,
    }
    if check or store is not None:
        payload['rebuild_diff'] = engine.verify_rebuild()
    if rejection is not None:
        payload['rejection'] = {
            'op_index': records[-1]['op_index'],
            'reason': rejection.reason,
            'context': rejection.context,
        }
        payload['remaining_operations'] = (
            len(v['operations']) - engine.op_position)
    return payload, rejection is None


def main():
    p = argparse.ArgumentParser()
    p.add_argument('command', choices=['inspect', 'replay'])
    p.add_argument('file')
    p.add_argument('--store', help='会话仓落盘目录：变更日志与检查点同事务持久化')
    p.add_argument('--resume', action='store_true',
                   help='从落盘目录的检查点恢复，跳过已处理输入续跑')
    p.add_argument('--check', action='store_true',
                   help='结束后从已接受事件独立重建并对照在线会话索引')
    a = p.parse_args()

    v, digest = load(a.file)
    if a.command == 'inspect':
        _print({'source_sha256': digest,
                'summary': summarize(v, Path(a.file).parent)})
        return

    try:
        payload, ok = _replay(v, digest, a.store, a.resume, a.check)
    except (ValueError, KeyError, TypeError, FileExistsError, IOError) as exc:
        # 配置/输入形状错误或落盘目录已存在：不产生会话状态
        _print({'source_sha256': digest, 'error': type(exc).__name__,
                'reason': str(exc)})
        raise SystemExit(1)
    _print(payload)
    if not ok:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
