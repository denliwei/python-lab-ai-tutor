# -*- coding: utf-8 -*-
"""
离线批量回算 AICI：对 instance/python_lab.db 中所有已完成提交计算 AI 协同
指数，写入 aigc_detections 表。与线上 /api/aici/recompute 口径完全一致
（复用 aici.py 的纯函数）。可重复运行（先清空旧记录再写入）。

用法： python backfill_aici.py
"""
import sqlite3
import json
from datetime import datetime
import aici

DB = 'instance/python_lab.db'


def parse_dt(s):
    if not s:
        return None
    for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def main():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    subs = c.execute(
        "SELECT * FROM submissions WHERE status='completed'").fetchall()

    # 预取每个实验的代码风格基线
    by_exp = {}
    for s in subs:
        by_exp.setdefault(s['experiment_id'], []).append(s)
    baselines = {
        eid: aici.compute_baseline(
            [aici.code_style_features(s['ai_collab_final_code']) for s in rows])
        for eid, rows in by_exp.items()
    }

    # 清空旧的回算记录，保持可重复运行
    c.execute("DELETE FROM aigc_detections")

    n = 0
    level_dist = {}
    for s in subs:
        # 学生侧对话消息
        msgs = [r['content'] for r in c.execute(
            "SELECT content FROM tutor_chats WHERE submission_id=? AND role='student'",
            (s['id'],)).fetchall()]
        created = parse_dt(s['created_at'])
        submitted = parse_dt(s['submitted_at'])
        duration_min = ((submitted - created).total_seconds() / 60
                        if created and submitted else None)

        b = aici.behavior_score(duration_min, len(msgs), s['total_score'])
        sem = aici.semantic_score(msgs)
        cs = aici.code_style_zscore(
            aici.code_style_features(s['ai_collab_final_code']),
            baselines[s['experiment_id']])
        dg = aici.defense_gap_score(s['total_score'], s['defense_score'])
        result = aici.aggregate(b, sem, cs, dg)
        level_dist[result['level']] = level_dist.get(result['level'], 0) + 1

        c.execute(
            "INSERT INTO aigc_detections "
            "(student_id, submission_id, content_type, content_snippet, ai_rate, detection_detail, detected_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (s['student_id'], s['id'], 'code',
             (s['ai_collab_final_code'] or '')[:200],
             round(result['aici'], 1),
             json.dumps(result, ensure_ascii=False),
             s['submitted_at'] or s['created_at']))
        n += 1

    conn.commit()
    conn.close()
    print(f"回算完成：{n} 条")
    print("等级分布：", level_dist)


if __name__ == '__main__':
    main()
