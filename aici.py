# -*- coding: utf-8 -*-
"""
AICI —— AI 协同指数（AI Collaboration Index）多信号融合画像

设计目标：放弃在 Python 教学场景下既不准确、也无教育意义的二元
「AI 写没写」判断，改为多信号融合，输出 0–100 的 AI 协同指数与四级
标签（自主 / 协同 / 依赖 / 疑似代劳），用于帮助教师定位需要个别化
辅导的学生，而非用于惩戒。

五类信号（第 ⑤ 类元信息需新埋点，默认不计入）：
  ① 过程行为  完成时长、零对话高分、极速完成        权重 25%
  ② 对话语义  索取答案次数、消息平均长度            权重 20%
  ③ 代码风格  与同实验班级基线比对的「过度规范」偏离  权重 25%
  ④ 答辩验证  作品高分但答辩低分的落差              权重 20%
  ⑤ 元信息    粘贴/击键节奏（埋点上线后启用）        权重 10%

本模块的评分函数为纯函数（不依赖 Flask / SQLAlchemy），既供线上
Flask 路由调用，也供离线批量回算脚本 backfill_aici.py 复用，保证
线上与离线口径完全一致。
"""

import re
import json
import statistics

# 与 app.py 中保持一致的「索取答案」关键词表
ANSWER_SEEK_KEYWORDS = [
    '答案是什么', '告诉我答案', '直接给我答案', '给我答案', '答案给我',
    '正确答案', '帮我写答案', '答案呢', '怎么填', '填什么',
    '选哪个', '选什么', '怎么选', '选A还是', '选B还是', '选C还是',
    '直接告诉我', '直接说答案', '不用引导', '别绕弯子',
    '就说答案', '答案直接给', '标准答案', '答案',
]

# 信号权重（⑤ 暂缺省，归一化到已有四类）
WEIGHTS = {
    'behavior': 0.25,
    'semantic': 0.20,
    'code_style': 0.25,
    'defense_gap': 0.20,
}

_STYLE_KEYS = (
    'comment_density', 'has_main_guard', 'has_type_hints',
    'avg_line_len', 'english_comment_ratio',
)


# ──────────────────── ① 过程行为 ────────────────────
def behavior_score(duration_min, chat_n, total_score):
    """完成时长 / 零对话高分 / 极速高分 三个子信号叠加，0-100。"""
    if duration_min is None:
        return 0.0
    score = 0
    ts = total_score or 0
    if duration_min <= 8 and ts >= 60:
        score += 40
    if duration_min <= 15 and chat_n == 0:
        score += 30
    if duration_min <= 20 and ts >= 85:
        score += 20
    return float(min(100, score))


# ──────────────────── ② 对话语义 ────────────────────
def semantic_score(student_messages):
    """student_messages: list[str]，学生侧消息文本。"""
    if not student_messages:
        # 完全无对话本身不算语义风险（行为信号已覆盖零对话），返回 0
        return 0.0
    seek = sum(1 for m in student_messages
               if any(k in m for k in ANSWER_SEEK_KEYWORDS))
    avg_len = sum(len(m) for m in student_messages) / len(student_messages)
    return float(min(100, seek * 15 + (20 if avg_len < 8 else 0)))


# ──────────────────── ③ 代码风格 ────────────────────
def _english_ratio(text):
    if not text or not text.strip():
        return 0.0
    english = sum(1 for ch in text if ch.isascii() and ch.isalpha())
    chinese = sum(1 for ch in text if '一' <= ch <= '鿿')
    return english / max(english + chinese, 1)


def code_style_features(code):
    code = code or ''
    lines = [l for l in code.split('\n') if l.strip()]
    if not lines:
        return {}
    comments = [l for l in lines if l.strip().startswith('#')]
    return {
        'comment_density': len(comments) / len(lines),
        'has_main_guard': int('__name__' in code and '__main__' in code),
        'has_type_hints': int(bool(re.search(r'def \w+\([^)]*:\s*\w+', code))),
        'has_docstring': int('"""' in code or "'''" in code),
        'avg_line_len': sum(len(l) for l in lines) / len(lines),
        'english_comment_ratio': _english_ratio(
            ' '.join(c.lstrip('#') for c in comments)),
    }


def compute_baseline(feature_list):
    """对一组 code_style_features 输出每个特征的 (mean, std) 基线。"""
    feats = [f for f in feature_list if f]
    baseline = {}
    for k in _STYLE_KEYS:
        vals = [f[k] for f in feats if k in f]
        baseline[k] = (
            statistics.mean(vals) if vals else 0.0,
            statistics.stdev(vals) if len(vals) > 1 else 1.0,
        )
    return baseline


def code_style_zscore(feats, baseline):
    """与同实验班级基线比对，仅关注「过度规范」方向，输出 0-100。"""
    if not feats or not baseline:
        return 0.0
    deviations = []
    for k in _STYLE_KEYS:
        mean, std = baseline.get(k, (0.0, 1.0))
        if std and std > 0:
            z = (feats.get(k, 0) - mean) / std
            if z > 0:  # 只关注比班级更「整齐规范」的方向
                deviations.append(min(z, 4))
    if not deviations:
        return 0.0
    return float(min(100, sum(deviations) / len(deviations) * 25))


# ──────────────────── ④ 答辩验证 ────────────────────
def defense_gap_score(total_score, defense_score):
    """作品总分 vs 答辩百分制得分的正向落差，0-100。"""
    if defense_score is None or not total_score:
        return 0.0
    gap = total_score - defense_score
    return float(max(0, min(100, gap * 2)))


# ──────────────────── 融合 ────────────────────
def _level(aici):
    return ('自主' if aici < 30 else
            '协同' if aici < 60 else
            '依赖' if aici < 80 else '疑似代劳')


def aggregate(behavior, semantic, code_style, defense_gap):
    # ⑤ 元信息信号暂未埋点，将其余四类权重归一化到 1.0，使指数用满 0-100 量程
    active_sum = (WEIGHTS['behavior'] + WEIGHTS['semantic']
                  + WEIGHTS['code_style'] + WEIGHTS['defense_gap'])
    aici = (behavior * WEIGHTS['behavior']
            + semantic * WEIGHTS['semantic']
            + code_style * WEIGHTS['code_style']
            + defense_gap * WEIGHTS['defense_gap']) / active_sum
    return {
        'aici': round(aici, 1),
        'level': _level(aici),
        'contributors': {
            'behavior': round(behavior, 1),
            'semantic': round(semantic, 1),
            'code_style': round(code_style, 1),
            'defense_gap': round(defense_gap, 1),
        },
        'weights': dict(WEIGHTS),
    }


# ──────────────────── Flask / SQLAlchemy 适配层 ────────────────────
def baseline_for_experiment(experiment_id, Submission):
    """对某实验所有已完成提交计算代码风格基线（供线上调用）。"""
    subs = Submission.query.filter_by(
        experiment_id=experiment_id, status='completed').all()
    return compute_baseline(
        [code_style_features(s.ai_collab_final_code) for s in subs])


def compute_aici_for_submission(sub, TutorChat, baseline):
    """对单条 Submission 对象计算 AICI（供线上 Flask 路由调用）。"""
    duration_min = None
    if sub.submitted_at and sub.created_at:
        duration_min = (sub.submitted_at - sub.created_at).total_seconds() / 60
    student_msgs = [c.content for c in TutorChat.query.filter_by(
        submission_id=sub.id, role='student').all()]
    b = behavior_score(duration_min, len(student_msgs), sub.total_score)
    s = semantic_score(student_msgs)
    c = code_style_zscore(code_style_features(sub.ai_collab_final_code), baseline)
    d = defense_gap_score(sub.total_score, sub.defense_score)
    return aggregate(b, s, c, d)
