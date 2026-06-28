# 课前智能答辩管线说明

## 1. 设计目标

「课前智能答辩」用于核验学生对**自己已提交作品**的真实理解：系统基于上次实验提交内容生成个性化闭卷题，学生在下次课前独立作答，AI 初评 + 教师终评，答辩表现作为「作品—交互—答辩—复核」证据链的关键一环。

## 2. 题目结构契约

每次答辩固定 3 题，字段 `{id, type, question, hint, score}`，三题 `score` 之和 = 100：

- 理解验证（40 分）：解释自己代码/思路的关键决策
- 迁移应用（35 分）：在变体场景下迁移所学
- AI 协同思考（25 分）：说明 AI 在本次任务中的作用与边界

`llm_service.generate_defense_questions` 的 prompt 已固定该契约；LLM 不可用时返回带 `_is_mock` 标志的占位题，保证流程不中断。

## 3. 管线现状（诚实口径）

| 环节 | 实现状态 |
|---|---|
| 单条生成 `/api/defense/generate/<sub_id>` | ✅ 已实现 |
| 学生作答 `/student/submission/<sub_id>/defense` + 提交 | ✅ 已实现（支持异步评分） |
| AI 初评 worker（`evaluate_defense`） | ✅ 已接入异步队列 |
| 教师复核改分 `/api/defense/teacher_review/<sub_id>` | ✅ 已实现 |
| **批量补生成** `/api/teacher/defense/batch_generate` | ✅ 本次新增 |
| **自动触发**（提交完成即生成，`Config.DEFENSE_AUTO_GENERATE`） | ✅ 本次新增，默认关闭 |
| **学生端「待答辩」入口**（dashboard 卡片） | ✅ 本次新增 |
| **答辩分计入综合**（`composite_score_with_defense`，非破坏性） | ✅ 本次新增，仅展示不写库 |
| 课前答辩规则（接入 PreClassQuiz） | ⏳ 设计完成，待接入 |

**当前数据状态**：`defense_status='generated'` 共 2 条（首轮试点样例）。全面铺开需在配置好昇腾/云端 LLM 端点后，由教师端「批量补生成」触发，建议先小批量验证题目质量再放量。

## 4. 综合评价口径

`composite_score_with_defense(sub)` 给出非破坏性综合分：

```
综合 = (1 - w) × 作品总分 + w × 答辩分     （w = Config.DEFENSE_COMPOSITE_WEIGHT，默认 0.3）
```

并对 `作品分 − 答辩分 > 25` 的样本给出「作品-答辩落差」预警（疑似代劳信号，回流到 AICI 第 ④ 类）。该函数**不改写** `submissions.total_score`，仅在教师复核界面展示，避免影响既有成绩。

## 5. 上线步骤建议

1. 教师端配置 LLM 端点（本地昇腾 / 云端），用单条生成验证题目质量。
2. 对目标实验调用批量补生成（`max_count` 默认 200，分批跑）。
3. 课前开放学生「待答辩」入口作答；异步队列自动初评。
4. 教师复核改分；在复核界面查看综合分与落差预警。
5. 验证稳定后，可开启 `DEFENSE_AUTO_GENERATE=true` 让新提交自动生成。

> 历史答辩迁移（向 `grade_archive.defense_score` 回填）需源库 `ai_lab.db` 提供历史答辩记录，请确认源库结构后再执行，本版未自动迁移以免写入不可靠数据。
