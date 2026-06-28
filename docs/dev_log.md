# 开发记录与版本演进

## 总体路径
教学痛点转译 → 评价量规设计 → 数据模型搭建 → AI 服务封装 → 学生端/教师端开发 → 试运行迭代。

## 关键里程碑
- **数据建模**：users/experiments/submissions/pre_class_quizzes/async_tasks/tutor_chats/grade_archive/question_pool/aigc_detections 等表；四阶段分数与综合分计算。
- **削峰填谷三层**：题库池预生成（课前批量调用 LLM）+ 异步评分队列（可控速率、失败重试、格式校验、云端回退）+ 实时辅导队列（并发与优先级控制）。
- **算力路由**：`llm_service._resolve_endpoint()` 实现 local/cloud/hybrid 三态，hybrid 下本地拥挤按每日预算溢出到云端。
- **安全沙箱**：`code_runner.py` 子进程隔离 + 超时 10s + 内存 50MB + 输出上限 + 危险模块/函数黑名单。
- **早期预警**：8 类规则 A–H，教师已实际处理 104 条预警记录。
- **AI 协同画像（AICI）**：以多信号融合替代关键词二元检测，对 597 条提交完成回算（见 `docs/aici_algorithm.md`）。
- **课前智能答辩**：单条生成/作答/异步初评/教师复核已通；批量补生成、自动触发、学生入口、综合评分本轮补齐（见 `docs/defense_pipeline.md`）。

## 生成式 AI 参与开发的留痕
见 `evidence/dev_prompts/`：需求拆解、评分量规草拟、苏格拉底式提示词调参、异常 JSON 修复、文案优化等 AI 协同开发对话片段。关键代码、评分规则与教学流程均由教师团队审查、测试后部署。

## 数据合规
随附数据库已脱敏：学生姓名掩码（姓+星号）、学号掩码（****+后四位）、`cloud_api_key` 清空、本地端点改为内网占位地址。
