"""
题库池服务 —— 削峰填谷第一层：预生成
教师创建实验/考试/作业后，系统在后台批量调用LLM预生成题目到题库池。
学生进入页面时从池中分配，无需实时等待LLM。
"""
import json
import logging
from datetime import datetime
from models import db, QuestionPool, Experiment, Homework, User

logger = logging.getLogger(__name__)


class QuestionPoolService:
    """题库池管理服务"""

    def __init__(self, llm_service=None):
        self.llm = llm_service

    def set_llm_service(self, llm_service):
        self.llm = llm_service

    # ==================== 从池中分配题目 ====================

    def allocate(self, source_type, source_id, task_type, student_id,
                 difficulty='medium', major='general'):
        """
        从题库池中领取一份未分配的题目。
        如果池中无可用题目，回退到实时生成（保证功能不中断）。
        返回: dict (题目内容)
        """
        # 原子操作：查找并锁定一条未分配的记录
        item = QuestionPool.query.filter_by(
            source_type=source_type,
            source_id=source_id,
            task_type=task_type,
            is_allocated=False,
        ).first()

        if item:
            item.is_allocated = True
            item.allocated_to = student_id
            item.allocated_at = datetime.utcnow()
            db.session.commit()
            logger.info(f"[题库池] 分配成功: {source_type}/{source_id}/{task_type} -> student {student_id}")
            return item.get_content()

        # 池中无可用题目 → 回退到实时生成
        logger.warning(f"[题库池] 池空，回退实时生成: {source_type}/{source_id}/{task_type}")
        return self._fallback_generate(source_type, source_id, task_type, difficulty, major)

    def get_pool_stats(self, source_type, source_id):
        """获取某个实验/考试/作业的题库池状态统计"""
        total = QuestionPool.query.filter_by(
            source_type=source_type, source_id=source_id
        ).count()
        allocated = QuestionPool.query.filter_by(
            source_type=source_type, source_id=source_id, is_allocated=True
        ).count()
        available = total - allocated
        return {'total': total, 'allocated': allocated, 'available': available}

    def get_pool_stats_by_type(self, source_type, source_id, task_type):
        """获取某种题型的池状态"""
        total = QuestionPool.query.filter_by(
            source_type=source_type, source_id=source_id, task_type=task_type
        ).count()
        allocated = QuestionPool.query.filter_by(
            source_type=source_type, source_id=source_id, task_type=task_type, is_allocated=True
        ).count()
        return {'total': total, 'allocated': allocated, 'available': total - allocated}

    # ==================== 批量预生成 ====================

    def prefill_experiment(self, experiment_id, count=60):
        """
        为一个实验批量预生成4种题目，每种生成count份。
        适合在夜间低峰由定时任务调用。
        """
        experiment = Experiment.query.get(experiment_id)
        if not experiment:
            logger.error(f"[题库池] 实验 {experiment_id} 不存在")
            return {'error': 'experiment not found'}

        topics = experiment.get_topics()
        difficulty = experiment.difficulty
        major = experiment.major_category or 'general'

        task_types = [
            ('knowledge_quiz', lambda: self.llm.generate_knowledge_quiz(topics, difficulty)),
            ('ai_collab_task', lambda: self.llm.generate_ai_collab_task(topics, difficulty, major)),
            ('thinking_task', lambda: self.llm.generate_thinking_task(topics, difficulty)),
            ('ethics_case', lambda: self.llm.generate_ethics_case(topics, difficulty)),
        ]

        stats = {}
        for task_type, generator in task_types:
            existing = QuestionPool.query.filter_by(
                source_type='experiment', source_id=experiment_id,
                task_type=task_type, is_allocated=False
            ).count()
            need = max(0, count - existing)
            generated = 0

            for _ in range(need):
                try:
                    content = generator()
                    if content:
                        pool_item = QuestionPool(
                            source_type='experiment',
                            source_id=experiment_id,
                            task_type=task_type,
                            difficulty=difficulty,
                            major_category=major,
                            content=json.dumps(content, ensure_ascii=False),
                        )
                        db.session.add(pool_item)
                        db.session.commit()
                        generated += 1
                except Exception as e:
                    logger.error(f"[题库池] 预生成失败 {task_type}: {e}")
                    db.session.rollback()
                    break  # 遇到LLM错误停止当前类型

            stats[task_type] = {'existing': existing, 'generated': generated, 'total': existing + generated}
            logger.info(f"[题库池] experiment/{experiment_id}/{task_type}: "
                        f"已有{existing}, 新增{generated}, 共{existing + generated}")

        return stats

    def prefill_homework(self, homework_id, count=60):
        """为作业预生成ai_collab_task题目"""
        homework = Homework.query.get(homework_id)
        if not homework:
            return {'error': 'homework not found'}

        topics = homework.get_topics()
        difficulty = homework.difficulty
        major = homework.major_category or 'general'

        existing = QuestionPool.query.filter_by(
            source_type='homework', source_id=homework_id,
            task_type='ai_collab_task', is_allocated=False
        ).count()
        need = max(0, count - existing)
        generated = 0

        for _ in range(need):
            try:
                content = self.llm.generate_ai_collab_task(topics, difficulty, major)
                if content:
                    pool_item = QuestionPool(
                        source_type='homework',
                        source_id=homework_id,
                        task_type='ai_collab_task',
                        difficulty=difficulty,
                        major_category=major,
                        content=json.dumps(content, ensure_ascii=False),
                    )
                    db.session.add(pool_item)
                    db.session.commit()
                    generated += 1
            except Exception as e:
                logger.error(f"[题库池] 作业预生成失败: {e}")
                db.session.rollback()
                break

        return {'ai_collab_task': {'existing': existing, 'generated': generated, 'total': existing + generated}}

    def prefill_all_active(self, pool_size=60):
        """为所有已激活的实验和作业预生成题目（定时任务入口）"""
        results = {}

        # 实验
        experiments = Experiment.query.filter_by(is_active=True).all()
        for exp in experiments:
            stats = self.prefill_experiment(exp.id, pool_size)
            results[f'experiment_{exp.id}'] = stats

        # 作业
        homeworks = Homework.query.filter_by(is_active=True).all()
        for hw in homeworks:
            stats = self.prefill_homework(hw.id, pool_size)
            results[f'homework_{hw.id}'] = stats

        logger.info(f"[题库池] 全量预生成完成: {len(results)} 个来源")
        return results

    # ==================== 池管理 ====================

    def check_and_refill(self, source_type, source_id, task_type,
                         threshold=0.1, refill_count=10):
        """
        检查池余量，低于阈值时触发紧急补充。
        threshold: 可用比例低于此值时补充（默认10%）
        """
        stats = self.get_pool_stats_by_type(source_type, source_id, task_type)
        if stats['total'] == 0:
            return  # 未初始化的池不自动补充

        available_ratio = stats['available'] / stats['total'] if stats['total'] > 0 else 0
        if available_ratio < threshold:
            logger.warning(f"[题库池] 余量不足({stats['available']}/{stats['total']}), 触发紧急补充")
            if source_type == 'experiment':
                self.prefill_experiment(source_id, stats['total'] + refill_count)
            elif source_type == 'homework':
                self.prefill_homework(source_id, stats['total'] + refill_count)

    def clear_pool(self, source_type, source_id):
        """清空某个来源的题库池（教师重置时使用）"""
        count = QuestionPool.query.filter_by(
            source_type=source_type, source_id=source_id
        ).delete()
        db.session.commit()
        logger.info(f"[题库池] 清空 {source_type}/{source_id}: 删除{count}条")
        return count

    # ==================== 回退生成 ====================

    def _fallback_generate(self, source_type, source_id, task_type, difficulty, major):
        """池空时的实时生成回退方案"""
        if not self.llm:
            return {}

        try:
            if source_type == 'experiment':
                exp = Experiment.query.get(source_id)
                topics = exp.get_topics() if exp else []
            elif source_type == 'homework':
                hw = Homework.query.get(source_id)
                topics = hw.get_topics() if hw else []
            else:
                topics = []

            generators = {
                'knowledge_quiz': lambda: self.llm.generate_knowledge_quiz(topics, difficulty),
                'ai_collab_task': lambda: self.llm.generate_ai_collab_task(topics, difficulty, major),
                'thinking_task': lambda: self.llm.generate_thinking_task(topics, difficulty),
                'ethics_case': lambda: self.llm.generate_ethics_case(topics, difficulty),
            }

            gen = generators.get(task_type)
            if gen:
                return gen()
        except Exception as e:
            logger.error(f"[题库池] 回退生成失败: {e}")

        return {}


# 模块级单例
pool_service = QuestionPoolService()
