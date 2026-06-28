"""
异步评分队列服务 —— 削峰填谷第二层
学生提交后评分任务进入队列，后台Worker按可控速率处理。
前端立即返回"已提交"，评分完成后可通过轮询获取结果。
"""
import json
import logging
import threading
import time
from datetime import datetime
from models import db, AsyncTask, Submission, ExamSubmission, HomeworkSubmission, PreClassQuiz

logger = logging.getLogger(__name__)


class AsyncTaskService:
    """异步评分任务管理服务"""

    SCORE_TASK_TYPES = {
        'evaluate_quiz', 'evaluate_ai_collab', 'evaluate_thinking',
        'evaluate_ethics', 'evaluate_pre_class_quiz', 'evaluate_hw_collab',
        'evaluate_defense',
    }

    def __init__(self, llm_service=None):
        self.llm = llm_service
        self._worker_running = False
        self._worker_thread = None
        # 可控速率：每秒最多处理的任务数
        self.rate_limit = 2
        # Worker轮询间隔（秒）
        self.poll_interval = 3

    def set_llm_service(self, llm_service):
        self.llm = llm_service

    # ==================== 入队接口 ====================

    def enqueue(self, task_type, params, submission_id=None, submission_type=None,
                priority=1):
        """
        将评分任务放入异步队列。
        priority: 0=最高(课中急需), 1=普通, 2=低(课后批量)
        返回: AsyncTask 对象
        """
        task = AsyncTask(
            task_type=task_type,
            priority=priority,
            submission_id=submission_id,
            submission_type=submission_type,
            params=json.dumps(params, ensure_ascii=False),
            status='pending',
        )
        db.session.add(task)
        db.session.commit()
        logger.info(f"[异步队列] 入队: {task_type} (id={task.id}, priority={priority}, "
                    f"sub={submission_type}/{submission_id})")
        return task

    # ==================== 查询接口 ====================

    def get_task_status(self, task_id):
        """查询单个任务状态"""
        task = AsyncTask.query.get(task_id)
        if not task:
            return None
        return task.to_dict()

    def get_submission_tasks(self, submission_id, submission_type):
        """查询某次提交的所有异步任务"""
        tasks = AsyncTask.query.filter_by(
            submission_id=submission_id,
            submission_type=submission_type,
        ).order_by(AsyncTask.created_at.asc()).all()
        return [t.to_dict() for t in tasks]

    def is_submission_graded(self, submission_id, submission_type):
        """检查某次提交的所有评分任务是否都已完成"""
        pending = AsyncTask.query.filter_by(
            submission_id=submission_id,
            submission_type=submission_type,
        ).filter(AsyncTask.status.in_(['pending', 'running'])).count()
        return pending == 0

    def get_queue_stats(self):
        """获取队列状态统计"""
        pending = AsyncTask.query.filter_by(status='pending').count()
        running = AsyncTask.query.filter_by(status='running').count()
        completed = AsyncTask.query.filter_by(status='completed').count()
        failed = AsyncTask.query.filter_by(status='failed').count()
        return {'pending': pending, 'running': running, 'completed': completed, 'failed': failed}

    # ==================== Worker执行引擎 ====================

    def _is_unusable_score_result(self, task, result):
        return (
            task.task_type in self.SCORE_TASK_TYPES
            and isinstance(result, dict)
            and result.get('_is_mock')
        )

    def start_worker(self, app):
        """启动后台Worker线程（在Flask app上下文中运行）"""
        if self._worker_running:
            return
        self._worker_running = True
        self._worker_thread = threading.Thread(
            target=self._worker_loop, args=(app,), daemon=True
        )
        self._worker_thread.start()
        logger.info("[异步队列] Worker已启动")

    def stop_worker(self):
        """停止Worker"""
        self._worker_running = False
        if self._worker_thread:
            self._worker_thread.join(timeout=10)
        logger.info("[异步队列] Worker已停止")

    def _worker_loop(self, app):
        """Worker主循环：按优先级取任务并执行"""
        while self._worker_running:
            try:
                with app.app_context():
                    self._process_next_task()
            except Exception as e:
                logger.error(f"[异步队列] Worker异常: {e}")
            time.sleep(self.poll_interval)

    def _process_next_task(self):
        """取出并执行一个最高优先级的待处理任务"""
        task = AsyncTask.query.filter_by(
            status='pending'
        ).order_by(
            AsyncTask.priority.asc(),      # 优先级数字越小越优先
            AsyncTask.created_at.asc()     # 同优先级先进先出
        ).first()

        if not task:
            return  # 队列空

        # 标记为执行中
        task.status = 'running'
        task.started_at = datetime.utcnow()
        db.session.commit()

        try:
            result = self._execute_task(task)
            if self._is_unusable_score_result(task, result):
                raise RuntimeError(result.get('feedback') or result.get('overall_feedback') or '评分服务暂时不可用')
            task.status = 'completed'
            task.result = json.dumps(result, ensure_ascii=False) if result else None
            task.completed_at = datetime.utcnow()

            # 将结果回写到原始提交记录
            self._apply_result(task, result)
            db.session.commit()
            logger.info(f"[异步队列] 完成: {task.task_type} (id={task.id})")

        except Exception as e:
            task.retry_count += 1
            task.error_message = str(e)[:500]
            if task.retry_count < task.max_retries:
                task.status = 'pending'  # 重新入队重试
                logger.warning(f"[异步队列] 重试: {task.task_type} (id={task.id}, "
                               f"retry={task.retry_count}): {e}")
            else:
                task.status = 'failed'
                task.completed_at = datetime.utcnow()
                logger.error(f"[异步队列] 失败: {task.task_type} (id={task.id}): {e}")
            db.session.commit()

    def _execute_task(self, task):
        """根据任务类型调用对应的LLM评分函数"""
        params = task.get_params()
        tt = task.task_type

        if tt == 'evaluate_quiz':
            return self.llm.evaluate_quiz(params['questions'], params['answers'])

        elif tt == 'evaluate_ai_collab':
            return self.llm.evaluate_ai_collab(
                params['task_description'], params['prompt_history'],
                params['final_code'], params['reflection'])

        elif tt == 'evaluate_thinking':
            return self.llm.evaluate_thinking(params['task_description'], params['answer'])

        elif tt == 'evaluate_ethics':
            return self.llm.evaluate_ethics(params['case_description'], params['answer'])

        elif tt == 'generate_evaluation':
            return self.llm.generate_evaluation(params['eval_data'], params['topics'])

        elif tt == 'evaluate_defense':
            return self.llm.evaluate_defense(params['questions'], params['answers'], params.get('context', {}))

        elif tt == 'evaluate_pre_class_quiz':
            return self.llm.evaluate_pre_class_quiz(params['questions'], params['answers'])

        elif tt == 'generate_pre_class_evaluation':
            return self.llm.generate_pre_class_evaluation(
                student_name=params['student_name'],
                prev_experiment_title=params['prev_experiment_title'],
                prev_submission_data=params.get('prev_submission_data', {}),
                quiz_score=params.get('quiz_score', 0),
                quiz_details=params.get('quiz_details', []))

        elif tt == 'evaluate_hw_collab':
            return self.llm.evaluate_ai_collab(
                params['task_description'], params['prompt_history'],
                params['final_code'], params.get('reflection', ''))

        else:
            raise ValueError(f"未知任务类型: {tt}")

    def _apply_result(self, task, result):
        """将评分结果回写到原始提交记录"""
        if not result or not task.submission_id:
            return

        sub_type = task.submission_type
        sub_id = task.submission_id
        tt = task.task_type

        if sub_type == 'submission':
            sub = Submission.query.get(sub_id)
            if not sub:
                return
            if tt == 'evaluate_quiz':
                sub.quiz_score = max(0, min(25, result.get('score', 0)))
                sub.quiz_feedback = json.dumps(result, ensure_ascii=False)
                sub.status = 'ai_collab'
            elif tt == 'evaluate_ai_collab':
                sub.ai_collab_score = max(0, min(30, result.get('score', 0)))
                sub.ai_collab_feedback = json.dumps(result, ensure_ascii=False)
                sub.status = 'thinking'
            elif tt == 'evaluate_thinking':
                sub.thinking_score = max(0, min(25, result.get('score', 0)))
                sub.thinking_feedback = json.dumps(result, ensure_ascii=False)
                sub.status = 'ethics'
            elif tt == 'evaluate_ethics':
                sub.ethics_score = max(0, min(20, result.get('score', 0)))
                sub.ethics_feedback = json.dumps(result, ensure_ascii=False)
                # 计算总分
                sub.calculate_total_score()
                sub.submitted_at = datetime.utcnow()
                # 入队综合评价任务
                eval_data = {
                    'quiz_score': sub.quiz_score or 0,
                    'ai_collab_score': sub.ai_collab_score or 0,
                    'thinking_score': sub.thinking_score or 0,
                    'ethics_score': sub.ethics_score or 0,
                }
                topics = sub.experiment.get_topics() if sub.experiment else []
                self.enqueue('generate_evaluation',
                             {'eval_data': eval_data, 'topics': topics},
                             submission_id=sub_id, submission_type='submission',
                             priority=1)
            elif tt == 'generate_evaluation':
                sub.final_evaluation = json.dumps(result, ensure_ascii=False)
                sub.status = 'completed'
                sub.completed_at = datetime.utcnow()

        elif sub_type == 'submission_defense':
            sub = Submission.query.get(sub_id)
            if sub:
                sub.defense_score = result.get('total_score', 0)
                sub.defense_feedback = json.dumps(result, ensure_ascii=False)
                sub.defense_status = 'submitted'
                sub.defense_submitted_at = datetime.utcnow()

        elif sub_type == 'hw_submission':
            sub = HomeworkSubmission.query.get(sub_id)
            if not sub:
                return
            if tt == 'evaluate_hw_collab':
                sub.hw_score = result.get('score', 0)
                sub.hw_feedback = json.dumps(result, ensure_ascii=False)
                sub.status = 'submitted'
                sub.submitted_at = datetime.utcnow()

        elif sub_type == 'hw_defense':
            sub = HomeworkSubmission.query.get(sub_id)
            if sub:
                sub.defense_score = result.get('total_score', 0)
                sub.defense_feedback = json.dumps(result, ensure_ascii=False)
                sub.defense_status = 'submitted'
                sub.defense_submitted_at = datetime.utcnow()

        elif sub_type == 'pre_class_quiz':
            quiz = PreClassQuiz.query.get(sub_id)
            if not quiz:
                return
            if tt == 'evaluate_pre_class_quiz':
                quiz.score = result.get('total_score', 0)
                quiz.answers_feedback = json.dumps(result, ensure_ascii=False)
                # 入队综合评价
                params = task.get_params()
                self.enqueue('generate_pre_class_evaluation', {
                    'student_name': params.get('student_name', ''),
                    'prev_experiment_title': params.get('prev_experiment_title', ''),
                    'prev_submission_data': params.get('prev_submission_data', {}),
                    'quiz_score': quiz.score,
                    'quiz_details': result.get('details', []),
                }, submission_id=sub_id, submission_type='pre_class_quiz', priority=1)
            elif tt == 'generate_pre_class_evaluation':
                quiz.comprehensive_evaluation = result if isinstance(result, str) else json.dumps(result, ensure_ascii=False)
                quiz.status = 'completed'
                quiz.completed_at = datetime.utcnow()

    # ==================== 手动触发批量执行 ====================

    def process_all_pending(self, max_tasks=100):
        """手动触发：处理所有待处理任务（管理接口使用）"""
        processed = 0
        for _ in range(max_tasks):
            task = AsyncTask.query.filter_by(
                status='pending'
            ).order_by(
                AsyncTask.priority.asc(), AsyncTask.created_at.asc()
            ).first()
            if not task:
                break
            self._process_next_task()
            processed += 1
        return processed


# 模块级单例
task_service = AsyncTaskService()
