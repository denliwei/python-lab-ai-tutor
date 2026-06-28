"""
实时请求公平调度队列 —— 削峰填谷第三层
对课堂中不可避免的实时LLM请求（小林辅导、AI编程、考试助手）进行
公平调度、优先级排序、流式输出支持。
"""
import threading
import time
import logging
from collections import defaultdict
from queue import PriorityQueue
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


# 优先级常量
PRIORITY_EXAM = 0       # P0: 考试AI助手（限时，最高优先）
PRIORITY_SOCRATIC = 1   # P1: 苏格拉底辅导（教学核心交互）
PRIORITY_COLLAB = 2     # P2: 编程实践辅助（可容忍稍长延迟）


@dataclass(order=True)
class QueuedRequest:
    """队列中的请求"""
    priority: int
    timestamp: float = field(compare=True)
    student_id: int = field(compare=False)
    request_data: Any = field(compare=False)
    event: Any = field(compare=False, default=None)
    result: Any = field(compare=False, default=None)


class RealtimeQueue:
    """公平实时请求队列"""

    def __init__(self, max_concurrent=5):
        # 最大并发LLM请求数
        self.max_concurrent = max_concurrent
        # 当前正在处理的请求数
        self._active_count = 0
        self._lock = threading.Lock()
        # 每个学生当前是否有请求在处理
        self._student_active = defaultdict(int)
        # 每个学生的最大并发请求数
        self.max_per_student = 1
        # 混合云溢出开关（由 app.py 根据 LLM 模式设置）
        self.hybrid_overflow = False
        # 等待队列
        self._queue = PriorityQueue()
        # 队列统计
        self._stats = {
            'total_queued': 0,
            'total_processed': 0,
            'total_rejected': 0,
            'total_cloud_overflow': 0,
        }

    def get_queue_position(self, student_id):
        """获取学生当前在队列中的位置（近似值）"""
        with self._lock:
            return self._queue.qsize()

    def get_stats(self):
        """获取队列统计"""
        with self._lock:
            return {
                'active': self._active_count,
                'waiting': self._queue.qsize(),
                'max_concurrent': self.max_concurrent,
                'hybrid_overflow': self.hybrid_overflow,
                **self._stats,
            }

    def try_execute(self, student_id, priority, execute_fn, *args, **kwargs):
        """
        尝试立即执行。
        - 同一学生重复请求 → 返回None（调用方显示"请稍候"）
        - 本地槽位满 + 混合模式 → 通过线程信号溢出到云端执行
        - 本地槽位满 + 非混合模式 → 返回None
        """
        with self._lock:
            # 检查该学生是否已有请求在处理
            if self._student_active[student_id] >= self.max_per_student:
                self._stats['total_rejected'] += 1
                logger.info(f"[实时队列] 学生{student_id}请求被合并（已有请求处理中）")
                return None  # 调用方应返回"请稍候"提示

            # 如果有空闲槽位，直接执行（走本地）
            if self._active_count < self.max_concurrent:
                self._active_count += 1
                self._student_active[student_id] += 1
                use_cloud = False
            elif self.hybrid_overflow:
                # ★ 混合云模式：本地满了，溢出到云端
                self._student_active[student_id] += 1
                use_cloud = True
                self._stats['total_cloud_overflow'] += 1
                logger.info(f"[实时队列] 本地满({self._active_count}/{self.max_concurrent})，"
                            f"学生{student_id}溢出到云端")
            else:
                # 非混合模式：拒绝
                self._stats['total_rejected'] += 1
                return None

        try:
            if use_cloud:
                # 通过线程局部变量通知 llm_service 走云端
                from llm_service import _route_signal
                _route_signal.use_cloud = True
                try:
                    self._stats['total_processed'] += 1
                    result = execute_fn(*args, **kwargs)
                finally:
                    _route_signal.use_cloud = False
            else:
                self._stats['total_processed'] += 1
                result = execute_fn(*args, **kwargs)
            return result
        finally:
            with self._lock:
                if not use_cloud:
                    self._active_count -= 1
                self._student_active[student_id] -= 1

    def get_priority_for_phase(self, phase_or_type):
        """根据请求类型映射优先级"""
        priority_map = {
            'exam_phase2': PRIORITY_EXAM,
            'exam_phase3': PRIORITY_EXAM,
            'quiz': PRIORITY_SOCRATIC,
            'ai_collab': PRIORITY_SOCRATIC,
            'thinking': PRIORITY_SOCRATIC,
            'ethics': PRIORITY_SOCRATIC,
            'collab_assist': PRIORITY_COLLAB,
            'hw_assist': PRIORITY_COLLAB,
        }
        return priority_map.get(phase_or_type, PRIORITY_COLLAB)


# 模块级单例
realtime_queue = RealtimeQueue(max_concurrent=10)
