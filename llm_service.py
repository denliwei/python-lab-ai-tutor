"""
LLM服务模块 - 《Python程序设计》课程实验系统
"小林同学"智能体后端服务
支持三种算力模式：纯本地 / 纯云端API / 自动混合云
"""
import requests
import json
import re
import time
import threading
import logging
from config import Config, get_timeout_for_task
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from datetime import date

logger = logging.getLogger(__name__)

# 线程局部变量：混合云模式下，realtime_queue溢出时设置此标志，指示当前请求走云端
_route_signal = threading.local()


class TaskType(Enum):
    GENERATE_KNOWLEDGE_QUIZ = "generate_knowledge_quiz"
    GENERATE_AI_COLLAB_TASK = "generate_ai_collab_task"
    GENERATE_THINKING_TASK = "generate_thinking_task"
    GENERATE_ETHICS_CASE = "generate_ethics_case"
    EVALUATE_QUIZ = "evaluate_quiz"
    EVALUATE_AI_COLLAB = "evaluate_ai_collab"
    EVALUATE_THINKING = "evaluate_thinking"
    EVALUATE_ETHICS = "evaluate_ethics"
    GENERATE_EVALUATION = "generate_evaluation"
    GENERATE_PROFILE = "generate_profile"
    SOCRATIC_TUTOR = "socratic_tutor"
    GENERATE_WARNING = "generate_warning"
    AI_COLLAB_ASSIST = "ai_collab_assist"  # 编程实践中的辅助
    GENERATE_DEFENSE_QUESTIONS = "generate_defense_questions"  # 生成答辩题
    EVALUATE_DEFENSE = "evaluate_defense"  # 评估答辩


@dataclass
class LLMResponse:
    data: Optional[Dict[str, Any]]
    is_mock: bool
    error_message: Optional[str]
    raw_response: Optional[str]

    def __bool__(self):
        return self.data is not None


# ==================== 中文主题映射 ====================
TOPICS_CN = {
    'python_overview': 'Python语言概述与开发环境',
    'python_io': 'Python基本输入输出',
    'variables_basics': '变量与代码规范',
    'number_types': '数字类型与类型转换',
    'string_ops': '字符串操作与格式化输出',
    'operators': '运算符与表达式',
    'list_type': '列表的定义与常用操作',
    'tuple_type': '元组的定义与常用操作',
    'dict_type': '字典的定义与常用操作',
    'set_type': '集合的定义与常用操作',
    'if_statement': 'if条件语句与嵌套',
    'for_loop': 'for循环语句',
    'while_loop': 'while循环语句',
    'loop_control': 'break与continue控制语句',
    'exception_handling': '异常处理与try语句',
    'assert_raise': 'assert与raise语句',
    'function_def': '函数的定义与调用',
    'function_params': '函数参数传递方式',
    'variable_scope': '局部变量与全局变量',
    'lambda_recursive': '匿名函数与递归函数',
    'builtin_functions': '常用内置函数',
    'module_import': '模块概念与导入方式',
    'standard_modules': '标准模块应用',
    'custom_modules': '自定义模块与包',
    'third_party': '第三方模块安装与使用',
    'gui_basics': 'GUI编程基础与tkinter',
    'tkinter_widgets': 'tkinter基本组件',
    'layout_managers': '几何布局管理器',
    'event_handling': '事件处理与对话框',
    'file_open_close': '文件打开与关闭',
    'file_read': '文件读取方法',
    'file_write': '文件写入方法',
    'file_operations': '文件与文件夹操作',
    'file_path': '文件路径操作',
    'oop_concepts': '面向对象基本概念',
    'class_def': '类的定义与对象创建',
    'constructor_destructor': '构造方法与析构方法',
    'class_static_methods': '类方法与静态方法',
    'inheritance_polymorphism': '继承与多态',
}


class LLMService:
    """LLM服务类 — "小林同学"后端"""

    TIMEOUT_CONFIG = {
        TaskType.GENERATE_KNOWLEDGE_QUIZ: 'long',
        TaskType.GENERATE_AI_COLLAB_TASK: 'long',
        TaskType.GENERATE_THINKING_TASK: 'long',
        TaskType.GENERATE_ETHICS_CASE: 'long',
        TaskType.EVALUATE_QUIZ: 'short',
        TaskType.EVALUATE_AI_COLLAB: 'short',
        TaskType.EVALUATE_THINKING: 'short',
        TaskType.EVALUATE_ETHICS: 'short',
        TaskType.GENERATE_EVALUATION: 'long',
        TaskType.GENERATE_PROFILE: 'long',
        TaskType.SOCRATIC_TUTOR: 'short',
        TaskType.GENERATE_WARNING: 'long',
        TaskType.AI_COLLAB_ASSIST: 'long',
        TaskType.GENERATE_DEFENSE_QUESTIONS: 'long',
        TaskType.EVALUATE_DEFENSE: 'long',
    }

    def __init__(self):
        # 本地端点（从config.py读取）
        self.local_url = Config.LLM_API_URL
        self.local_key = Config.LLM_API_KEY
        self.local_model = Config.LLM_MODEL
        # 云端端点（运行时由教师通过页面配置）
        self.cloud_url = None
        self.cloud_key = None
        self.cloud_model = None
        # 运行模式：local / cloud / hybrid
        self.mode = 'local'
        # 混合云每日云端调用计数
        self._cloud_calls_today = 0
        self._cloud_calls_date = ''
        self._cloud_daily_limit = 500
        self._cloud_lock = threading.Lock()
        # 云端调用累计统计（不按天重置）
        self._cloud_total_calls = 0
        # 兼容旧属性（供存量代码中直接引用 self.api_url 的地方）
        self.api_url = self.local_url
        self.api_key = self.local_key
        self.model = self.local_model
        self.topics_cn = TOPICS_CN

    # ==================== 算力路由核心 ====================

    def _resolve_endpoint(self):
        """
        根据当前模式和线程信号，决定本次请求使用哪个端点。
        返回: (api_url, api_key, model, is_cloud)
        """
        # 线程级云端强制信号（由 realtime_queue 溢出时设置）
        force_cloud = getattr(_route_signal, 'use_cloud', False)

        if self.mode == 'cloud':
            if self.cloud_url and self.cloud_key:
                self._increment_cloud_count()
                return (self.cloud_url, self.cloud_key, self.cloud_model or self.local_model, True)
            # 云端未配置，回退本地
            return (self.local_url, self.local_key, self.local_model, False)

        elif self.mode == 'hybrid' and (force_cloud or False):
            # 混合模式 + 被溢出信号触发 → 尝试走云端
            if self.cloud_url and self.cloud_key and self._check_cloud_budget():
                return (self.cloud_url, self.cloud_key, self.cloud_model or self.local_model, True)
            # 云端不可用或预算用尽 → 回退本地
            return (self.local_url, self.local_key, self.local_model, False)

        # 默认走本地（mode=='local' 或 hybrid 但未触发溢出）
        return (self.local_url, self.local_key, self.local_model, False)

    def _check_cloud_budget(self):
        """检查今日云端调用是否在预算内，是则扣减一次"""
        today = date.today().isoformat()
        with self._cloud_lock:
            if self._cloud_calls_date != today:
                self._cloud_calls_date = today
                self._cloud_calls_today = 0
            if self._cloud_calls_today >= self._cloud_daily_limit:
                logger.warning(f"[算力路由] 云端日限额已用尽 ({self._cloud_calls_today}/{self._cloud_daily_limit})")
                return False
            self._cloud_calls_today += 1
            self._cloud_total_calls += 1
            return True

    def _increment_cloud_count(self):
        """纯云端模式下仅计数（不做预算限制）"""
        today = date.today().isoformat()
        with self._cloud_lock:
            if self._cloud_calls_date != today:
                self._cloud_calls_date = today
                self._cloud_calls_today = 0
            self._cloud_calls_today += 1
            self._cloud_total_calls += 1

    def load_provider_config(self, cfg_row):
        """从数据库 LLMProviderConfig 行加载配置到运行时"""
        if not cfg_row:
            return
        self.mode = cfg_row.mode or 'local'
        self.cloud_url = cfg_row.cloud_api_url
        self.cloud_key = cfg_row.cloud_api_key
        self.cloud_model = cfg_row.cloud_model
        self._cloud_daily_limit = cfg_row.cloud_daily_limit or 500
        # 恢复今日计数
        today = date.today().isoformat()
        if cfg_row.cloud_calls_date == today:
            self._cloud_calls_today = cfg_row.cloud_calls_today or 0
        else:
            self._cloud_calls_today = 0
        self._cloud_calls_date = today
        # ★ 本地配置始终以 config.py 为准，不从数据库覆盖
        # （数据库中的 local_url/local_model 仅用于页面展示，不回写运行时）
        logger.info(f"[算力路由] 配置加载: mode={self.mode}, "
                    f"local={self.local_url}/{self.local_model}, "
                    f"cloud={self.cloud_url}/{self.cloud_model}, "
                    f"limit={self._cloud_daily_limit}")

    def get_routing_stats(self):
        """获取路由统计信息（供监控面板）"""
        today = date.today().isoformat()
        with self._cloud_lock:
            if self._cloud_calls_date != today:
                calls_today = 0
            else:
                calls_today = self._cloud_calls_today
        return {
            'mode': self.mode,
            'local_url': self.local_url,
            'local_model': self.local_model,
            'cloud_url': self.cloud_url,
            'cloud_model': self.cloud_model,
            'cloud_calls_today': calls_today,
            'cloud_daily_limit': self._cloud_daily_limit,
            'cloud_total_calls': self._cloud_total_calls,
            'cloud_configured': bool(self.cloud_url and self.cloud_key),
        }

    def _extract_api_error(self, resp):
        """从API响应中提取可读的错误信息"""
        try:
            body = resp.json()
            # OpenAI兼容格式
            err = body.get('error', {})
            if isinstance(err, dict):
                msg = err.get('message', '')
                code = err.get('code', '')
                if msg:
                    return f"{msg} (code={code})" if code else msg
            # 其他格式
            if 'message' in body:
                return body['message']
            return str(body)[:200]
        except Exception:
            return resp.text[:200] if resp.text else f'HTTP {resp.status_code}'

    def _do_request(self, api_url, api_key, model, messages, temp, tokens, timeout):
        """执行单次LLM API请求，返回 (success, response_json_or_error_str)"""
        # MiniMax等模型要求temperature在(0.0, 1.0]范围内，做安全钳位
        if temp is not None:
            temp = max(0.01, min(1.0, temp))
        try:
            resp = requests.post(
                api_url,
                headers={'Content-Type': 'application/json', 'Authorization': f'Bearer {api_key}'},
                json={'model': model, 'messages': messages,
                      'temperature': temp, 'max_tokens': tokens},
                timeout=timeout,
            )
            if resp.status_code != 200:
                detail = self._extract_api_error(resp)
                return False, f'HTTP {resp.status_code}: {detail}'
            return True, resp.json()
        except requests.exceptions.Timeout:
            return False, f'请求超时({timeout}秒)'
        except requests.exceptions.ConnectionError:
            return False, '无法连接到服务器（请检查URL是否正确）'
        except Exception as e:
            return False, str(e)

    def test_cloud_connection(self):
        """测试云端API连接（算力配置页面使用）"""
        if not self.cloud_url or not self.cloud_key:
            return {'success': False, 'error': '云端API未配置（URL或Key为空）'}
        test_messages = [{'role': 'user', 'content': '你好，请回复"连接成功"'}]
        model = self.cloud_model or self.local_model
        ok, result = self._do_request(
            self.cloud_url, self.cloud_key, model,
            test_messages, 0.1, 50, 15)
        if ok:
            try:
                content = result['choices'][0]['message']['content'][:100]
                return {'success': True, 'reply': content, 'model': model}
            except (KeyError, IndexError) as e:
                return {'success': False, 'error': f'响应格式异常：{result}'}
        return {'success': False, 'error': result}

    # ==================== 底层通信 ====================

    @staticmethod
    def _extract_api_error(resp):
        """从API HTTP错误响应中提取具体原因（而非只返回状态码）"""
        try:
            body = resp.json()
            # OpenAI/MiniMax 格式: {"error": {"message": "...", "type": "..."}}
            if 'error' in body:
                err = body['error']
                if isinstance(err, dict):
                    msg = err.get('message', '')
                    etype = err.get('type', '')
                    code = err.get('code', '')
                    parts = [p for p in [f'HTTP {resp.status_code}', etype, code, msg] if p]
                    return ' | '.join(parts)
                return f'HTTP {resp.status_code} | {err}'
            # 其他格式
            if 'message' in body:
                return f'HTTP {resp.status_code} | {body["message"]}'
            return f'HTTP {resp.status_code} | {json.dumps(body, ensure_ascii=False)[:200]}'
        except Exception:
            # 非JSON响应
            text = (resp.text or '')[:300].strip()
            return f'HTTP {resp.status_code} | {text}' if text else f'HTTP {resp.status_code}'

    def _get_timeout(self, task_type: TaskType) -> int:
        category = self.TIMEOUT_CONFIG.get(task_type, 'long')
        if category == 'short':
            return getattr(Config, 'LLM_TIMEOUT_SHORT', 60)
        return getattr(Config, 'LLM_TIMEOUT_LONG', 180)

    def _extract_message_content(self, result):
        """Extract text content from OpenAI-compatible chat responses."""
        message = result['choices'][0].get('message', {})
        content = message.get('content', '')
        if isinstance(content, list):
            parts = []
            for item in content:
                if isinstance(item, dict):
                    parts.append(str(item.get('text') or item.get('content') or ''))
                else:
                    parts.append(str(item))
            content = ''.join(parts)
        if content is None:
            content = ''
        return str(content)

    def _parse_llm_json_result(self, result):
        content = self._extract_message_content(result)
        content = self._strip_thinking(content)
        parsed = self._parse_json_response(content)
        return content, parsed

    def _call_llm(self, messages: List[Dict], task_type: TaskType = None,
                  temperature: float = None, max_tokens: int = None) -> LLMResponse:
        timeout = self._get_timeout(task_type) if task_type else Config.LLM_TIMEOUT
        temp = temperature or Config.LLM_DEFAULT_TEMPERATURE
        tokens = max_tokens or Config.LLM_DEFAULT_MAX_TOKENS

        api_url, api_key, model, is_cloud = self._resolve_endpoint()
        tag = '云端' if is_cloud else '本地'
        last_error = None
        last_raw = None

        for attempt in range(Config.LLM_RETRY_COUNT):
            ok, result = self._do_request(api_url, api_key, model, messages, temp, tokens, timeout)
            if ok:
                content, parsed = self._parse_llm_json_result(result)
                if parsed is not None:
                    return LLMResponse(data=parsed, is_mock=False, error_message=None, raw_response=content)
                last_error = 'JSON解析失败'
                last_raw = content
                logger.warning(f"[{tag}] 第{attempt+1}次JSON解析失败: {content[:300]}")
            else:
                last_error = result
                logger.warning(f"[{tag}] 第{attempt+1}次失败: {result}")
            if attempt < Config.LLM_RETRY_COUNT - 1:
                time.sleep(Config.LLM_RETRY_DELAY)

        if is_cloud and self.local_url:
            logger.info(f"[算力路由] 云端{Config.LLM_RETRY_COUNT}次失败，回退本地")
            for attempt in range(Config.LLM_RETRY_COUNT):
                ok, result = self._do_request(
                    self.local_url, self.local_key, self.local_model,
                    messages, temp, tokens, timeout)
                if not ok:
                    last_error = result
                    logger.warning(f"[本地] 第{attempt+1}次失败: {result}")
                    if attempt < Config.LLM_RETRY_COUNT - 1:
                        time.sleep(Config.LLM_RETRY_DELAY)
                    continue
                content, parsed = self._parse_llm_json_result(result)
                if parsed is not None:
                    return LLMResponse(data=parsed, is_mock=False, error_message=None, raw_response=content)
                last_error = 'JSON解析失败'
                last_raw = content
                logger.warning(f"[本地] 第{attempt+1}次JSON解析失败: {content[:300]}")
                if attempt < Config.LLM_RETRY_COUNT - 1:
                    time.sleep(Config.LLM_RETRY_DELAY)

        return LLMResponse(data=None, is_mock=True,
                           error_message=last_error or f'[{tag}] 所有尝试均失败',
                           raw_response=last_raw)

    def _call_llm_text(self, messages: list, task_type: TaskType = None,
                       temperature: float = None, max_tokens: int = None) -> str:
        """返回纯文本（用于苏格拉底式对话等），云端失败自动回退本地"""
        timeout = self._get_timeout(task_type) if task_type else Config.LLM_TIMEOUT
        temp = temperature or Config.LLM_DEFAULT_TEMPERATURE
        tokens = max_tokens or Config.LLM_DEFAULT_MAX_TOKENS

        # ★ 算力路由：根据模式选择端点
        api_url, api_key, model, is_cloud = self._resolve_endpoint()
        tag = '云端' if is_cloud else '本地'

        ok, result = self._do_request(api_url, api_key, model, messages, temp, tokens, timeout)
        if ok:
            raw_content = result['choices'][0]['message']['content']
            content = self._strip_thinking(raw_content)
            if not (content or '').strip() and (raw_content or '').strip():
                content = raw_content.strip()
            return content

        # ★ 云端失败 → 自动回退本地再试一次
        if is_cloud and self.local_url:
            logger.warning(f"[算力路由] 云端调用失败({result})，自动回退本地")
            ok2, result2 = self._do_request(
                self.local_url, self.local_key, self.local_model,
                messages, temp, tokens, timeout)
            if ok2:
                raw_content = result2['choices'][0]['message']['content']
                content = self._strip_thinking(raw_content)
                if not (content or '').strip() and (raw_content or '').strip():
                    content = raw_content.strip()
                return content
            # 本地也失败
            return f"抱歉，小林同学暂时无法回答（云端和本地均不可用）。请联系教师检查算力配置。"

        # 本地失败的友好提示
        return f"抱歉，小林同学暂时无法回答（{tag}：{result}）。请稍后再试。"

    def _parse_json_response(self, text):
        if not text:
            return None
        # 去掉 think 标签
        text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
        # 尝试直接解析
        try:
            return json.loads(text.strip())
        except:
            pass
        # 尝试提取代码块
        m = re.search(r'```(?:json)?\s*([\s\S]*?)```', text)
        if m:
            try:
                return json.loads(m.group(1).strip())
            except:
                pass
        # 尝试提取 { ... }
        m = re.search(r'\{[\s\S]*\}', text)
        if m:
            try:
                return json.loads(m.group(0))
            except:
                pass
        decoder = json.JSONDecoder()
        for match in re.finditer(r'\{', text):
            try:
                obj, _ = decoder.raw_decode(text[match.start():])
                if isinstance(obj, dict):
                    return obj
            except:
                continue
        return None

    # ==================== MOCK数据 ====================

    def _mock_knowledge_quiz(self, topics):
        topics_desc = ', '.join([self.topics_cn.get(t, t) for t in topics])
        return {
            '_is_mock': True, '_error_message': 'AI服务不可用，使用示例数据',
            'questions': [
                {
                    'id': 1, 'type': 'single_choice',
                    'question': f'关于{topics_desc}，以下哪项描述是正确的？',
                    'options': ['A. 冯·诺依曼架构的核心思想是存储程序', 'B. 第一台电子计算机使用晶体管',
                                'C. 人工智能的概念诞生于1990年代', 'D. 图灵机是一种实际的物理设备'],
                    'correct_answer': 'A',
                    'explanation': '冯·诺依曼架构的核心是"存储程序"概念，即程序和数据一起存储在内存中。'
                },
                {
                    'id': 2, 'type': 'fill_blank',
                    'question': '人工智能的三大学派分别是符号主义、____主义和行为主义。',
                    'correct_answer': '连接',
                    'explanation': '连接主义以神经网络为代表，强调通过大量简单单元的连接来模拟智能。'
                },
                {
                    'id': 3, 'type': 'short_answer',
                    'question': '请简述Python四种基本数据结构的特点。',
                    'reference_answer': 'Python四种基本数据结构：列表（有序可变序列）、元组（有序不可变序列）、字典（键值对映射）、集合（无序不重复元素集）。',
                    'keywords': ['分解', '模式识别', '抽象', '算法'],
                },
            ],
        }

    def _mock_ai_collab_task(self, topics, major):
        return {
            '_is_mock': True, '_error_message': 'AI服务不可用，使用示例数据',
            'task_title': '猜数字小游戏',
            'task_description': '使用Python和AI协同完成一个猜数字小游戏：程序随机生成1-100的整数，玩家反复猜测，程序提示"大了"或"小了"，直到猜对为止，最后显示猜了几次。',
            'requirements': [
                '用提示词引导AI生成猜数字游戏的基础代码',
                '阅读代码并理解每一行的作用',
                '做一个小改进（如限制最多猜10次、增加难度选择等）',
            ],
            'starter_prompt': '请帮我用Python写一个猜数字游戏，电脑随机生成1到100的数字，我来猜，程序告诉我猜大了还是小了。',
            'evaluation_criteria': '评分标准：提示词质量(8分)、代码理解度(8分)、创新改进(8分)、反思深度(6分)',
        }

    def _mock_thinking_task(self, topics):
        return {
            '_is_mock': True, '_error_message': 'AI服务不可用，使用示例数据',
            'task_title': '学生成绩管理系统的程序设计分析',
            'task_description': '请设计一个学生成绩管理程序，能够录入学生信息和成绩，计算平均分和排名，并按不同条件查询。请分析程序的数据结构设计和核心算法逻辑。',
            'sub_tasks': [
                {'step': '分解', 'prompt': '请将这个复杂的排课问题分解为若干子问题，列出你认为需要解决的关键子问题。'},
                {'step': '模式识别', 'prompt': '在排课问题中，哪些约束条件是重复出现的？请归纳这些模式。'},
                {'step': '抽象', 'prompt': '如果要用数据结构表示这个问题，你会如何抽象？请描述关键数据结构。'},
                {'step': '算法设计', 'prompt': '请用自然语言或伪代码描述一个基本的排课算法思路。'},
            ],
            'hints': '提示：可以考虑贪心算法或约束满足问题(CSP)的思路。',
        }

    def _mock_ethics_case(self, topics):
        return {
            '_is_mock': True, '_error_message': 'AI服务不可用，使用示例数据',
            'case_title': 'AI人脸识别在校园安全中的伦理困境',
            'case_description': '某大学计划在校园内部署AI人脸识别系统以提高安全管理效率。该系统可以自动记录学生出勤、识别陌生人员、追踪校园活动轨迹。然而，部分师生对此表示担忧，认为可能侵犯个人隐私和行动自由。',
            'analysis_questions': [
                '请从隐私保护的角度分析该系统可能带来的风险。',
                '请列举该系统的潜在收益和可能的伤害。',
                '如果你是决策者，你会如何在安全和隐私之间取得平衡？请提出至少3条具体措施。',
                '请结合我国《个人信息保护法》等相关法规，分析该系统需要满足哪些合规要求。',
            ],
            'reference_framework': '建议使用"技术向善"框架：必要性、最小化、透明度、可控性四原则进行分析。',
        }

    # ==================== 生成类方法 ====================

    def generate_knowledge_quiz(self, topics, difficulty='medium'):
        topics_desc = ', '.join([self.topics_cn.get(t, t) for t in topics])
        diff_cn = {'easy': '基础', 'medium': '进阶', 'hard': '挑战'}.get(difficulty, '进阶')

        prompt = f"""请根据以下Python知识主题，生成一组概念理解测试题目。

【学习主题】{topics_desc}
【难度等级】{diff_cn}
【课程背景】《Python程序设计》基础课程，面向非计算机专业本科学生

【要求】
生成3道题目，包含以下类型各1道：
1. 单选题(single_choice)：考查核心概念理解
2. 填空题(fill_blank)：考查关键术语记忆
3. 简答题(short_answer)：考查综合理解能力

【输出JSON格式】
{{
    "questions": [
        {{
            "id": 1,
            "type": "single_choice",
            "question": "题目文本",
            "options": ["A. 选项1", "B. 选项2", "C. 选项3", "D. 选项4"],
            "correct_answer": "A",
            "explanation": "解析说明"
        }},
        {{
            "id": 2,
            "type": "fill_blank",
            "question": "含____的题目文本",
            "correct_answer": "正确答案",
            "explanation": "解析说明"
        }},
        {{
            "id": 3,
            "type": "short_answer",
            "question": "简答题文本",
            "reference_answer": "参考答案（100-200字）",
            "keywords": ["关键词1", "关键词2", "关键词3"]
        }}
    ]
}}

请直接输出JSON，不要包含其他文本。"""

        messages = [
            {'role': 'system', 'content': '你是"小林同学"，《Python程序设计》课程的AI教学助手。你需要生成高质量的知识测试题目。直接输出JSON。'},
            {'role': 'user', 'content': prompt}
        ]

        response = self._call_llm(messages, TaskType.GENERATE_KNOWLEDGE_QUIZ)
        if response.data:
            response.data['_is_mock'] = response.is_mock
            response.data['_error_message'] = response.error_message
            return response.data
        return self._mock_knowledge_quiz(topics)

    def generate_ai_collab_task(self, topics, difficulty='medium', major_category='general'):
        topics_desc = ', '.join([self.topics_cn.get(t, t) for t in topics])
        major_cn = {'engineering': '工科', 'forestry': '林科', 'business': '经管', 'design': '设计'}.get(major_category, '通用')

        prompt = f"""请生成一个适合非计算机专业学生的Python编程实践任务。

【学习主题】{topics_desc}
【专业方向】{major_cn}类
【难度】{difficulty}
【课程背景】本课程强调"人机协作"工作范式——学生学会与编程实践

【重要：复杂度限制】
这是面向大一非计算机专业学生的通识课程，学生编程基础薄弱，任务必须简单：
- 最终代码控制在30-50行以内（含注释）
- 只使用Python基础语法（print、input、if/else、for循环、列表、字典）
- 可以使用简单的库（如random、math），但不要求复杂的第三方库
- 任务目标明确单一，不要有多个子任务
- 重点考查"人机协作能力"（如何写好提示词、如何理解AI代码），而非编程能力本身

【好的任务示例】
- 用Python制作一个简单的猜数字游戏
- 用Python统计一段文字中每个字出现的次数
- 用Python计算并展示BMI健康指数
- 用Python模拟简单的掷骰子概率统计

【不好的任务示例（太复杂）】
- 构建完整的数据可视化系统
- 实现机器学习分类器
- 开发Web应用或爬虫程序
- 使用pandas/numpy做数据分析

【任务设计要求】
学生需要：
1. 撰写提示词引导AI生成代码（重点！）
2. 阅读理解AI生成的代码
3. 做一些小修改（如改变输出格式、增加一个简单功能）
4. 反思人机协作过程

【输出JSON格式】
{{
    "task_title": "任务标题（简洁明确）",
    "task_description": "任务描述（100-150字，贴近生活或专业场景，说清楚要做什么）",
    "requirements": ["要求1", "要求2", "要求3"],
    "starter_prompt": "给学生参考的初始提示词示例",
    "evaluation_criteria": "评分标准说明（侧重提示词质量和代码理解，而非代码复杂度）"
}}

请直接输出JSON。"""

        messages = [
            {'role': 'system', 'content': '你是"小林同学"，专注于Python编程实践教学。生成贴近学生专业场景的Python编程任务。直接输出JSON。'},
            {'role': 'user', 'content': prompt}
        ]

        response = self._call_llm(messages, TaskType.GENERATE_AI_COLLAB_TASK)
        if response.data:
            response.data['_is_mock'] = response.is_mock
            response.data['_error_message'] = response.error_message
            return response.data
        return self._mock_ai_collab_task(topics, major_category)

    def generate_thinking_task(self, topics, difficulty='medium'):
        topics_desc = ', '.join([self.topics_cn.get(t, t) for t in topics])

        prompt = f"""请生成一道Python程序分析训练题。

【学习主题】{topics_desc}
【难度】{difficulty}

【要求】
设计一道需要学生阅读理解Python代码、分析程序逻辑、预测运行结果的题目。

【输出JSON格式】
{{
    "task_title": "任务标题",
    "task_description": "场景描述（100-200字）",
    "sub_tasks": [
        {{"step": "分解", "prompt": "引导学生进行问题分解的具体问题"}},
        {{"step": "模式识别", "prompt": "引导学生识别模式的具体问题"}},
        {{"step": "抽象", "prompt": "引导学生进行抽象建模的具体问题"}},
        {{"step": "算法设计", "prompt": "引导学生设计算法的具体问题"}}
    ],
    "hints": "适当的提示信息"
}}

请直接输出JSON。"""

        messages = [
            {'role': 'system', 'content': '你是"小林同学"，擅长出Python程序分析题，培养学生的代码阅读和逻辑分析能力。直接输出JSON。'},
            {'role': 'user', 'content': prompt}
        ]

        response = self._call_llm(messages, TaskType.GENERATE_THINKING_TASK)
        if response.data:
            response.data['_is_mock'] = response.is_mock
            response.data['_error_message'] = response.error_message
            return response.data
        return self._mock_thinking_task(topics)

    def generate_ethics_case(self, topics, difficulty='medium'):
        topics_desc = ', '.join([self.topics_cn.get(t, t) for t in topics])

        prompt = f"""请生成一个Python综合应用编程题。

【学习主题】{topics_desc}
【难度】{difficulty}
【课程背景】培养学生综合运用Python知识解决实际问题的能力

【输出JSON格式】
{{
    "case_title": "案例标题",
    "case_description": "案例背景描述（150-250字），要贴近学生生活或社会热点",
    "analysis_questions": [
        "分析角度1的问题",
        "分析角度2的问题",
        "决策建议类问题",
        "法规/规范相关问题"
    ],
    "reference_framework": "推荐的分析框架提示"
}}

请直接输出JSON。"""

        messages = [
            {'role': 'system', 'content': '你是"小林同学"，擅长设计Python综合应用编程题。生成需要综合运用所学知识的编程任务。直接输出JSON。'},
            {'role': 'user', 'content': prompt}
        ]

        response = self._call_llm(messages, TaskType.GENERATE_ETHICS_CASE)
        if response.data:
            response.data['_is_mock'] = response.is_mock
            response.data['_error_message'] = response.error_message
            return response.data
        return self._mock_ethics_case(topics)

    # ==================== 评估类方法 ====================

    def evaluate_quiz(self, questions, student_answers):
        prompt = f"""请评估学生的知识测试答案。

【题目】
{json.dumps(questions, ensure_ascii=False, indent=2)}

【学生答案】
{json.dumps(student_answers, ensure_ascii=False, indent=2)}

【评分标准】
- 单选题：答对10分，答错0分
- 填空题：答对8分，部分正确4分，答错0分
- 简答题：满分7分，根据关键词覆盖和表述质量综合评分
- 总分满分25分

【输出JSON】
{{
    "score": 总分(0-25),
    "details": [
        {{"id": 1, "score": 得分, "is_correct": true/false, "feedback": "评语"}},
        {{"id": 2, "score": 得分, "is_correct": true/false, "feedback": "评语"}},
        {{"id": 3, "score": 得分, "is_correct": true/false, "feedback": "评语"}}
    ],
    "overall_feedback": "总体评语（50-100字）"
}}"""

        messages = [
            {'role': 'system', 'content': '你是"小林同学"，公正客观地评估学生答案。直接输出JSON。'},
            {'role': 'user', 'content': prompt}
        ]

        response = self._call_llm(messages, TaskType.EVALUATE_QUIZ)
        if response.data:
            response.data['score'] = max(0, min(25, response.data.get('score', 0)))
            return response.data
        return {'score': 0, 'details': [], 'overall_feedback': '评估服务暂时不可用', '_is_mock': True}

    def evaluate_ai_collab(self, task, prompt_history, final_code, reflection):
        prompt = f"""请评估学生的编程实践任务完成情况。

【任务描述】
{task}

【学生的提示词交互记录】
{prompt_history}

【最终代码】
```python
{final_code}
```

【学生反思】
{reflection}

【评分标准（满分30分）】
- 提示词质量(8分)：是否清晰、具体、有层次
- 代码理解度(8分)：是否理解AI生成的代码、能否解释关键逻辑
- 创新改进(8分)：是否在AI基础上做了有意义的改进
- 反思深度(6分)：对人机协作过程的思考深度

【输出JSON】
{{
    "score": 总分(0-30),
    "prompt_quality_score": 0-8,
    "code_understanding_score": 0-8,
    "innovation_score": 0-8,
    "reflection_score": 0-6,
    "feedback": "综合评语（100-200字）",
    "suggestions": ["改进建议1", "改进建议2"]
}}"""

        messages = [
            {'role': 'system', 'content': '你是"小林同学"，评估非计算机专业学生的Python编程实践能力。重点关注代码正确性、逻辑清晰度和编程规范。直接输出JSON。'},
            {'role': 'user', 'content': prompt}
        ]

        response = self._call_llm(messages, TaskType.EVALUATE_AI_COLLAB)
        if response.data:
            response.data['score'] = max(0, min(30, response.data.get('score', 0)))
            return response.data
        return {'score': 0, 'feedback': '评估服务暂时不可用', '_is_mock': True}

    def evaluate_thinking(self, task, student_answer):
        prompt = f"""请评估学生的程序分析分析。

【任务】
{task}

【学生作答】
{student_answer}

【评分标准（满分25分）】
- 问题分解(7分)：分解是否合理、完整
- 模式识别(6分)：是否准确识别关键模式
- 抽象建模(6分)：抽象是否恰当
- 算法设计(6分)：算法思路是否可行

【输出JSON】
{{
    "score": 总分(0-25),
    "decomposition_score": 0-7,
    "pattern_score": 0-6,
    "abstraction_score": 0-6,
    "algorithm_score": 0-6,
    "feedback": "综合评语（100-150字）",
    "suggestions": ["建议1", "建议2"]
}}"""

        messages = [
            {'role': 'system', 'content': '你是"小林同学"，评估学生的Python程序分析能力。直接输出JSON。'},
            {'role': 'user', 'content': prompt}
        ]

        response = self._call_llm(messages, TaskType.EVALUATE_THINKING)
        if response.data:
            response.data['score'] = max(0, min(25, response.data.get('score', 0)))
            return response.data
        return {'score': 0, 'feedback': '评估服务暂时不可用', '_is_mock': True}

    def evaluate_ethics(self, case, student_answer):
        prompt = f"""请评估学生的综合应用。

【案例】
{case}

【学生分析】
{student_answer}

【评分标准（满分20分）】
- 问题识别(5分)：是否准确识别伦理问题
- 多角度分析(5分)：是否从多个利益相关方角度思考
- 解决方案(5分)：建议是否具体可行
- 法规意识(5分)：是否有法律法规层面的思考

【输出JSON】
{{
    "score": 总分(0-20),
    "identification_score": 0-5,
    "analysis_score": 0-5,
    "solution_score": 0-5,
    "regulation_score": 0-5,
    "feedback": "综合评语（100-150字）",
    "suggestions": ["建议1", "建议2"]
}}"""

        messages = [
            {'role': 'system', 'content': '你是"小林同学"，评估学生的Python综合应用能力。直接输出JSON。'},
            {'role': 'user', 'content': prompt}
        ]

        response = self._call_llm(messages, TaskType.EVALUATE_ETHICS)
        if response.data:
            response.data['score'] = max(0, min(20, response.data.get('score', 0)))
            return response.data
        return {'score': 0, 'feedback': '评估服务暂时不可用', '_is_mock': True}

    # ==================== 综合评价 ====================

    def generate_evaluation(self, submission_data, topics):
        topics_desc = ', '.join([self.topics_cn.get(t, t) for t in topics])

        prompt = f"""请根据学生本次实验的各阶段表现生成综合评价。

【学习主题】{topics_desc}

【各阶段得分】
1. 概念理解（满分25分）：{submission_data.get('quiz_score', 0)}分
2. 编程实践（满分30分）：{submission_data.get('ai_collab_score', 0)}分
3. 程序分析（满分25分）：{submission_data.get('thinking_score', 0)}分
4. 综合应用（满分20分）：{submission_data.get('ethics_score', 0)}分

【输出JSON】
{{
    "total_score": 总分,
    "grade": "优秀/良好/中等/及格/不及格",
    "strengths": ["优点1", "优点2"],
    "weaknesses": ["不足1", "不足2"],
    "suggestions": ["建议1", "建议2"],
    "evaluation": "200-300字的综合评语"
}}"""

        messages = [
            {'role': 'system', 'content': '你是"小林同学"，生成鼓励性且客观的学习评价。直接输出JSON。'},
            {'role': 'user', 'content': prompt}
        ]

        response = self._call_llm(messages, TaskType.GENERATE_EVALUATION)
        if response.data:
            response.data['_is_mock'] = response.is_mock
            return response.data
        total = sum([
            submission_data.get('quiz_score', 0),
            submission_data.get('ai_collab_score', 0),
            submission_data.get('thinking_score', 0),
            submission_data.get('ethics_score', 0),
        ])
        grade = '优秀' if total >= 90 else '良好' if total >= 80 else '中等' if total >= 70 else '及格' if total >= 60 else '不及格'
        return {
            'total_score': total, 'grade': grade,
            'strengths': ['完成了所有实验阶段'], 'weaknesses': ['需进一步提升'],
            'suggestions': ['建议多练习Python编程'], 'evaluation': '综合评价服务暂时不可用。',
            '_is_mock': True,
        }

    # ==================== 苏格拉底式辅导 ====================

    def _strip_thinking(self, text):
        """去除模型输出中的思考过程，只保留最终回复

        处理三种常见情况：
        1. <think>...</think> 标签包裹的思考（标准DeepSeek-R1格式）
        2. <think>... 未闭合标签，但后面已有正式回复
        3. 无标签的思考过程（中文或英文推理 + 正式回复）
        """
        if not text:
            return text

        original_text = text
        text = text.strip()

        # 策略1：标准 <think>...</think>，取最后一个 </think> 之后的正式回复
        think_end = text.rfind('</think>')
        if think_end != -1:
            after_think = text[think_end + len('</think>'):].strip()
            if after_think:
                return after_think

        # 策略2：存在未闭合 <think> 时，不要直接截成空字符串。
        # 优先尝试从“正式回复起点”抽取，例如“小林同学 ……”
        think_start = text.find('<think>')
        if think_start != -1:
            marker_after_think = re.search(r'(小林同学\s*[📚💡🤔✨😊🎯：:,，!！?？]*)', text[think_start:])
            if marker_after_think:
                start_pos = think_start + marker_after_think.start()
                candidate = text[start_pos:].strip()
                if candidate:
                    return candidate

            # 若能找到另一个较明确的中文正式回复起点，也从那里开始保留
            lines = [line.strip() for line in text[think_start:].split('\n') if line.strip()]
            for line in lines:
                if re.search(r'[\u4e00-\u9fff]', line) and not line.lower().startswith('<think'):
                    if len(line) >= 6:
                        return line

            # 保底：不修改原文，交给后续规则继续判断，避免误清空
            text = original_text.strip()

        # 策略3：检测“小林同学”标记作为正式回复起点
        # DeepSeek-R1常见模式：先输出一大段推理分析，然后以“小林同学”开头输出正式回复
        marker_pattern = re.search(r'(^|\n)\s*(小林同学\s*[📚💡🤔✨😊🎯：:,，!！?？]*)', text)
        if marker_pattern:
            before = text[:marker_pattern.start()].strip()
            after = text[marker_pattern.start():].strip()
            if after and before and len(before) > 20:
                return after

        # 策略4：检测英文推理 + 中文回复的模式
        lines = text.split('\n')
        chinese_start = -1
        for i, line in enumerate(lines):
            stripped = line.strip()
            if not stripped:
                continue
            if re.search(r'[\u4e00-\u9fff]', stripped):
                chinese_start = i
                break
        if chinese_start > 0:
            english_lines = [l.strip() for l in lines[:chinese_start] if l.strip()]
            if len(english_lines) >= 2 and all(not re.search(r'[\u4e00-\u9fff]', l) for l in english_lines):
                text = '\n'.join(lines[chinese_start:]).strip()

        return text.strip() or original_text.strip()

    def socratic_chat(self, student_message, context, phase, history=None):
        phase_hints = {
            'quiz': """- 你可以看到学生当前正在作答的所有题目（在实验背景中）
- 当学生提到"第几题"时，你可以根据题目内容进行针对性引导
- 引导学生回忆相关知识点，不直接告诉答案
- 可以提问："你还记得这个概念是在哪个章节学的吗？"
- 对于选择题，帮助学生用排除法缩小选项范围
- 对于填空题和简答题，引导学生回忆关键概念""",
            'ai_collab': """- 你可以看到学生当前的编程任务详情（在实验背景中）
- 引导学生思考提示词如何改进
- 提问："你觉得这段代码的核心逻辑是什么？"
- 鼓励学生先理解AI生成的代码再修改
- 可以针对具体任务要求，引导学生分析需要哪些功能模块""",
            'thinking': """- 你可以看到学生当前的程序分析任务（在实验背景中）
- 引导学生将问题拆解为更小的部分
- 提问："这个问题可以分成哪几个步骤？"
- 帮助学生识别问题中的模式
- 可以针对具体场景，引导学生思考分解、抽象、模式识别等策略""",
            'ethics': """- 你可以看到学生当前的伦理分析案例（在实验背景中）
- 引导学生从多个角度思考伦理问题
- 提问："如果你是这个AI系统的用户，你会有什么感受？"
- 鼓励学生思考技术与社会的关系
- 可以针对具体案例中的冲突点，引导学生进行多方利益分析""",
        }.get(phase, '- 用启发式问题引导学生思考')

        system_prompt = f"""你是一位采用苏格拉底教学法的AI课程辅导老师。你的名字叫"小林同学"。

## 核心原则（必须严格遵守）

1. **绝对不能直接给出答案**：无论学生怎么请求，都不能直接告诉他们答案。
2. **通过提问引导思考**：用启发式问题帮助学生自己发现答案。
3. **循序渐进**：根据学生的回答逐步深入，不要一次给出太多提示。
4. **鼓励与耐心**：保持友善和鼓励的态度，让学生感到安全，不怕犯错。
5. **控制回复长度**：每次回复简洁（2-4句话），以1-2个关键问题结尾。

## 你拥有的信息
- 你可以在"当前实验背景"消息中看到学生正在做的**所有题目内容**
- 当学生提到"第一题"、"第二题"等，你知道具体是什么题目
- 利用这些信息进行**针对性引导**，但绝不直接透露答案

## 你可以做的事情
- 针对具体题目提出引导性问题："这道题考查的是哪个知识点？你对这个概念有什么了解？"
- 给出思考方向："试着想想，这个选项和其他选项有什么区别？"
- 用类比解释概念
- 肯定学生的正确思路

## 你绝对不能做的事情
- ❌ 直接给出任何答案（包括选择题的选项字母）
- ❌ 在学生多次追问后妥协给出答案
- ❌ 说"答案是..."、"你应该选..."、"正确答案是..."等
- ❌ 变相暗示答案（如"A和C都不对，B和D中再想想"这种只剩一个选项的暗示）

## 当学生直接要求答案时
坚定但友善地拒绝，然后根据具体题目内容将对话引导回思考过程。例如：
"我理解你想要答案，但自己思考出来才能真正掌握知识。来，我们一起看看这道题——{{针对题目的引导性问题}}"

## 当前辅导阶段：{phase}

## 阶段特定引导策略
{phase_hints}

## 回复格式要求
- 使用中文回复
- 回复必须以"小林同学"开头（这是你的身份标识）
- 保持简洁友善（2-4句话，150字以内）
- 以1-2个启发式问题结尾
- 适当使用emoji增加亲和力
- **禁止输出任何思考过程、推理分析，直接输出最终回复**"""

        messages = [{'role': 'system', 'content': system_prompt}]
        if context:
            messages.append({'role': 'user', 'content': f'[当前实验背景]\n{context}'})
            messages.append({'role': 'assistant', 'content': '小林同学 📚 好的，我已经看到了你正在做的题目，随时准备帮助你！有什么不懂的尽管问～'})
        if history:
            for h in history[-6:]:  # 最近6轮
                role = 'assistant' if h['role'] == 'tutor' else 'user'
                messages.append({'role': role, 'content': h['content']})
        messages.append({'role': 'user', 'content': student_message})

        response = self._call_llm_text(messages, TaskType.SOCRATIC_TUTOR, temperature=0.8, max_tokens=500)
        return self._strip_thinking(response)

    # ==================== 编程实践中的AI辅助 ====================

    def ai_collab_assist(self, student_prompt, task_context):
        """在编程实践阶段，响应学生的编程提示词"""
        system_prompt = """你是一个Python编程助手，正在帮助大一非计算机专业学生完成编程任务。

【代码复杂度要求】
- 代码控制在30-50行以内（含注释）
- 只使用Python基础语法：print、input、if/else、for/while循环、列表、字典、函数定义
- 可以使用简单标准库（random、math等），避免复杂的第三方库
- 每个关键步骤都要加中文注释，帮助学生理解
- 代码结构清晰，逻辑简单直白

【回复格式】
- 先用1-2句话说明代码思路
- 然后给出完整可运行的代码（用```python```包裹）
- 最后简要说明学生可以尝试修改的地方（1-2个小改进方向）
- 禁止输出任何思考过程或推理分析，直接输出最终内容"""

        messages = [
            {'role': 'system', 'content': system_prompt},
            {'role': 'user', 'content': f'[任务背景] {task_context}\n\n[我的需求] {student_prompt}'}
        ]

        return self._call_llm_text(messages, TaskType.AI_COLLAB_ASSIST, temperature=0.7, max_tokens=4000)

    # ==================== 学生画像 ====================

    def generate_student_profile(self, student_info, submissions_data, topic_stats):
        prompt = f"""请为以下学生生成学习画像分析报告。

【学生信息】
{json.dumps(student_info, ensure_ascii=False, indent=2)}

【提交历史】
{json.dumps(submissions_data[:10], ensure_ascii=False, indent=2)}

【知识点掌握情况】
{json.dumps(topic_stats, ensure_ascii=False, indent=2)}

【输出JSON】
{{
    "overall_level": "优秀/良好/中等/待提升",
    "computational_thinking_index": 0-100,
    "ai_literacy_index": 0-100,
    "cross_disciplinary_index": 0-100,
    "ethics_awareness_index": 0-100,
    "ai_dependency_index": 0-100,
    "strengths": ["优势1", "优势2"],
    "weaknesses": ["不足1", "不足2"],
    "recommendations": ["建议1", "建议2"],
    "summary": "200字的综合画像描述"
}}"""

        messages = [
            {'role': 'system', 'content': '你是"小林同学"，根据学生数据生成全面的学习画像。直接输出JSON。'},
            {'role': 'user', 'content': prompt}
        ]

        response = self._call_llm(messages, TaskType.GENERATE_PROFILE)
        if response.data:
            return response.data
        return {
            'overall_level': '待评估', 'computational_thinking_index': 50,
            'ai_literacy_index': 50, 'cross_disciplinary_index': 50,
            'ethics_awareness_index': 50, 'ai_dependency_index': 50,
            'strengths': ['数据不足'], 'weaknesses': ['需要更多数据'],
            'recommendations': ['请完成更多实验'], 'summary': '画像数据不足，请完成更多实验后再生成。',
            '_is_mock': True,
        }


    # ==================== 课前闭卷测试 ====================

    def generate_pre_class_quiz(self, prev_topics, prev_experiment_title, difficulty='medium',
                                 prev_quiz_questions=None, prev_quiz_student_answers=None,
                                 prev_ai_collab_code=None,
                                 prev_thinking_task=None, prev_thinking_answer=None):
        """生成课前闭卷3道题目：
        第1题：基于上次实验概念理解（quiz）题目延伸出的考察题
        第2题：基于学生上次编程实践提交代码，生成代码改错或代码填空题
        第3题：基于上次实验程序分析任务的延伸考察题
        """
        topics_str = '、'.join(prev_topics) if prev_topics else '人工智能基础'


        # ── 第1题素材：上次概念理解（全部题目 + 学生完整作答） ──
        q1_context = ''
        if prev_quiz_questions:
            try:
                import json as _json
                qs_raw = _json.loads(prev_quiz_questions) if isinstance(prev_quiz_questions, str) else prev_quiz_questions
                # quiz_questions 存储的可能是 {"questions": [...]} 整体dict，需取出列表
                if isinstance(qs_raw, dict):
                    qs = qs_raw.get('questions', [])
                elif isinstance(qs_raw, list):
                    qs = qs_raw
                else:
                    qs = []
                ans_map = {}
                if prev_quiz_student_answers:
                    raw_ans = _json.loads(prev_quiz_student_answers) if isinstance(prev_quiz_student_answers, str) else prev_quiz_student_answers
                    if isinstance(raw_ans, dict):
                        ans_map = {str(k): v for k, v in raw_ans.items()}
                if qs:
                    q_lines_ctx = ['上次实验「' + prev_experiment_title + '」概念理解全部题目及学生作答：']
                    for idx_q, q in enumerate(qs):
                        if not isinstance(q, dict):
                            continue
                        qid = str(q.get('id', idx_q + 1))
                        q_text = q.get('question', '')
                        opts = q.get('options', [])
                        # options 可能是 list 或 dict，统一转为可读字符串
                        if isinstance(opts, dict):
                            opts_str = '  选项：' + '  '.join(k + '. ' + v for k, v in opts.items())
                        elif isinstance(opts, list) and opts:
                            opts_str = '  选项：' + '  '.join(str(o) for o in opts)
                        else:
                            opts_str = ''
                        student_ans = ans_map.get(qid, ans_map.get(str(idx_q), '（未作答）'))
                        correct = q.get('correct', q.get('correct_answer', ''))
                        q_lines_ctx.append(
                            '第' + str(idx_q+1) + '题[' + q.get('type','') + ']：' + q_text
                            + opts_str
                            + '  学生作答：' + str(student_ans)
                            + ('  正确答案：' + str(correct) if correct else '')
                        )
                    q1_context = '\n'.join(q_lines_ctx)
            except Exception:
                pass
        if not q1_context:
            q1_context = '上次实验「' + prev_experiment_title + '」涉及主题：' + topics_str

        # ── 第2题素材：学生提交的编程实践代码 ──
        q2_context = ''
        if prev_ai_collab_code and len(prev_ai_collab_code.strip()) > 30:
            code_snippet = prev_ai_collab_code.strip()[:800]
            q2_context = '学生上次实验提交的编程实践代码（节选）：\n```python\n' + code_snippet + '\n```'
        else:
            q2_context = f'上次实验「{prev_experiment_title}」的编程实践任务（主题：{topics_str}）'


        # ── 第3题素材：上次程序分析任务和学生作答（自动解析JSON格式）──
        q3_context = ''
        if prev_thinking_task:
            try:
                import json as _json
                t = None
                if isinstance(prev_thinking_task, str) and prev_thinking_task.strip().startswith('{'):
                    t = _json.loads(prev_thinking_task)
                if t:
                    task_title = t.get('task_title', '')
                    task_desc  = t.get('task_description', '')
                    sub_tasks  = t.get('sub_tasks', [])
                    q3_context = '上次实验程序分析任务标题：' + task_title
                    if task_desc:
                        q3_context += '\n任务场景描述：' + task_desc
                    if sub_tasks:
                        steps = '、'.join(s.get('step','') for s in sub_tasks if s.get('step'))
                        prompts = [s.get('prompt','') for s in sub_tasks if s.get('prompt')]
                        q3_context += '\n包含程序分析步骤：' + steps
                        if prompts:
                            q3_context += '\n各步骤引导问题：' + '  '.join(prompts[:4])
                else:
                    q3_context = '上次实验程序分析任务：' + str(prev_thinking_task)
            except Exception:
                q3_context = '上次实验程序分析任务：' + str(prev_thinking_task)
            if prev_thinking_answer:
                q3_context += '\n学生当时的完整作答：' + str(prev_thinking_answer)[:500]
        else:
            q3_context = '上次实验「' + prev_experiment_title + '」的程序分析主题：' + topics_str

        prompt = f"""请为《Python程序设计》课程生成课前闭卷测试，共3道题，严格按以下要求出题：

【第1题】概念理解回顾题（单选题）
素材：{q1_context}
要求：围绕该知识点出一道单选题，考察学生对核心概念的记忆与理解，不得照抄原题，应换一个角度考察。

【第2题】代码实践题（代码改错题 或 代码填空题，二选一）
素材：{q2_context}
要求：从该代码中提炼出一个典型知识点，出一道"代码改错"或"代码填空"题。
- 代码改错：给出含1-2处bug的代码片段，让学生找出并改正
- 代码填空：给出含空白的代码片段，让学生填写关键代码
选择更能考察理解的题型。题目中必须包含代码块。

【第3题】程序分析延伸题（简答题）
素材：{q3_context}
要求：围绕程序分析（问题分解、模式识别、抽象、算法设计之一），出一道需要学生用自己语言组织回答的简答题，字数要求50-100字。

输出严格JSON格式：
{{
    "questions": [
        {{
            "type": "single_choice",
            "question": "第1题题目",
            "options": {{"A": "...", "B": "...", "C": "...", "D": "..."}},
            "correct_answer": "正确选项字母",
            "explanation": "答案解析"
        }},
        {{
            "type": "code_practice",
            "sub_type": "debug 或 fill_blank",
            "question": "题目说明（含代码块，使用```python包裹）",
            "correct_answer": "正确代码或填空答案",
            "explanation": "解析说明"
        }},
        {{
            "type": "short_answer",
            "question": "第3题简答题目",
            "reference_answer": "参考答案要点",
            "key_points": ["要点1", "要点2", "要点3"]
        }}
    ]
}}"""

        messages = [
            {'role': 'system', 'content': (
                '你是《Python程序设计》课程的出题专家。'
                '出题必须紧扣提供的学生代码和实验素材，不得凭空编造无关内容。'
                '代码题中的代码必须来自或改编自提供的素材代码。'
                '直接输出合法JSON，不要有多余文字。'
            )},
            {'role': 'user', 'content': prompt}
        ]
        response = self._call_llm(messages, TaskType.GENERATE_KNOWLEDGE_QUIZ)
        if response.data and 'questions' in response.data:
            return response.data

        # fallback
        import json as _json
        _fb_template = '# ' + topics_str + '示例代码\nresult = []\nfor i in range(10):\n    result.append(i * 2)\nprint(result)'
        fallback_code = prev_ai_collab_code.strip()[:300] if prev_ai_collab_code else _fb_template
        return {
            'questions': [
                {
                    'type': 'single_choice',
                    'question': f'关于「{prev_experiment_title}」中的{topics_str}，下列说法正确的是？',
                    'options': {'A': f'{topics_str}只能应用于单一领域', 'B': f'{topics_str}是AI协同的重要组成', 'C': 'AI无法辅助完成此类任务', 'D': '该技术对数据质量没有要求'},
                    'correct_answer': 'B',
                    'explanation': f'{topics_str}在AI协同中具有重要价值。'
                },
                {
                    'type': 'code_practice',
                    'sub_type': 'debug',
                    'question': '以下是上次实验中的代码片段，其中有1处错误，请找出并改正：\n```python\n' + fallback_code[:200] + '\n```',
                    'correct_answer': '修正语法错误（如缩进、冒号等）',
                    'explanation': 'Python代码中for循环后必须有冒号。'
                },
                {
                    'type': 'short_answer',
                    'question': f'请用自己的话说明，在「{prev_experiment_title}」实验中，你是如何对问题进行分解（问题分解是程序分析的核心步骤之一）？',
                    'reference_answer': '描述将复杂问题拆解为若干子问题的过程',
                    'key_points': ['明确子问题', '逐步求解', '结果整合']
                }
            ]
        }

    def evaluate_pre_class_quiz(self, questions, student_answers):
        """评分闭卷测试，同时给出每题详细反馈。
        严格按题目实际顺序逐题评分，避免题号错位。
        """
        TYPE_LABEL = {
            'single_choice': '单选题（概念理解）',
            'true_false':     '判断题',
            'code_practice':  '代码实践题（代码改错/填空）',
            'short_answer':   '简答题（程序分析）',
        }
        SCORE_RULE = {
            'single_choice': '完全正确得10分，错误0分',
            'true_false':     '完全正确得10分，错误0分',
            'code_practice':  '关键修改点全对得8-10分，思路对但不完整得4-7分，明显错误得0-3分',
            'short_answer':   '按要点覆盖程度0-10分，能清晰说明核心思路即可满分',
        }

        # 逐题构建评分说明，题号严格与questions数组下标一致
        q_lines = []
        for i, q in enumerate(questions):
            qtype = q.get('type', 'short_answer')
            label = TYPE_LABEL.get(qtype, qtype)
            rule  = SCORE_RULE.get(qtype, '按质量0-10分')
            ans_key = (q.get('correct_answer') or q.get('reference_answer') or
                       q.get('correct_code') or '（见参考答案）')
            student_ans = student_answers.get(str(i), student_answers.get(i, '（未作答）'))
            q_lines.append(
                '【第' + str(i+1) + '题 index=' + str(i) + ' 题型=' + label + '】\n'
                + '题目：' + q.get('question', '') + '\n'
                + '参考答案：' + str(ans_key) + '\n'
                + '学生答案：' + str(student_ans) + '\n'
                + '评分规则：' + rule
            )

        questions_block = '\n\n'.join(q_lines)

        prompt = f"""请对以下课前闭卷测试逐题评分，共{len(questions)}题，总分30分（每题10分）。

{questions_block}

⚠️ 重要：输出的 details 数组必须严格按 index=0,1,2 的顺序排列，第1项对应index=0，以此类推，不得乱序。

输出JSON：
{{
    "total_score": 数值(0-30),
    "details": [
        {{
            "question_index": 0,
            "question_type": "题型名称",
            "student_answer": "学生作答摘要（不超过30字）",
            "score": 数值(0-10),
            "is_correct": true/false,
            "feedback": "点评（指出对错和原因，不超过60字）"
        }}
    ],
    "overall_comment": "整体评价（不超过60字）"
}}"""

        messages = [
            {'role': 'system', 'content': (
                '你是公正严格的AI课程评阅老师。'
                '评分必须针对题目实际内容，不得混淆题号。'
                '直接输出合法JSON，details数组按question_index升序排列。'
            )},
            {'role': 'user', 'content': prompt}
        ]
        response = self._call_llm(messages, TaskType.EVALUATE_QUIZ)
        if response.data and 'total_score' in response.data:
            # 防御：按 question_index 排序，避免 LLM 乱序
            details = response.data.get('details', [])
            details.sort(key=lambda x: x.get('question_index', 99))
            response.data['details'] = details
            return response.data
        return {'total_score': 0, 'details': [], 'overall_comment': '评分失败，请联系教师。'}

    def generate_pre_class_evaluation(self, student_name, prev_experiment_title,
                                       prev_submission_data, quiz_score, quiz_details):
        """结合闭卷成绩和上次实验完成情况，生成个性化学习评价和建议"""
        prev_scores = {
            '概念理解': prev_submission_data.get('quiz_score', 0),
            '编程实践': prev_submission_data.get('ai_collab_score', 0),
            '程序分析': prev_submission_data.get('thinking_score', 0),
            '综合应用': prev_submission_data.get('ethics_score', 0),
            '总分': prev_submission_data.get('total_score', 0),
        }
        details_text = ''
        for i, d in enumerate(quiz_details):
            details_text += f'\n  第{i+1}题：{d.get("score",0)}/10分 — {d.get("feedback","")}'

        prompt = f"""请为学生「{student_name}」生成课前闭卷测试评价和本次实验的学习建议。

【上一次实验】{prev_experiment_title}
【上次实验各阶段得分】
{json.dumps(prev_scores, ensure_ascii=False)}
【本次课前闭卷测试得分】{quiz_score}/30分
【各题情况】{details_text}

请用温和、鼓励的语气，结合两份数据，生成：
1. 对学生上次实验掌握程度的客观评价（指出具体的薄弱点，如有）
2. 本次课前测试反映的知识遗留问题
3. 针对本次新实验的3条具体学习建议

输出格式（直接输出文字，不要JSON）：

**📊 上次实验学情回顾**
（2-3句，结合各阶段分数）

**📝 课前测试分析**
（1-2句，结合答题情况）

**🎯 本次实验学习建议**
1. ...
2. ...
3. ..."""

        messages = [
            {'role': 'system', 'content': '你是关心学生的AI课程辅导老师，语言温和具体，建议有针对性。'},
            {'role': 'user', 'content': prompt}
        ]
        return self._call_llm_text(messages, TaskType.GENERATE_EVALUATION, temperature=0.7, max_tokens=600)

    # ==================== 教师学情预警 ====================

    def generate_warning_analysis(self, student_info, warning_data, chat_stats, chat_summary):
        """为教师生成学生预警深度分析与干预建议"""
        warnings_text = '\n'.join([
            f"  - [{w['severity'].upper()}] {w['message']}"
            for w in warning_data.get('warnings', [])
        ])
        stats = warning_data.get('stats', {})
        chat_q_text = ''
        if chat_summary:
            chat_q_text = '\n'.join([
                f"  - [{c.get('phase','?')}阶段 {c.get('time','')}] {c.get('content','')}"
                for c in chat_summary[:15]
            ])
        phase_dist_json = json.dumps(chat_stats.get('phase_distribution', {}), ensure_ascii=False)

        prompt = f"""请根据以下数据为教师生成学情预警分析报告。

## 学生信息
姓名：{student_info.get('name','?')}，学号：{student_info.get('student_id','?')}，班级：{student_info.get('class_name','?')}

## 系统预警信号
{warnings_text or '无预警'}

## 实验完成情况
- 已完成：{stats.get('completed_count',0)} 次，进行中/未完成：{stats.get('in_progress_count',0)} 次
- 各阶段平均分：概念理解 {stats.get('avg_quiz_score',0):.0f}/25，AI协同 {stats.get('avg_ai_collab_score',0):.0f}/30，程序分析 {stats.get('avg_thinking_score',0):.0f}/25，综合应用 {stats.get('avg_ethics_score',0):.0f}/20
- 总体均分：{stats.get('avg_total_score',0):.0f}/100

## AI辅导（小林同学）互动数据
- 总对话：{chat_stats.get('total_messages',0)} 条（学生 {chat_stats.get('student_messages',0)} 条）
- 涉及 {chat_stats.get('total_sessions',0)} 次实验，求助频率：{chat_stats.get('help_seeking_level','无数据')}
- 阶段分布：{phase_dist_json}

## 学生近期向AI提出的问题
{chat_q_text or '暂无对话记录'}

请用中文输出（不要JSON）：

### 🔍 问题诊断
（2-3句，结合成绩和对话数据，指出核心薄弱点）

### 💡 四阶段针对性建议
（分别对概念理解/AI协同/程序分析/综合应用中最薄弱的方面给出建议，2-4条）

### 💬 建议面谈话术
（1-2句建议教师与该学生沟通时的开场白，语气温和）"""

        messages = [
            {'role': 'system', 'content': '你是经验丰富的AI课程教学督导专家，善于从数据中发现学生的真实困难并给出精准干预建议。'},
            {'role': 'user', 'content': prompt}
        ]
        return self._call_llm_text(messages, TaskType.GENERATE_EVALUATION, temperature=0.7, max_tokens=1200)


    # ==================== 三段式期末考试 ====================

    def generate_exam_phase1(self, topics, difficulty='medium'):
        """生成第一段：闭卷基础题（15道，满分20分，适配20分钟考试时间）"""
        topics_cn = [self.topics_cn.get(t, t) for t in topics[:3]]
        mock = [
            {"type": "single_choice", "question": "以下关于机器学习的说法，正确的是？",
             "options": {"A": "从数据中自动学习规律", "B": "基于规则推理", "C": "随机生成答案", "D": "查询数据库"}, "answer": "A", "points": 2},
            {"type": "single_choice", "question": "Python中用于科学计算的核心库是？",
             "options": {"A": "requests", "B": "numpy", "C": "flask", "D": "sqlite3"}, "answer": "B", "points": 2},
            {"type": "single_choice", "question": "以下哪种不属于监督学习算法？",
             "options": {"A": "线性回归", "B": "K-Means聚类", "C": "决策树", "D": "支持向量机"}, "answer": "B", "points": 1},
            {"type": "single_choice", "question": "GPT中的T代表什么？",
             "options": {"A": "Training", "B": "Transformer", "C": "Transfer", "D": "Tensor"}, "answer": "B", "points": 1},
            {"type": "single_choice", "question": "以下哪个是Python的合法变量名？",
             "options": {"A": "2name", "B": "my-var", "C": "_count", "D": "class"}, "answer": "C", "points": 1},
            {"type": "single_choice", "question": "神经网络中激活函数的主要作用是？",
             "options": {"A": "加速训练", "B": "引入非线性", "C": "防止过拟合", "D": "降低维度"}, "answer": "B", "points": 1},
            {"type": "single_choice", "question": "以下关于提示词工程的说法，错误的是？",
             "options": {"A": "提示词应该尽量具体明确", "B": "可以通过few-shot示例引导模型", "C": "提示词越长效果越好", "D": "可以为模型指定输出格式"}, "answer": "C", "points": 1},
            {"type": "single_choice", "question": "Python中 list.append() 和 list.extend() 的区别是？",
             "options": {"A": "没有区别", "B": "append添加单个元素，extend合并可迭代对象", "C": "append更快", "D": "extend只能添加列表"}, "answer": "B", "points": 1},
            {"type": "fill_blank", "question": "程序分析四要素：分解、______、抽象、算法设计。", "answer": "模式识别", "points": 1},
            {"type": "fill_blank", "question": "大语言模型生成回复的概率机制叫______采样。", "answer": "温度（Temperature）", "points": 1},
            {"type": "fill_blank", "question": "在Python中，用于异常处理的关键字组合是 try 和 ______。", "answer": "except", "points": 1},
            {"type": "fill_blank", "question": "机器学习中，将数据分为训练集和______以评估模型泛化能力。", "answer": "测试集", "points": 1},
            {"type": "short_answer", "question": "请简述提示词工程的三个核心要素。", "answer": "明确目标、提供上下文、迭代优化", "points": 2},
            {"type": "short_answer", "question": "请简述过拟合的含义及一种常见的解决方法。", "answer": "过拟合指模型在训练集上表现很好但在新数据上表现差。常见解决方法包括增加数据量、正则化、Dropout等。", "points": 2},
            {"type": "short_answer", "question": "请用一两句话解释什么是API，并举一个日常使用API的例子。", "answer": "API是应用程序编程接口，用于不同软件系统之间的通信。例如天气App通过调用气象局的API获取天气数据。", "points": 2},
        ]
        prompt = f"""为《Python程序设计》课程期末考试生成第一段（闭卷基础知识）题目。
主题：{', '.join(topics_cn)}，难度：{difficulty}
要求：15道题（8道单选、4道填空、3道简答），满分20分。单选题每题1-2分，填空题每题1分，简答题每题2分。
严格输出JSON数组：[{{"type":"single_choice","question":"题目","options":{{"A":"","B":"","C":"","D":""}},"answer":"","points":分值}}, {{"type":"fill_blank","question":"题目中有______需填写","answer":"答案","points":分值}}, {{"type":"short_answer","question":"题目","answer":"参考答案","points":分值}}]"""
        resp = self._call_llm(
            [{'role': 'system', 'content': '你是AI课程出题专家，严格按JSON数组格式输出，不要嵌套在其他对象中。'},
             {'role': 'user', 'content': prompt}],
            TaskType.GENERATE_KNOWLEDGE_QUIZ
        )
        return resp.data if resp.data else mock

    def generate_exam_phase1_quick(self, topics, difficulty='medium'):
        """快速生成第一段试卷（直接返回预设题目，不调用LLM，用于教师创建考试时秒级响应）"""
        topics_cn = [self.topics_cn.get(t, t) for t in topics[:3]]
        return [
            {"type": "single_choice", "question": "以下关于机器学习的说法，正确的是？",
             "options": {"A": "从数据中自动学习规律", "B": "基于规则推理", "C": "随机生成答案", "D": "查询数据库"}, "answer": "A", "points": 2},
            {"type": "single_choice", "question": "Python中用于科学计算的核心库是？",
             "options": {"A": "requests", "B": "numpy", "C": "flask", "D": "sqlite3"}, "answer": "B", "points": 2},
            {"type": "single_choice", "question": "以下哪种不属于监督学习算法？",
             "options": {"A": "线性回归", "B": "K-Means聚类", "C": "决策树", "D": "支持向量机"}, "answer": "B", "points": 1},
            {"type": "single_choice", "question": "GPT中的T代表什么？",
             "options": {"A": "Training", "B": "Transformer", "C": "Transfer", "D": "Tensor"}, "answer": "B", "points": 1},
            {"type": "single_choice", "question": "以下哪个是Python的合法变量名？",
             "options": {"A": "2name", "B": "my-var", "C": "_count", "D": "class"}, "answer": "C", "points": 1},
            {"type": "single_choice", "question": "神经网络中激活函数的主要作用是？",
             "options": {"A": "加速训练", "B": "引入非线性", "C": "防止过拟合", "D": "降低维度"}, "answer": "B", "points": 1},
            {"type": "single_choice", "question": "以下关于提示词工程的说法，错误的是？",
             "options": {"A": "提示词应该尽量具体明确", "B": "可以通过few-shot示例引导模型", "C": "提示词越长效果越好", "D": "可以为模型指定输出格式"}, "answer": "C", "points": 1},
            {"type": "single_choice", "question": "Python中 list.append() 和 list.extend() 的区别是？",
             "options": {"A": "没有区别", "B": "append添加单个元素，extend合并可迭代对象", "C": "append更快", "D": "extend只能添加列表"}, "answer": "B", "points": 1},
            {"type": "fill_blank", "question": "程序分析四要素：分解、______、抽象、算法设计。", "answer": "模式识别", "points": 1},
            {"type": "fill_blank", "question": "大语言模型生成回复的概率机制叫______采样。", "answer": "温度（Temperature）", "points": 1},
            {"type": "fill_blank", "question": "在Python中，用于异常处理的关键字组合是 try 和 ______。", "answer": "except", "points": 1},
            {"type": "fill_blank", "question": "机器学习中，将数据分为训练集和______以评估模型泛化能力。", "answer": "测试集", "points": 1},
            {"type": "short_answer", "question": "请简述提示词工程的三个核心要素。", "answer": "明确目标、提供上下文、迭代优化", "points": 2},
            {"type": "short_answer", "question": "请简述过拟合的含义及一种常见的解决方法。", "answer": "过拟合指模型在训练集上表现很好但在新数据上表现差。常见解决方法包括增加数据量、正则化、Dropout等。", "points": 2},
            {"type": "short_answer", "question": "请用一两句话解释什么是API，并举一个日常使用API的例子。", "answer": "API是应用程序编程接口，用于不同软件系统之间的通信。例如天气App通过调用气象局的API获取天气数据。", "points": 2},
        ]

    def generate_exam_phase2(self, topics, difficulty='medium'):
        """生成第二段：含bug的代码项目"""
        mock = {
            "title": "学生学习数据分析程序（含2处bug）",
            "code": "scores = [85, 92, 78, 65, 88, 95, 72, 60, 77, 83]\n\ndef calcuate_stats(data):  # Bug1: 拼写错误\n    avg = sum(data) / len(data)\n    return avg\n\nfor score in scores:\n    if score > 90:  # Bug2: 应为>=\n        grade = '优秀'\n    elif score > 80:  # 应为>=\n        grade = '良好'\n    elif score >= 60:\n        grade = '及格'\n    else:\n        grade = '不及格'\n    print(f'分数{score}: {grade}')\n\nprint(f'平均分: {calcuate_stats(scores)}')",
            "tasks": ["找出并修复代码中的2处bug", "添加统计最高分、最低分和标准差的功能", "使用小林同学辅助，为代码添加数据可视化功能（柱状图）"],
            "hints": "可以使用小林同学辅助思路，但不能让AI直接给出完整解决方案"
        }
        return mock

    def generate_exam_phase3(self, topics, difficulty='medium'):
        """生成第三段：项目构建需求"""
        mock = {
            "title": "AI协同构建数据分析应用",
            "description": "在40分钟内，充分利用小林同学的辅助，构建一个完整的Python数据分析应用程序。",
            "requirements": [
                "从文件或内置数据集读取数据（如CSV、sklearn内置数据集）",
                "进行至少3种统计分析（均值、方差、相关性等）",
                "生成至少2种数据可视化图表",
                "代码有清晰的中文注释",
                "程序能够完整运行，输出分析结论"
            ],
            "hint": "完全开放使用小林同学，可多次对话优化，重点考察你驾驭AI完成完整项目的能力。",
            "scoring": "功能完整性(20分) + 代码质量(15分) + AI协同创新性(15分)"
        }
        return mock

    def evaluate_exam_phase2(self, project_data, student_answer):
        """评估第二段答案"""
        mock = {"score": 20, "bug_fix": 8, "extension": 7, "ai_collab": 5,
                "feedback": "bug修复准确，功能拓展完整，AI辅助使用恰当。"}
        if not student_answer.strip():
            return {"score": 0, "feedback": "未作答"}
        score = min(25, max(5, len(student_answer) // 20 + 10))
        return {"score": score, "feedback": f"代码调试{'有效' if score > 18 else '基本完成'}，功能拓展{'良好' if score > 20 else '有待加强'}。"}

    def evaluate_exam_phase3(self, req_data, code, prompt_log):
        """评估第三段项目"""
        if not code.strip():
            return {"score": 0, "feedback": "未提交代码"}
        score = min(25, max(5, len(code) // 30 + len(prompt_log) // 50 + 8))
        return {"score": score, "feedback": f"项目{'功能完整，展现了优秀的AI协同能力' if score > 20 else '基本完成，建议进一步优化'}。"}

    def generate_exam_evaluation(self, eval_data, topics):
        """生成三段式考试综合评价"""
        p1, p2, p3 = eval_data.get('p1', 0), eval_data.get('p2', 0), eval_data.get('p3', 0)
        total = p1 + p2 + p3
        grade = '优秀' if total >= 90 else '良好' if total >= 80 else '中等' if total >= 70 else '及格' if total >= 60 else '不及格'
        return {
            "evaluation": f"三段式期末考试综合得分{total:.1f}分，等级：{grade}。",
            "phase_analysis": [
                f"第一段（闭卷基础）{p1:.1f}/20：{'基础知识扎实' if p1 >= 16 else '基础知识需要加强'}",
                f"第二段（有限AI）{p2:.1f}/30：{'项目调试和拓展能力较强' if p2 >= 24 else 'AI辅助思考能力有待提升'}",
                f"第三段（完全AI协同）{p3:.1f}/50：{'AI驾驭能力出色' if p3 >= 40 else '需加强AI协同项目实践'}"
            ],
            "suggestions": ["继续保持AI工具的熟练应用" if total >= 80 else "建议多做编程实践练习，提升人机协作能力"]
        }

    def _looks_like_direct_answer_request(self, message: str) -> bool:
        """检测学生是否在考试第二段直接索要答案/完整代码"""
        if not message:
            return False
        msg = str(message).lower()
        patterns = [
            r'直接.*答案', r'告诉我.*答案', r'给我.*答案', r'标准答案', r'正确答案',
            r'完整代码', r'全部代码', r'修好.*代码', r'改好.*代码', r'直接.*代码',
            r'把.*bug.*找出来', r'全部bug', r'最终版', r'直接帮我写', r'你帮我做',
            r'给我现成', r'直接改', r'替我完成', r'答案发我', r'整段代码',
        ]
        return any(re.search(p, msg) for p in patterns)

    def _detect_exam_phase2_intent(self, message: str) -> str:
        """粗略识别第二段提问意图，便于生成更像专用助教的提示"""
        msg = str(message or '')
        if re.search(r'拓展|扩展|新增|添加|柱状图|可视化|功能', msg):
            return 'extension'
        if re.search(r'为什么|原因|影响|会报错|报错|异常|错在', msg):
            return 'bug_reason'
        if re.search(r'哪一行|哪里错|bug|定位|检查|排查|修复', msg):
            return 'bug_locate'
        if re.search(r'这段代码|什么意思|逻辑|作用|看不懂|读不懂|流程', msg):
            return 'code_reading'
        if re.search(r'测试|验证|运行|输出|结果', msg):
            return 'test_verify'
        return 'generic'

    def _build_exam_phase2_fallback(self, message: str, direct_request: bool = False) -> str:
        """第二段的本地兜底回复，确保风格始终像项目阅读/改错/功能拓展专用助教"""
        intent = self._detect_exam_phase2_intent(message)
        if direct_request:
            return (
                '小林同学 📚 这部分考试我不能直接给你完整答案或整段修正代码，但我可以陪你一步步把它找出来。'
                '先从最可疑的一处下手：看看函数名是否前后一致、条件边界是否覆盖临界值。你最怀疑哪一行？'
            )
        mapping = {
            'bug_locate': '小林同学 📚 第二段先别急着整段重写，重点是定位问题。建议你按“函数定义与调用是否一致、条件判断是否覆盖边界、变量/缩进是否影响流程”这三步排查。你现在最怀疑哪一行？',
            'bug_reason': '小林同学 📚 这题不只要改对，还要说明为什么错。你可以先回答三个点：哪一行有问题、它会导致什么现象、改完后输出会怎样变化。你想先分析哪一处？',
            'extension': '小林同学 📚 功能拓展别一上来就写完整代码，先定“小目标”。先想清楚要新增什么输入、核心处理步骤是什么、最后如何展示结果。你准备先做统计拓展，还是先做可视化？',
            'code_reading': '小林同学 📚 读项目代码时，可以按“输入数据→核心处理→输出结果”三段来拆。你先告诉我这段程序最开始读了什么、最后打印了什么，我们再一起补中间逻辑。',
            'test_verify': '小林同学 📚 第二段改完后一定要验证。你可以先挑一个边界样例或最容易出错的输入，看看输出是否符合预期。你现在运行后看到的现象是什么？',
            'generic': '小林同学 📚 我会只围绕项目阅读、改错和功能拓展来帮你。你可以把问题说得更具体一点，比如“哪一行可能有bug”“这个条件为什么错”“柱状图功能该分几步实现”。'
        }
        return mapping.get(intent, mapping['generic'])

    def _build_exam_phase3_fallback(self, message: str, context: str = '') -> str:
        """第三段本地兜底回复，避免空响应时退化成前端默认文案"""
        msg = str(message or '')
        ctx = str(context or '')
        if re.search(r'完整代码|全部代码|直接写|帮我实现|帮我写', msg):
            return (
                '小林同学 🚀 第三段可以完整协助你。建议先把项目拆成 3 块：输入与数据准备、核心处理逻辑、结果展示与测试。'
                '你先告诉我你想要 Python 控制台程序，还是带图表输出的版本？'
            )
        if re.search(r'柱状图|折线图|可视化|图表', msg):
            return (
                '小林同学 🚀 这个需求适合按“准备数据 → 调用绘图库 → 设置标题和坐标轴 → 显示图表”来实现。'
                '如果你愿意，我可以先给你一个最小可运行的 matplotlib 柱状图框架。'
            )
        if re.search(r'报错|错误|异常|运行不了|不能运行', msg):
            return (
                '小林同学 🚀 第三段先别急着重写，先把报错信息和出错位置贴出来。通常按“缺少库/变量未定义/缩进或类型不匹配”这三个方向先查最快。'
                '你现在看到的第一条报错是什么？'
            )
        if re.search(r'读取|导入|csv|excel|文件', msg) or re.search(r'数据|统计|分析', ctx):
            return (
                '小林同学 🚀 这类项目一般先把流程定成：读取数据、清洗或统计、输出结果。你可以先写一个读取样例数据的最小版本，确认输入没问题后再补分析逻辑。'
                '要不要我先给你搭一个主程序框架？'
            )
        return (
            '小林同学 🚀 第三段现在是完全开放支持，我可以陪你把项目一步步搭起来。'
            '建议先确定三件事：程序要处理什么输入、核心功能分哪几步、结果怎么展示。你想先做整体框架，还是先写其中一个函数？'
        )

    def exam_ai_assist(self, message, context, restricted=False, history=None):
        """考试中的AI辅助（phase2限制，phase3完全开放）"""
        history = history or []

        if restricted:
            direct_request = self._looks_like_direct_answer_request(message)
            if direct_request:
                return self._build_exam_phase2_fallback(message, direct_request=True)

            system = """你是《Python程序设计》课程考试第二部分“项目阅读、改错与功能拓展”专用助教，你的名字叫“小林同学”。

【你的唯一职责】
你只能围绕以下四类事情辅导学生：
1. 帮学生读懂项目代码与执行流程
2. 引导学生定位bug，并分析bug产生原因与影响
3. 引导学生设计功能拓展的实现步骤
4. 提醒学生如何验证修改是否正确

【你绝对不能做的事】
- 不能直接给出完整答案
- 不能直接列出全部bug及其标准修复结果
- 不能直接给出完整修正后代码或完整功能拓展代码
- 不能把“应该改成什么”一次性全部说完

【你可以提供的帮助】
- 指出“应该重点检查哪一类问题”，例如函数名、条件边界、缩进、索引、变量类型、返回值
- 给出1到2步排查顺序
- 给出伪代码、局部思路或验证方法
- 当学生问“为什么错”时，重点解释错误原因与程序现象，而不是直接给最终答案

【回复风格】
- 你不是通用AI助手，而是“考试第二部分项目教练”
- 多用“先看这段代码”“先检查这一类问题”“这一步建议你先验证什么”这类说法
- 每次回复只解决当前一步，不泛泛谈AI知识
- 回复必须简洁，3到5句话，180字以内
- 回复必须以“小林同学”开头
- 结尾用1到2个具体问题推进学生继续思考
- 禁止输出任何思考过程、分析草稿或<think>标签
"""
            messages = [
                {'role': 'system', 'content': system},
                {'role': 'user', 'content': f"[当前考试第二部分题目]\n{context}"},
                {'role': 'assistant', 'content': '小林同学 📚 我已经看到了这道第二部分项目题，我会只围绕读代码、找bug、解释原因和功能拓展来帮助你。'},
            ]
            for h in history[-6:]:
                role = 'assistant' if h.get('role') == 'tutor' else 'user'
                messages.append({'role': role, 'content': h.get('content', '')})
            messages.append({'role': 'user', 'content': f'[学生当前问题]\n{message}'})
            response = self._call_llm_text(messages, TaskType.SOCRATIC_TUTOR, temperature=0.4, max_tokens=320)
            response = self._strip_thinking(response)
            if (not response.strip()) or ('暂时无法回答' in response):
                return self._build_exam_phase2_fallback(message)
            return response

        system = """你是考试第三部分“完整项目构建”阶段的AI助手小林同学。当前为完全支持模式：
- 可以提供完整代码、详细解释和实现方案
- 优先按“需求拆解→代码实现→测试优化”的顺序协助
- 回复尽量结合当前项目要求，不要泛泛而谈
- 禁止输出任何思考过程、分析草稿或<think>标签
"""
        messages = [
            {'role': 'system', 'content': system},
            {'role': 'user', 'content': f"[当前考试第三部分要求]\n{context}"},
        ]
        for h in history[-6:]:
            role = 'assistant' if h.get('role') == 'tutor' else 'user'
            messages.append({'role': role, 'content': h.get('content', '')})
        messages.append({'role': 'user', 'content': f'[学生当前问题]\n{message}'})
        response = self._call_llm_text(messages, TaskType.SOCRATIC_TUTOR, temperature=0.6, max_tokens=4000)
        response = self._strip_thinking(response)
        if (not response.strip()) or ('暂时无法回答' in response):
            return self._build_exam_phase3_fallback(message, context)
        return response


    # ==================== 作业答辩 ====================

    def generate_defense_questions(self, submission_data, student_profile=None):
        """根据学生提交内容和学生画像，生成个性化答辩题目"""
        topics_desc = ', '.join([self.topics_cn.get(t, t) for t in submission_data.get('topics', [])])
        profile_hint = ''
        if student_profile:
            profile_hint = f"""
【学生画像参考】
- 薄弱点：{student_profile.get('weak_topics', '暂无数据')}
- AI依赖倾向：{student_profile.get('ai_dependency', '正常')}
- 综合水平：{student_profile.get('level', '中等')}
（请重点针对薄弱点和代码理解程度出题，检验学生是否真正理解了AI协助完成的内容）"""

        prompt = f"""你是"小林同学"，请根据学生的作业提交内容，生成3道个性化答辩题目，用于当堂闭卷答辩，检验学生对作业的真实理解程度（而非考察死记硬背）。

【实验主题】{topics_desc}

【学生提交概况】
- 编程实践代码片段（前300字）：{str(submission_data.get('ai_collab_final_code', ''))[:300]}
- AI协同反思：{str(submission_data.get('ai_collab_reflection', ''))[:200]}
- 程序分析作答摘要：{str(submission_data.get('thinking_student_answer', ''))[:200]}
{profile_hint}

【出题要求】
1. 第1题：针对学生提交的代码/作业，询问某段逻辑的实现原理（考察真实理解）
2. 第2题：提出一个改进或扩展需求，考察学生能否灵活应用（考察迁移能力）
3. 第3题：结合本专业场景，问一个开放性的AI协同思考题（考察AI协同意识）

【输出JSON格式】
{{
    "questions": [
        {{
            "id": 1,
            "type": "理解验证",
            "question": "题目内容",
            "hint": "评分要点（仅教师可见）",
            "score": 40
        }},
        {{
            "id": 2,
            "type": "迁移应用",
            "question": "题目内容",
            "hint": "评分要点（仅教师可见）",
            "score": 35
        }},
        {{
            "id": 3,
            "type": "AI协同思考",
            "question": "题目内容",
            "hint": "评分要点（仅教师可见）",
            "score": 25
        }}
    ],
    "defense_focus": "本次答辩重点考察方向（一句话）"
}}"""

        messages = [
            {'role': 'system', 'content': '你是"小林同学"，根据学生作业生成个性化答辩题。直接输出JSON，不要解释。'},
            {'role': 'user', 'content': prompt}
        ]
        response = self._call_llm(messages, TaskType.GENERATE_DEFENSE_QUESTIONS)
        if response.data:
            response.data['_is_mock'] = response.is_mock
            return response.data

        # Fallback
        return {
            'questions': [
                {'id': 1, 'type': '理解验证', 'score': 40,
                 'question': f'请解释你在{topics_desc}实验中，编程实践部分的核心代码逻辑，并说明每个关键步骤的作用。',
                 'hint': '考察学生是否真正理解代码，而非仅仅获取答案'},
                {'id': 2, 'type': '迁移应用', 'score': 35,
                 'question': f'如果要将你的{topics_desc}实验结果应用到一个新的数据集上，你会修改哪些部分？为什么？',
                 'hint': '考察学生能否灵活调整代码，体现对逻辑的掌握'},
                {'id': 3, 'type': 'AI协同思考', 'score': 25,
                 'question': '在本次实验中，你认为AI协同帮助你最大的地方是什么？如果没有AI，你会如何完成这个任务？',
                 'hint': '考察学生对AI协同价值的理解与反思'},
            ],
            'defense_focus': f'重点考察学生对{topics_desc}实验中AI协同完成内容的真实理解程度',
            '_is_mock': True
        }

    def evaluate_defense(self, questions, answers, submission_data):
        """评估答辩回答，生成答辩总结与学习建议"""
        qa_text = ''
        total_possible = 0
        for q in questions:
            qid = q['id']
            ans = answers.get(str(qid), answers.get(qid, '（未作答）'))
            qa_text += f"\n【第{qid}题 {q['type']}（{q['score']}分）】\n题目：{q['question']}\n学生回答：{ans}\n评分要点：{q.get('hint', '')}\n"
            total_possible += q.get('score', 0)

        prompt = f"""你是"小林同学"，请评估以下答辩表现，生成答辩总结和学习建议。

{qa_text}

【评分说明】总分{total_possible}分，请根据回答质量给出各题得分（满分参考题目括号内分值）。
评分标准：90%+优秀，70-90%良好，50-70%中等，30-50%需加强，<30%待努力

【输出JSON】
{{
    "question_scores": [{{"id": 1, "score": 实得分, "comment": "简评"}}, ...],
    "total_score": 总分（满分{total_possible}分制换算为100分制）,
    "defense_grade": "优秀/良好/中等/需加强/待努力",
    "understanding_level": "对作业的真实理解评估（1-2句话）",
    "ai_collab_quality": "AI协同能力评估（1-2句话）",
    "strengths": ["表现好的方面1", "表现好的方面2"],
    "improvement": ["需要改进的地方1", "需要改进的地方2"],
    "suggestions": ["具体学习建议1", "具体学习建议2", "具体学习建议3"],
    "summary": "200字以内的答辩总结评语（客观、鼓励性）"
}}"""

        messages = [
            {'role': 'system', 'content': '你是"小林同学"，公平客观地评估答辩表现。直接输出JSON。'},
            {'role': 'user', 'content': prompt}
        ]
        response = self._call_llm(messages, TaskType.EVALUATE_DEFENSE)
        if response.data:
            response.data['_is_mock'] = response.is_mock
            return response.data

        # Fallback scoring
        answered = sum(1 for q in questions if answers.get(str(q['id']), answers.get(q['id'], '')).strip())
        ratio = answered / max(len(questions), 1)
        score = round(ratio * 70 + 10, 1)
        grade = '良好' if score >= 80 else '中等' if score >= 60 else '需加强'
        return {
            'question_scores': [{'id': q['id'], 'score': round(q['score'] * ratio, 1), 'comment': '已作答'} for q in questions],
            'total_score': score, 'defense_grade': grade,
            'understanding_level': '答辩服务暂时不可用，请教师手动评分。',
            'ai_collab_quality': '请教师结合实际情况复核。',
            'strengths': ['完成了答辩作答'], 'improvement': ['需要更深入地理解代码逻辑'],
            'suggestions': ['建议重新梳理实验中每段代码的作用', '多尝试在无AI辅助情况下解释自己的代码'],
            'summary': f'答辩已完成，共回答{answered}/{len(questions)}题。评分服务暂时不可用，以上为初步评估，请教师进行人工复核。',
            '_is_mock': True
        }


# 全局实例
llm_service = LLMService()
