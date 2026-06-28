"""
配置文件 - 《Python程序设计》课程实验系统
"""
import os
import secrets
from datetime import timedelta


class Config:
    """基础配置类"""
    SECRET_KEY = os.environ.get('SECRET_KEY') or secrets.token_hex(32)
    PERMANENT_SESSION_LIFETIME = timedelta(hours=4)
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'

    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or 'sqlite:///python_lab.db'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ENGINE_OPTIONS = {
        'pool_pre_ping': True,
        'pool_recycle': 300,
        'pool_size': 20,
        'max_overflow': 30,
        'pool_timeout': 120,
        'connect_args': {'timeout': 30},
    }

    # ==================== LLM服务配置 ====================
    # 默认指向校内昇腾算力节点（团队教育厅在研项目「全栈国产化教育算力平台」提供）的
    # 通用 Chat Completions 兼容推理端点（接口格式兼容，实际接入国产大模型服务）；地址通过环境变量 LLM_API_URL 注入，请勿在代码中硬编码密钥。
    LLM_API_URL = os.environ.get('LLM_API_URL') or 'http://ascend-llm.local:1234/v1/chat/completions'
    LLM_API_URL_BACKUP = os.environ.get('LLM_API_URL_BACKUP') or None
    LLM_API_KEY = os.environ.get('LLM_API_KEY') or 'lm-studio'
    LLM_MODEL = os.environ.get('LLM_MODEL') or 'MiniMax-M2.5'
    LLM_RETRY_COUNT = int(os.environ.get('LLM_RETRY_COUNT', 3))
    LLM_RETRY_DELAY = int(os.environ.get('LLM_RETRY_DELAY', 2))
    LLM_TIMEOUT = int(os.environ.get('LLM_TIMEOUT', 300))
    LLM_TIMEOUT_LONG = int(os.environ.get('LLM_TIMEOUT_LONG', 300))
    LLM_TIMEOUT_SHORT = int(os.environ.get('LLM_TIMEOUT_SHORT', 120))
    LLM_ENABLE_FEW_SHOT = os.environ.get('LLM_ENABLE_FEW_SHOT', 'true').lower() == 'true'
    LLM_DEFAULT_TEMPERATURE = float(os.environ.get('LLM_DEFAULT_TEMPERATURE', 0.7))
    LLM_DEFAULT_MAX_TOKENS = int(os.environ.get('LLM_DEFAULT_MAX_TOKENS', 8000))

    LLM_TIMEOUT_CONFIG = {
        'generate_knowledge_quiz': 120,
        'generate_ai_collab_task': 180,
        'generate_thinking_task': 150,
        'generate_ethics_case': 150,
        'evaluate_quiz': 60,
        'evaluate_ai_collab': 90,
        'evaluate_thinking': 90,
        'evaluate_ethics': 90,
        'generate_evaluation': 150,
        'generate_profile': 180,
    }

    # ==================== 代码运行沙箱配置 ====================
    CODE_RUNNER_ENABLED = True
    CODE_RUNNER_TIMEOUT = 10
    CODE_RUNNER_MEMORY_LIMIT = 50
    CODE_RUNNER_MAX_OUTPUT = 10000
    CODE_RUNNER_FORBIDDEN_MODULES = [
        'os', 'sys', 'subprocess', 'shutil', 'socket', 'requests',
        'urllib', 'http', 'ftplib', 'smtplib', 'telnetlib',
        'pickle', 'shelve', 'marshal', 'importlib',
        'ctypes', 'multiprocessing', 'threading',
    ]
    CODE_RUNNER_ALLOWED_BUILTINS = [
        'abs', 'all', 'any', 'bin', 'bool', 'chr', 'dict', 'dir',
        'divmod', 'enumerate', 'filter', 'float', 'format', 'frozenset',
        'hash', 'hex', 'int', 'isinstance', 'issubclass', 'iter',
        'len', 'list', 'map', 'max', 'min', 'next', 'oct', 'ord',
        'pow', 'print', 'range', 'repr', 'reversed', 'round', 'set',
        'slice', 'sorted', 'str', 'sum', 'tuple', 'type', 'zip', 'input',
    ]

    # ==================== 密码安全配置 ====================
    PASSWORD_MIN_LENGTH = 6
    PASSWORD_REQUIRE_UPPERCASE = False
    PASSWORD_REQUIRE_LOWERCASE = True
    PASSWORD_REQUIRE_DIGIT = True
    PASSWORD_REQUIRE_SPECIAL = False

    # ==================== 学习主题定义（Python程序设计课程） ====================
    LEARNING_TOPICS = {
        # ---- 第1章 Python概述 ----
        'python_overview': 'Python语言概述与开发环境',
        'python_io': 'Python基本输入输出',
        'variables_basics': '变量与代码规范',
        # ---- 第2章 数据类型与运算符 ----
        'number_types': '数字类型与类型转换',
        'string_ops': '字符串操作与格式化输出',
        'operators': '运算符与表达式',
        'list_type': '列表的定义与常用操作',
        'tuple_type': '元组的定义与常用操作',
        'dict_type': '字典的定义与常用操作',
        'set_type': '集合的定义与常用操作',
        # ---- 第3章 基本语句 ----
        'if_statement': 'if条件语句与嵌套',
        'for_loop': 'for循环语句',
        'while_loop': 'while循环语句',
        'loop_control': 'break与continue控制语句',
        'exception_handling': '异常处理与try语句',
        'assert_raise': 'assert与raise语句',
        # ---- 第4章 函数 ----
        'function_def': '函数的定义与调用',
        'function_params': '函数参数传递方式',
        'variable_scope': '局部变量与全局变量',
        'lambda_recursive': '匿名函数与递归函数',
        'builtin_functions': '常用内置函数',
        # ---- 第5章 模块应用 ----
        'module_import': '模块概念与导入方式',
        'standard_modules': '标准模块应用',
        'custom_modules': '自定义模块与包',
        'third_party': '第三方模块安装与使用',
        # ---- 第6章 图形用户界面编程 ----
        'gui_basics': 'GUI编程基础与tkinter',
        'tkinter_widgets': 'tkinter基本组件',
        'layout_managers': '几何布局管理器',
        'event_handling': '事件处理与对话框',
        # ---- 第7章 文件处理 ----
        'file_open_close': '文件打开与关闭',
        'file_read': '文件读取方法',
        'file_write': '文件写入方法',
        'file_operations': '文件与文件夹操作',
        'file_path': '文件路径操作',
        # ---- 第8章 类和对象 ----
        'oop_concepts': '面向对象基本概念',
        'class_def': '类的定义与对象创建',
        'constructor_destructor': '构造方法与析构方法',
        'class_static_methods': '类方法与静态方法',
        'inheritance_polymorphism': '继承与多态',
    }

    # 专业大类（非计算机专业学生）
    MAJOR_CATEGORIES = {
        'engineering': '工科类',
        'forestry': '林科类',
        'business': '经管类',
        'design': '设计类',
        'science': '理科类',
        'general': '通用',
    }

    # 难度级别
    DIFFICULTY_LEVELS = {
        'easy': '基础',
        'medium': '进阶',
        'hard': '挑战',
    }

    # ==================== 错误通知配置 ====================
    SHOW_LLM_FALLBACK_NOTICE = True
    LLM_FALLBACK_MESSAGES = {
        'timeout': '由于"小林同学"响应超时，当前显示的是示例数据。',
        'connection': '由于无法连接"小林同学"服务，当前显示的是示例数据。',
        'parse_error': '由于AI响应格式异常，当前显示的是示例数据。',
        'default': '由于"小林同学"暂时不可用，当前显示的是示例数据。',
    }

    # ==================== 削峰填谷配置 ====================
    POOL_ENABLED = os.environ.get('POOL_ENABLED', 'true').lower() == 'true'
    POOL_DEFAULT_SIZE = int(os.environ.get('POOL_DEFAULT_SIZE', 60))
    POOL_REFILL_THRESHOLD = float(os.environ.get('POOL_REFILL_THRESHOLD', 0.1))
    POOL_REFILL_COUNT = int(os.environ.get('POOL_REFILL_COUNT', 10))
    POOL_PREFILL_HOUR = int(os.environ.get('POOL_PREFILL_HOUR', 2))

    ASYNC_EVAL_ENABLED = os.environ.get('ASYNC_EVAL_ENABLED', 'true').lower() == 'true'

    # 智能答辩：是否在提交完成时自动生成答辩题（默认关闭，避免同步调用LLM增加提交延迟；
    # 可改用教师端「批量补生成」接口 /api/teacher/defense/batch_generate）
    DEFENSE_AUTO_GENERATE = os.environ.get('DEFENSE_AUTO_GENERATE', 'false').lower() == 'true'
    # 答辩分计入综合评价的权重（非破坏性，仅用于教师复核界面展示综合分与落差预警）
    DEFENSE_COMPOSITE_WEIGHT = float(os.environ.get('DEFENSE_COMPOSITE_WEIGHT', '0.3'))
    ASYNC_WORKER_POLL_INTERVAL = int(os.environ.get('ASYNC_WORKER_POLL_INTERVAL', 3))
    ASYNC_WORKER_RATE_LIMIT = int(os.environ.get('ASYNC_WORKER_RATE_LIMIT', 2))

    REALTIME_QUEUE_ENABLED = os.environ.get('REALTIME_QUEUE_ENABLED', 'true').lower() == 'true'
    REALTIME_MAX_CONCURRENT = int(os.environ.get('REALTIME_MAX_CONCURRENT', 10))
    REALTIME_MAX_PER_STUDENT = int(os.environ.get('REALTIME_MAX_PER_STUDENT', 1))


class DevelopmentConfig(Config):
    DEBUG = True
    SESSION_COOKIE_SECURE = False
    PASSWORD_MIN_LENGTH = 4
    PASSWORD_REQUIRE_UPPERCASE = False


class ProductionConfig(Config):
    DEBUG = False
    SESSION_COOKIE_SECURE = True


class TestingConfig(Config):
    TESTING = True
    SQLALCHEMY_DATABASE_URI = 'sqlite:///:memory:'
    CODE_RUNNER_ENABLED = False


config_map = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig,
}


def get_config():
    env = os.environ.get('FLASK_ENV', 'development')
    return config_map.get(env, DevelopmentConfig)


def get_timeout_for_task(task_type: str) -> int:
    config = get_config()
    if hasattr(config, 'LLM_TIMEOUT_CONFIG') and task_type in config.LLM_TIMEOUT_CONFIG:
        return config.LLM_TIMEOUT_CONFIG[task_type]
    if task_type.startswith('evaluate'):
        return getattr(config, 'LLM_TIMEOUT_SHORT', 60)
    return getattr(config, 'LLM_TIMEOUT_LONG', 180)
