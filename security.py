"""
安全模块 - Python程序设计课程实验系统
包含：密码策略、CSRF保护、输入验证、安全辅助函数
"""
import re
import secrets
import string
import hashlib
import hmac
from datetime import datetime, timedelta
from functools import wraps
from flask import request, session, abort, jsonify, current_app, g
import logging

# 配置日志
logger = logging.getLogger(__name__)


# ==================== 密码安全 ====================

class PasswordValidator:
    """密码强度验证器"""
    
    def __init__(self, app=None):
        self.app = app
        if app is not None:
            self.init_app(app)
    
    def init_app(self, app):
        """初始化配置"""
        self.app = app
    
    def get_config(self, key, default=None):
        """获取配置项"""
        if self.app:
            return self.app.config.get(key, default)
        return default
    
    def validate(self, password):
        """
        验证密码强度
        返回: (is_valid, error_message)
        """
        errors = []
        
        # 最小长度检查
        min_length = self.get_config('PASSWORD_MIN_LENGTH', 8)
        if len(password) < min_length:
            errors.append(f'密码长度至少{min_length}位')
        
        # 最大长度检查（防止DoS攻击）
        if len(password) > 128:
            errors.append('密码长度不能超过128位')
        
        # 大写字母检查
        if self.get_config('PASSWORD_REQUIRE_UPPERCASE', True):
            if not any(c.isupper() for c in password):
                errors.append('密码需要包含至少一个大写字母')
        
        # 小写字母检查
        if self.get_config('PASSWORD_REQUIRE_LOWERCASE', True):
            if not any(c.islower() for c in password):
                errors.append('密码需要包含至少一个小写字母')
        
        # 数字检查
        if self.get_config('PASSWORD_REQUIRE_DIGIT', True):
            if not any(c.isdigit() for c in password):
                errors.append('密码需要包含至少一个数字')
        
        # 特殊字符检查
        if self.get_config('PASSWORD_REQUIRE_SPECIAL', False):
            special_chars = self.get_config('PASSWORD_SPECIAL_CHARS', '!@#$%^&*()_+-=[]{}|;:,.<>?')
            if not any(c in special_chars for c in password):
                errors.append(f'密码需要包含至少一个特殊字符')
        
        # 常见弱密码检查
        weak_passwords = [
            'password', '123456', '12345678', 'qwerty', 'abc123',
            'password123', 'admin', 'letmein', 'welcome', 'monkey',
            'student', 'teacher', 'python', 'test123'
        ]
        if password.lower() in weak_passwords:
            errors.append('密码过于简单，请使用更复杂的密码')
        
        if errors:
            return False, errors
        return True, []
    
    def get_strength(self, password):
        """
        计算密码强度评分 (0-100)
        """
        score = 0
        
        # 长度得分 (最高30分)
        length = len(password)
        if length >= 8:
            score += 10
        if length >= 12:
            score += 10
        if length >= 16:
            score += 10
        
        # 字符多样性得分 (最高40分)
        if any(c.islower() for c in password):
            score += 10
        if any(c.isupper() for c in password):
            score += 10
        if any(c.isdigit() for c in password):
            score += 10
        if any(c in '!@#$%^&*()_+-=[]{}|;:,.<>?' for c in password):
            score += 10
        
        # 唯一字符比例得分 (最高20分)
        unique_ratio = len(set(password)) / len(password) if password else 0
        score += int(unique_ratio * 20)
        
        # 连续字符惩罚
        for i in range(len(password) - 2):
            if password[i:i+3].isdigit():
                # 检查连续数字如123, 321
                nums = [int(c) for c in password[i:i+3]]
                if nums[1] - nums[0] == nums[2] - nums[1] and abs(nums[1] - nums[0]) == 1:
                    score -= 5
        
        return max(0, min(100, score))
    
    def get_strength_label(self, password):
        """获取密码强度标签"""
        score = self.get_strength(password)
        if score >= 80:
            return '强', 'success'
        elif score >= 60:
            return '中等', 'warning'
        elif score >= 40:
            return '弱', 'danger'
        else:
            return '很弱', 'danger'


def generate_secure_password(length=12):
    """
    生成安全的随机密码
    """
    # 确保包含各类字符
    lowercase = secrets.choice(string.ascii_lowercase)
    uppercase = secrets.choice(string.ascii_uppercase)
    digit = secrets.choice(string.digits)
    special = secrets.choice('!@#$%^&*')
    
    # 填充剩余长度
    remaining_length = length - 4
    all_chars = string.ascii_letters + string.digits + '!@#$%^&*'
    remaining = ''.join(secrets.choice(all_chars) for _ in range(remaining_length))
    
    # 打乱顺序
    password_list = list(lowercase + uppercase + digit + special + remaining)
    secrets.SystemRandom().shuffle(password_list)
    
    return ''.join(password_list)


# ==================== CSRF保护 ====================

class CSRFProtect:
    """CSRF保护类"""
    
    TOKEN_LENGTH = 32
    TOKEN_KEY = '_csrf_token'
    HEADER_NAME = 'X-CSRF-Token'
    
    def __init__(self, app=None):
        self.app = app
        self._exempt_views = set()
        self._exempt_blueprints = set()
        
        if app is not None:
            self.init_app(app)
    
    def init_app(self, app):
        """初始化Flask应用"""
        self.app = app
        
        # 注册before_request钩子
        app.before_request(self._check_csrf)
        
        # 注册模板全局函数
        app.jinja_env.globals['csrf_token'] = self.generate_token
        
        # 配置
        app.config.setdefault('WTF_CSRF_ENABLED', True)
        app.config.setdefault('WTF_CSRF_CHECK_DEFAULT', True)
        app.config.setdefault('WTF_CSRF_TIME_LIMIT', 3600)  # 1小时过期
        
        logger.info("CSRF保护已初始化")
    
    def generate_token(self):
        """生成CSRF令牌"""
        if self.TOKEN_KEY not in session:
            session[self.TOKEN_KEY] = secrets.token_hex(self.TOKEN_LENGTH)
        return session[self.TOKEN_KEY]
    
    def validate_token(self, token):
        """验证CSRF令牌"""
        if not token:
            return False
        
        session_token = session.get(self.TOKEN_KEY)
        if not session_token:
            return False
        
        # 使用常量时间比较防止时序攻击
        return hmac.compare_digest(token, session_token)
    
    def _check_csrf(self):
        """检查CSRF令牌（before_request钩子）"""
        # 检查是否启用CSRF
        if not current_app.config.get('WTF_CSRF_ENABLED', True):
            return
        
        # 安全方法不需要检查
        if request.method in ('GET', 'HEAD', 'OPTIONS', 'TRACE'):
            return
        
        # 检查是否豁免
        if self._is_exempt():
            return
        
        # 获取令牌
        token = self._get_token()
        
        # 验证令牌
        if not self.validate_token(token):
            logger.warning(f"CSRF验证失败: {request.path} from {request.remote_addr}")
            self._handle_csrf_error()
    
    def _get_token(self):
        """从请求中获取CSRF令牌"""
        # 优先从header获取（用于AJAX请求）
        token = request.headers.get(self.HEADER_NAME)
        if token:
            return token
        
        # 从表单数据获取
        token = request.form.get('csrf_token')
        if token:
            return token
        
        # 从JSON数据获取
        if request.is_json:
            data = request.get_json(silent=True) or {}
            token = data.get('csrf_token')
            if token:
                return token
        
        return None
    
    def _is_exempt(self):
        """检查当前请求是否豁免CSRF检查"""
        # 检查视图函数
        view = current_app.view_functions.get(request.endpoint)
        if view:
            if hasattr(view, '_csrf_exempt') and view._csrf_exempt:
                return True
        
        # 检查蓝图
        if request.blueprint and request.blueprint in self._exempt_blueprints:
            return True
        
        return False
    
    def _handle_csrf_error(self):
        """处理CSRF错误"""
        if request.is_json or request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            abort(jsonify({
                'success': False,
                'error': 'CSRF令牌无效或已过期，请刷新页面后重试',
                'code': 'CSRF_ERROR'
            }), 403)
        else:
            abort(403, description='CSRF令牌验证失败，请刷新页面后重试')
    
    def exempt(self, view):
        """装饰器：豁免特定视图的CSRF检查"""
        view._csrf_exempt = True
        return view
    
    def exempt_blueprint(self, blueprint_name):
        """豁免整个蓝图的CSRF检查"""
        self._exempt_blueprints.add(blueprint_name)


# 全局CSRF实例
csrf = CSRFProtect()


def csrf_exempt(view):
    """装饰器：豁免CSRF检查"""
    view._csrf_exempt = True
    return view


# ==================== 输入验证 ====================

class InputValidator:
    """输入验证工具类"""
    
    @staticmethod
    def sanitize_string(value, max_length=None, strip=True):
        """
        清理字符串输入
        """
        if value is None:
            return None
        
        value = str(value)
        
        if strip:
            value = value.strip()
        
        if max_length and len(value) > max_length:
            value = value[:max_length]
        
        return value
    
    @staticmethod
    def validate_username(username):
        """
        验证用户名
        返回: (is_valid, error_message)
        """
        if not username:
            return False, '用户名不能为空'
        
        if len(username) < 3:
            return False, '用户名长度至少3位'
        
        if len(username) > 32:
            return False, '用户名长度不能超过32位'
        
        # 只允许字母、数字、下划线
        if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', username):
            return False, '用户名只能包含字母、数字和下划线，且必须以字母开头'
        
        return True, None
    
    @staticmethod
    def validate_student_id(student_id):
        """
        验证学号
        返回: (is_valid, error_message)
        """
        if not student_id:
            return True, None  # 学号可选
        
        # 学号格式：8-12位数字
        if not re.match(r'^\d{8,12}$', student_id):
            return False, '学号格式不正确，应为8-12位数字'
        
        return True, None
    
    @staticmethod
    def validate_name(name):
        """
        验证姓名
        返回: (is_valid, error_message)
        """
        if not name:
            return False, '姓名不能为空'
        
        if len(name) < 2:
            return False, '姓名长度至少2位'
        
        if len(name) > 32:
            return False, '姓名长度不能超过32位'
        
        # 允许中文、字母、空格
        if not re.match(r'^[\u4e00-\u9fa5a-zA-Z\s]+$', name):
            return False, '姓名只能包含中文、字母和空格'
        
        return True, None
    
    @staticmethod
    def validate_class_name(class_name):
        """
        验证班级名称
        返回: (is_valid, error_message)
        """
        if not class_name:
            return True, None  # 班级可选
        
        if len(class_name) > 64:
            return False, '班级名称不能超过64位'
        
        return True, None


# ==================== 登录保护 ====================

class LoginRateLimiter:
    """登录频率限制器（防止暴力破解）"""
    
    def __init__(self, max_attempts=5, lockout_time=300):
        """
        max_attempts: 最大尝试次数
        lockout_time: 锁定时间（秒）
        """
        self.max_attempts = max_attempts
        self.lockout_time = lockout_time
        self._attempts = {}  # {ip: [(timestamp, username), ...]}
        self._lockouts = {}  # {ip: lockout_until}
    
    def is_locked(self, ip):
        """检查IP是否被锁定"""
        if ip in self._lockouts:
            if datetime.now() < self._lockouts[ip]:
                return True
            else:
                # 锁定已过期，清除
                del self._lockouts[ip]
                if ip in self._attempts:
                    del self._attempts[ip]
        return False
    
    def get_lockout_remaining(self, ip):
        """获取剩余锁定时间（秒）"""
        if ip in self._lockouts:
            remaining = (self._lockouts[ip] - datetime.now()).total_seconds()
            return max(0, int(remaining))
        return 0
    
    def record_attempt(self, ip, username):
        """记录登录尝试"""
        now = datetime.now()
        
        # 清理旧记录（超过lockout_time的）
        if ip in self._attempts:
            cutoff = now - timedelta(seconds=self.lockout_time)
            self._attempts[ip] = [
                (ts, user) for ts, user in self._attempts[ip]
                if ts > cutoff
            ]
        else:
            self._attempts[ip] = []
        
        # 记录本次尝试
        self._attempts[ip].append((now, username))
        
        # 检查是否需要锁定
        if len(self._attempts[ip]) >= self.max_attempts:
            self._lockouts[ip] = now + timedelta(seconds=self.lockout_time)
            logger.warning(f"IP {ip} 因多次登录失败被锁定 {self.lockout_time}秒")
            return True
        
        return False
    
    def reset(self, ip):
        """重置IP的尝试记录（登录成功时调用）"""
        if ip in self._attempts:
            del self._attempts[ip]
        if ip in self._lockouts:
            del self._lockouts[ip]


# 全局登录限制器实例
login_limiter = LoginRateLimiter(max_attempts=5, lockout_time=300)


# ==================== 安全响应头 ====================

def add_security_headers(response):
    """
    添加安全响应头
    """
    # 防止点击劫持
    response.headers['X-Frame-Options'] = 'SAMEORIGIN'
    
    # 防止MIME类型嗅探
    response.headers['X-Content-Type-Options'] = 'nosniff'
    
    # XSS过滤器
    response.headers['X-XSS-Protection'] = '1; mode=block'
    
    # 引用策略
    response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
    
    # 内容安全策略（基础版，可根据需要调整）
    # response.headers['Content-Security-Policy'] = "default-src 'self'; script-src 'self' 'unsafe-inline' cdn.jsdelivr.net cdnjs.cloudflare.com; style-src 'self' 'unsafe-inline' cdn.jsdelivr.net cdnjs.cloudflare.com fonts.googleapis.com; font-src 'self' fonts.gstatic.com cdnjs.cloudflare.com;"
    
    return response


# ==================== 辅助函数 ====================

def get_client_ip():
    """获取客户端真实IP"""
    # 处理代理情况
    if request.headers.get('X-Forwarded-For'):
        ip = request.headers.get('X-Forwarded-For').split(',')[0].strip()
    elif request.headers.get('X-Real-IP'):
        ip = request.headers.get('X-Real-IP')
    else:
        ip = request.remote_addr
    return ip


def log_security_event(event_type, message, user_id=None, extra=None):
    """记录安全事件"""
    log_data = {
        'event': event_type,
        'message': message,
        'ip': get_client_ip(),
        'user_id': user_id,
        'timestamp': datetime.now().isoformat(),
        'user_agent': request.user_agent.string if request.user_agent else None,
    }
    if extra:
        log_data.update(extra)
    
    logger.info(f"安全事件: {log_data}")


# ==================== 初始化函数 ====================

def init_security(app):
    """
    初始化所有安全组件
    """
    # 初始化CSRF保护
    csrf.init_app(app)
    
    # 初始化密码验证器
    password_validator = PasswordValidator(app)
    app.password_validator = password_validator
    
    # 添加安全响应头
    @app.after_request
    def apply_security_headers(response):
        return add_security_headers(response)
    
    # 添加模板全局函数
    app.jinja_env.globals['generate_secure_password'] = generate_secure_password
    
    logger.info("安全模块初始化完成")
    
    return {
        'csrf': csrf,
        'password_validator': password_validator,
        'login_limiter': login_limiter
    }
