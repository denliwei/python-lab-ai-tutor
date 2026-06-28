"""
数据库模型 - 《Python程序设计》课程实验系统
"""
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime
import json

db = SQLAlchemy()


class User(db.Model):
    """用户模型"""
    __tablename__ = 'users'

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(256), nullable=False)
    name = db.Column(db.String(64), nullable=False)
    role = db.Column(db.String(20), nullable=False)  # teacher / student
    student_id = db.Column(db.String(32), unique=True, nullable=True)
    class_name = db.Column(db.String(64), nullable=True)
    major_category = db.Column(db.String(32), nullable=True)  # engineering/forestry/business/design
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    submissions = db.relationship('Submission', backref='student', lazy='dynamic',
                                  foreign_keys='Submission.student_id')
    experiments_created = db.relationship('Experiment', backref='teacher', lazy='dynamic')
    tutor_chats = db.relationship('TutorChat', backref='student_user', lazy='dynamic',
                                   foreign_keys='TutorChat.student_id')

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

    def to_dict(self):
        return {
            'id': self.id, 'username': self.username, 'name': self.name,
            'role': self.role, 'student_id': self.student_id,
            'class_name': self.class_name, 'major_category': self.major_category,
        }


class Experiment(db.Model):
    """实验/作业模型"""
    __tablename__ = 'experiments'

    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200), nullable=False)
    description = db.Column(db.Text)
    teacher_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    topics = db.Column(db.Text, nullable=False)          # JSON 学习主题列表
    difficulty = db.Column(db.String(20), default='medium')
    major_category = db.Column(db.String(32), default='general')  # 适用专业
    is_active = db.Column(db.Boolean, default=True)
    start_time = db.Column(db.DateTime)
    end_time = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    submissions = db.relationship('Submission', backref='experiment', lazy='dynamic')

    def get_topics(self):
        return json.loads(self.topics) if self.topics else []

    def set_topics(self, topics_list):
        self.topics = json.dumps(topics_list)

    def to_dict(self):
        return {
            'id': self.id, 'title': self.title, 'description': self.description,
            'topics': self.get_topics(), 'difficulty': self.difficulty,
            'major_category': self.major_category, 'is_active': self.is_active,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'teacher': self.teacher.name if self.teacher else None,
        }


class Submission(db.Model):
    """学生提交记录 — 四阶段实验（概念理解→编程实践→程序分析→综合应用）"""
    __tablename__ = 'submissions'

    id = db.Column(db.Integer, primary_key=True)
    experiment_id = db.Column(db.Integer, db.ForeignKey('experiments.id'), nullable=False)
    student_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)

    # ---- 阶段1：概念理解（满分25分）----
    quiz_questions = db.Column(db.Text)         # JSON: LLM生成的知识题
    quiz_student_answers = db.Column(db.Text)   # JSON: 学生答案
    quiz_score = db.Column(db.Float)
    quiz_feedback = db.Column(db.Text)

    # ---- 阶段2：编程实践（满分30分）----
    ai_collab_task = db.Column(db.Text)          # 任务描述
    ai_collab_prompt_history = db.Column(db.Text) # JSON: 学生与AI的提示词交互记录
    ai_collab_final_code = db.Column(db.Text)    # 最终代码
    ai_collab_reflection = db.Column(db.Text)    # 学生反思
    ai_collab_score = db.Column(db.Float)
    ai_collab_feedback = db.Column(db.Text)

    # ---- 阶段3：程序分析（满分25分）----
    thinking_task = db.Column(db.Text)           # 程序分析任务描述
    thinking_student_answer = db.Column(db.Text) # 学生作答(可含伪代码/流程图描述)
    thinking_score = db.Column(db.Float)
    thinking_feedback = db.Column(db.Text)

    # ---- 阶段4：综合应用（满分20分）----
    ethics_case = db.Column(db.Text)             # 伦理案例
    ethics_student_answer = db.Column(db.Text)   # 学生分析
    ethics_score = db.Column(db.Float)
    ethics_feedback = db.Column(db.Text)

    # ---- 总分和综合评价 ----
    total_score = db.Column(db.Float)
    final_evaluation = db.Column(db.Text)

    # 状态流: quiz(概念理解) -> ai_collab(编程实践) -> thinking(程序分析) -> ethics(综合应用) -> completed
    status = db.Column(db.String(20), default='quiz')
    submitted_at = db.Column(db.DateTime)
    completed_at = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    # ---- 作业答辩 ----
    # defense_status: none / generated / in_progress / submitted / reviewed
    defense_status = db.Column(db.String(20), default='none')
    defense_questions = db.Column(db.Text)       # JSON: AI针对该生生成的个性化答辩题
    defense_answers = db.Column(db.Text)         # JSON: 学生的闭卷回答
    defense_score = db.Column(db.Float)          # 答辩得分（0-100，教师可修改）
    defense_feedback = db.Column(db.Text)        # JSON: 小林同学生成的答辩总结与建议
    defense_submitted_at = db.Column(db.DateTime)
    teacher_defense_note = db.Column(db.Text)    # 教师复核意见
    teacher_reviewed_at = db.Column(db.DateTime)

    def calculate_total_score(self):
        # 各阶段满分：quiz=25, ai_collab=30, thinking=25, ethics=20
        PHASE_MAX = [25, 30, 25, 20]
        raw_scores = [
            self.quiz_score or 0,
            self.ai_collab_score or 0,
            self.thinking_score or 0,
            self.ethics_score or 0,
        ]
        scores = [max(0, min(m, s)) for s, m in zip(raw_scores, PHASE_MAX)]
        self.quiz_score, self.ai_collab_score, self.thinking_score, self.ethics_score = scores
        self.total_score = sum(scores)
        return self.total_score

    def to_dict(self):
        return {
            'id': self.id,
            'experiment_id': self.experiment_id,
            'student_id': self.student_id,
            'student_name': self.student.name if self.student else None,
            'quiz_score': self.quiz_score,
            'ai_collab_score': self.ai_collab_score,
            'thinking_score': self.thinking_score,
            'ethics_score': self.ethics_score,
            'total_score': self.total_score,
            'status': self.status,
            'final_evaluation': self.final_evaluation,
            'submitted_at': self.submitted_at.isoformat() if self.submitted_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
        }


class PreClassQuiz(db.Model):
    """课前闭卷测试 — 每节课开始前针对上一个实验的3道闭卷题"""
    __tablename__ = 'pre_class_quizzes'

    id = db.Column(db.Integer, primary_key=True)
    # 当前要上的实验（进入该实验前需先完成本测试）
    current_experiment_id = db.Column(db.Integer, db.ForeignKey('experiments.id'), nullable=False)
    # 上一个实验（题目来源）
    prev_experiment_id = db.Column(db.Integer, db.ForeignKey('experiments.id'), nullable=True)
    student_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)

    # 3道题目 (JSON)
    questions = db.Column(db.Text)          # LLM生成的3道题
    student_answers = db.Column(db.Text)    # 学生作答 (JSON)
    score = db.Column(db.Float)             # 本次闭卷得分（满分30分，每题10分）
    answers_feedback = db.Column(db.Text)   # 每题评分详情 (JSON)

    # 综合评价：结合闭卷答题情况 + 上次实验成绩 → LLM生成
    comprehensive_evaluation = db.Column(db.Text)  # 综合评价文本

    status = db.Column(db.String(20), default='pending')  # pending / completed
    started_at = db.Column(db.DateTime, default=datetime.utcnow)
    completed_at = db.Column(db.DateTime)

    current_experiment = db.relationship('Experiment', foreign_keys=[current_experiment_id])
    prev_experiment = db.relationship('Experiment', foreign_keys=[prev_experiment_id])
    student = db.relationship('User', foreign_keys=[student_id])

    def get_questions(self):
        return json.loads(self.questions) if self.questions else []

    def to_dict(self):
        return {
            'id': self.id,
            'current_experiment_id': self.current_experiment_id,
            'prev_experiment_id': self.prev_experiment_id,
            'student_id': self.student_id,
            'score': self.score,
            'status': self.status,
            'comprehensive_evaluation': self.comprehensive_evaluation,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
        }


class TutorChat(db.Model):
    """学生与"小林同学"的对话记录"""
    __tablename__ = 'tutor_chats'

    id = db.Column(db.Integer, primary_key=True)
    student_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False, index=True)
    submission_id = db.Column(db.Integer, db.ForeignKey('submissions.id'), nullable=True, index=True)
    experiment_id = db.Column(db.Integer, db.ForeignKey('experiments.id'), nullable=True)

    role = db.Column(db.String(20), nullable=False)   # student / tutor
    content = db.Column(db.Text, nullable=False)
    phase = db.Column(db.String(30), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)

    submission = db.relationship('Submission', backref=db.backref('tutor_chats', lazy='dynamic'))

    def to_dict(self):
        return {
            'id': self.id, 'student_id': self.student_id,
            'submission_id': self.submission_id,
            'role': self.role, 'content': self.content,
            'phase': self.phase,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S') if self.created_at else None,
        }


class Exam(db.Model):
    """三段式期末考试"""
    __tablename__ = 'exams'

    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200), nullable=False)
    description = db.Column(db.Text)
    teacher_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    topics = db.Column(db.Text, nullable=False)
    difficulty = db.Column(db.String(20), default='medium')
    is_active = db.Column(db.Boolean, default=False)
    # 三段时间配置（分钟）
    phase1_minutes = db.Column(db.Integer, default=20)
    phase2_minutes = db.Column(db.Integer, default=30)
    phase3_minutes = db.Column(db.Integer, default=40)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    # 预生成的统一试卷内容（创建考试时由教师端生成，所有学生共用）
    phase1_questions = db.Column(db.Text)        # 第一段：闭卷基础题（JSON）
    phase2_project = db.Column(db.Text)          # 第二段：代码项目（JSON）
    phase3_requirement = db.Column(db.Text)      # 第三段：项目需求（JSON）

    exam_submissions = db.relationship('ExamSubmission', backref='exam', lazy='dynamic')
    teacher = db.relationship('User', foreign_keys=[teacher_id])

    def get_topics(self):
        return json.loads(self.topics) if self.topics else []

    def set_topics(self, topics_list):
        self.topics = json.dumps(topics_list)

    def to_dict(self):
        return {
            'id': self.id, 'title': self.title, 'description': self.description,
            'topics': self.get_topics(), 'difficulty': self.difficulty,
            'is_active': self.is_active,
            'phase1_minutes': self.phase1_minutes,
            'phase2_minutes': self.phase2_minutes,
            'phase3_minutes': self.phase3_minutes,
        }


class ExamSubmission(db.Model):
    """三段式考试提交"""
    __tablename__ = 'exam_submissions'

    id = db.Column(db.Integer, primary_key=True)
    exam_id = db.Column(db.Integer, db.ForeignKey('exams.id'), nullable=False)
    student_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)

    # ---- 第一段：闭卷基础（满分20分，20min）----
    phase1_questions = db.Column(db.Text)        # LLM生成的5道基础题
    phase1_answers = db.Column(db.Text)          # 学生答案
    phase1_score = db.Column(db.Float)
    phase1_feedback = db.Column(db.Text)
    phase1_started_at = db.Column(db.DateTime)
    phase1_submitted_at = db.Column(db.DateTime)

    # ---- 第二段：有限AI支持（满分30分，30min）----
    phase2_project = db.Column(db.Text)          # 代码项目（含bug）
    phase2_answers = db.Column(db.Text)          # 学生改错+拓展答案
    phase2_score = db.Column(db.Float)
    phase2_feedback = db.Column(db.Text)
    phase2_started_at = db.Column(db.DateTime)
    phase2_submitted_at = db.Column(db.DateTime)

    # ---- 第三段：完全AI协同（满分50分，40min）----
    phase3_requirement = db.Column(db.Text)      # 项目构建需求
    phase3_code = db.Column(db.Text)             # 学生最终代码
    phase3_prompt_log = db.Column(db.Text)       # 提示词记录
    phase3_score = db.Column(db.Float)
    phase3_feedback = db.Column(db.Text)
    phase3_started_at = db.Column(db.DateTime)
    phase3_submitted_at = db.Column(db.DateTime)

    # ---- 综合 ----
    total_score = db.Column(db.Float)
    final_evaluation = db.Column(db.Text)
    # status: phase1 -> phase2 -> phase3 -> completed
    status = db.Column(db.String(20), default='phase1')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    completed_at = db.Column(db.DateTime)

    student = db.relationship('User', foreign_keys=[student_id])

    def calculate_total(self):
        self.total_score = (self.phase1_score or 0) + (self.phase2_score or 0) + (self.phase3_score or 0)
        return self.total_score

    def to_dict(self):
        return {
            'id': self.id, 'exam_id': self.exam_id, 'student_id': self.student_id,
            'student_name': self.student.name if self.student else None,
            'phase1_score': self.phase1_score, 'phase2_score': self.phase2_score,
            'phase3_score': self.phase3_score, 'total_score': self.total_score,
            'status': self.status,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
        }



class Homework(db.Model):
    """课后作业模型（区别于课堂实验）"""
    __tablename__ = 'homeworks'

    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200), nullable=False)
    description = db.Column(db.Text)
    teacher_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    topics = db.Column(db.Text, nullable=False)        # JSON 学习主题列表
    difficulty = db.Column(db.String(20), default='medium')
    major_category = db.Column(db.String(32), default='general')
    is_active = db.Column(db.Boolean, default=False)
    due_date = db.Column(db.DateTime)                  # 截止时间（可选）
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    teacher = db.relationship('User', foreign_keys=[teacher_id])
    hw_submissions = db.relationship('HomeworkSubmission', backref='homework', lazy='dynamic')

    def get_topics(self):
        return json.loads(self.topics) if self.topics else []

    def set_topics(self, topics_list):
        self.topics = json.dumps(topics_list)


class HomeworkSubmission(db.Model):
    """学生课后作业提交记录"""
    __tablename__ = 'homework_submissions'

    id = db.Column(db.Integer, primary_key=True)
    homework_id = db.Column(db.Integer, db.ForeignKey('homeworks.id'), nullable=False)
    student_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)

    # ---- 作业内容（个性化生成）----
    hw_task = db.Column(db.Text)              # JSON: 小林同学为该生生成的个性化作业题目
    hw_answer = db.Column(db.Text)            # 学生作答内容（代码/文字）
    hw_prompt_log = db.Column(db.Text)        # JSON: 与小林同学的交互记录（可选）
    hw_score = db.Column(db.Float)            # 作业得分（AI评分）
    hw_feedback = db.Column(db.Text)          # JSON: AI评分反馈

    # ---- 答辩（与 Submission 相同字段）----
    defense_status = db.Column(db.String(20), default='none')
    # none / generated / in_progress / submitted / reviewed
    defense_questions = db.Column(db.Text)    # JSON: 个性化答辩题
    defense_answers = db.Column(db.Text)      # JSON: 学生闭卷作答
    defense_score = db.Column(db.Float)       # 答辩得分（教师可修改）
    defense_feedback = db.Column(db.Text)     # JSON: 小林同学答辩总结
    defense_submitted_at = db.Column(db.DateTime)
    teacher_defense_note = db.Column(db.Text)
    teacher_reviewed_at = db.Column(db.DateTime)

    # ---- 状态与时间 ----
    # status: assigned / in_progress / submitted
    status = db.Column(db.String(20), default='assigned')
    submitted_at = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    student = db.relationship('User', foreign_keys=[student_id])

    def to_dict(self):
        return {
            'id': self.id,
            'homework_id': self.homework_id,
            'student_id': self.student_id,
            'student_name': self.student.name if self.student else None,
            'hw_score': self.hw_score,
            'defense_score': self.defense_score,
            'defense_status': self.defense_status,
            'status': self.status,
            'submitted_at': self.submitted_at.isoformat() if self.submitted_at else None,
        }

class AIGCDetection(db.Model):
    """AIGC检测记录"""
    __tablename__ = 'aigc_detections'

    id = db.Column(db.Integer, primary_key=True)
    student_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    submission_id = db.Column(db.Integer, db.ForeignKey('submissions.id'), nullable=True)
    content_type = db.Column(db.String(30))  # code / text / report
    content_snippet = db.Column(db.Text)
    ai_rate = db.Column(db.Float)            # 0-100, AI生成比例
    detection_detail = db.Column(db.Text)    # JSON: 详细检测结果
    detected_at = db.Column(db.DateTime, default=datetime.utcnow)

    student = db.relationship('User', foreign_keys=[student_id])


# ==================== 削峰填谷：题库池 ====================

class QuestionPool(db.Model):
    """预生成题库池 —— 课前批量调用LLM生成题目，课中直接分配，消除实时生成延迟"""
    __tablename__ = 'question_pool'

    id = db.Column(db.Integer, primary_key=True)
    # 关联到实验/考试/作业
    source_type = db.Column(db.String(20), nullable=False, index=True)  # experiment / exam / homework / pre_class
    source_id = db.Column(db.Integer, nullable=False, index=True)       # experiment_id / exam_id / homework_id
    # 题目类型
    task_type = db.Column(db.String(40), nullable=False, index=True)
    # knowledge_quiz / ai_collab_task / thinking_task / ethics_case / pre_class_quiz / defense_questions
    difficulty = db.Column(db.String(20), default='medium')
    major_category = db.Column(db.String(32), default='general')

    # 预生成的题目内容（JSON）
    content = db.Column(db.Text, nullable=False)

    # 分配状态
    is_allocated = db.Column(db.Boolean, default=False, index=True)
    allocated_to = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)
    allocated_at = db.Column(db.DateTime, nullable=True)

    # 生成时间
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    student = db.relationship('User', foreign_keys=[allocated_to])

    def get_content(self):
        return json.loads(self.content) if self.content else {}


# ==================== 削峰填谷：异步任务队列 ====================

class AsyncTask(db.Model):
    """异步评分任务 —— 学生提交后立即入队，后台Worker按可控速率处理"""
    __tablename__ = 'async_tasks'

    id = db.Column(db.Integer, primary_key=True)
    # 任务类型
    task_type = db.Column(db.String(40), nullable=False, index=True)
    # evaluate_quiz / evaluate_ai_collab / evaluate_thinking / evaluate_ethics
    # evaluate_defense / generate_evaluation / evaluate_exam_phase2 / evaluate_exam_phase3
    # generate_exam_evaluation / evaluate_pre_class / generate_pre_class_eval

    # 优先级：0=最高（课中急需）, 1=高, 2=中（课后批量）
    priority = db.Column(db.Integer, default=1, index=True)

    # 关联对象
    submission_id = db.Column(db.Integer, nullable=True)     # Submission / ExamSubmission / HomeworkSubmission / PreClassQuiz 的 id
    submission_type = db.Column(db.String(30), nullable=True) # submission / exam_submission / hw_submission / pre_class_quiz

    # 任务参数（JSON序列化的函数参数）
    params = db.Column(db.Text, nullable=False)

    # 执行状态：pending / running / completed / failed
    status = db.Column(db.String(20), default='pending', index=True)
    result = db.Column(db.Text, nullable=True)               # JSON: 执行结果
    error_message = db.Column(db.Text, nullable=True)
    retry_count = db.Column(db.Integer, default=0)
    max_retries = db.Column(db.Integer, default=3)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    started_at = db.Column(db.DateTime, nullable=True)
    completed_at = db.Column(db.DateTime, nullable=True)

    def get_params(self):
        return json.loads(self.params) if self.params else {}

    def get_result(self):
        return json.loads(self.result) if self.result else {}

    def to_dict(self):
        return {
            'id': self.id, 'task_type': self.task_type, 'priority': self.priority,
            'status': self.status, 'submission_id': self.submission_id,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
        }


# ==================== LLM算力路由配置 ====================

class LLMProviderConfig(db.Model):
    """LLM算力路由配置 - 教师可在页面上切换本地/云端/混合云模式"""
    __tablename__ = 'llm_provider_config'

    id = db.Column(db.Integer, primary_key=True)

    # 运行模式: local / cloud / hybrid
    mode = db.Column(db.String(20), nullable=False, default='local')

    # 云端API配置
    cloud_api_url = db.Column(db.String(500), nullable=True)
    cloud_api_key = db.Column(db.String(500), nullable=True)
    cloud_model = db.Column(db.String(100), nullable=True)

    # 混合云控制
    cloud_daily_limit = db.Column(db.Integer, default=500)
    cloud_calls_today = db.Column(db.Integer, default=0)
    cloud_calls_date = db.Column(db.String(10), nullable=True)

    # 本地配置（记录当前值，方便页面显示）
    local_api_url = db.Column(db.String(500), nullable=True)
    local_model = db.Column(db.String(100), nullable=True)

    updated_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_by = db.Column(db.String(64), nullable=True)

    def get_cloud_key_masked(self):
        k = self.cloud_api_key or ''
        if len(k) <= 8:
            return '*' * len(k)
        return k[:4] + '*' * (len(k) - 8) + k[-4:]

    def to_dict(self):
        return {
            'id': self.id, 'mode': self.mode,
            'cloud_api_url': self.cloud_api_url,
            'cloud_api_key_masked': self.get_cloud_key_masked(),
            'cloud_model': self.cloud_model,
            'cloud_daily_limit': self.cloud_daily_limit,
            'cloud_calls_today': self.cloud_calls_today,
            'cloud_calls_date': self.cloud_calls_date,
            'local_api_url': self.local_api_url,
            'local_model': self.local_model,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'updated_by': self.updated_by,
        }


class DismissedWarning(db.Model):
    """已消除的预警记录（归档备查）"""
    __tablename__ = 'dismissed_warnings'

    id = db.Column(db.Integer, primary_key=True)
    student_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    warning_type = db.Column(db.String(50), nullable=False)        # e.g. low_score, no_submission, answer_seeking ...
    warning_message = db.Column(db.Text, nullable=False)           # 预警描述文本
    warning_level = db.Column(db.String(20), nullable=False)       # red / orange / yellow
    warning_severity = db.Column(db.String(20), nullable=True)     # 单条预警的 severity
    dismissed_by = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)  # 操作教师id
    dismissed_at = db.Column(db.DateTime, default=datetime.utcnow) # 消除时间
    note = db.Column(db.Text, nullable=True)                       # 教师备注（可选）

    student = db.relationship('User', foreign_keys=[student_id], backref='dismissed_warnings')
    teacher = db.relationship('User', foreign_keys=[dismissed_by])

    def to_dict(self):
        return {
            'id': self.id,
            'student_id': self.student_id,
            'student_name': self.student.name if self.student else '',
            'student_no': self.student.student_id if self.student else '',
            'class_name': self.student.class_name if self.student else '',
            'warning_type': self.warning_type,
            'warning_message': self.warning_message,
            'warning_level': self.warning_level,
            'warning_severity': self.warning_severity,
            'dismissed_by_name': self.teacher.name if self.teacher else '',
            'dismissed_at': self.dismissed_at.strftime('%Y-%m-%d %H:%M') if self.dismissed_at else '',
            'note': self.note or '',
        }


class GradeArchive(db.Model):
    """历史成绩归档 —— 保存从原课程系统迁移过来的学生成绩，用于期末统计核算"""
    __tablename__ = 'grade_archive'

    id = db.Column(db.Integer, primary_key=True)
    student_id = db.Column(db.Integer, nullable=False)
    student_username = db.Column(db.String(64))
    student_name = db.Column(db.String(64))
    student_no = db.Column(db.String(32))
    class_name = db.Column(db.String(64))
    major_category = db.Column(db.String(32))

    # 原课程信息
    source_course = db.Column(db.String(100), default='从计算机到人工智能')
    source_experiment_id = db.Column(db.Integer)
    source_experiment_title = db.Column(db.String(200))

    # 四阶段成绩
    quiz_score = db.Column(db.Float)         # 概念理解
    ai_collab_score = db.Column(db.Float)    # 编程实践
    thinking_score = db.Column(db.Float)     # 程序分析
    ethics_score = db.Column(db.Float)       # 综合应用
    total_score = db.Column(db.Float)

    # 答辩成绩
    defense_score = db.Column(db.Float)

    # 状态与时间
    status = db.Column(db.String(20))
    completed_at = db.Column(db.String(50))

    # 综合评价
    final_evaluation = db.Column(db.Text)

    # 归档时间
    archived_at = db.Column(db.DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            'id': self.id,
            'student_name': self.student_name,
            'student_no': self.student_no,
            'class_name': self.class_name,
            'source_experiment_title': self.source_experiment_title,
            'quiz_score': self.quiz_score,
            'ai_collab_score': self.ai_collab_score,
            'thinking_score': self.thinking_score,
            'ethics_score': self.ethics_score,
            'total_score': self.total_score,
            'defense_score': self.defense_score,
            'status': self.status,
            'completed_at': self.completed_at,
        }
