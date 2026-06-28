"""
数据迁移与初始化脚本 - 《Python程序设计》课程实验系统

迁移策略：
  - 学生账号：完整迁移（账号、密码、班级、专业）
  - 旧成绩：归档到 grade_archive 表（供期末统计核算）
  - 新实验：12次全新Python实验，零提交，干净启动
  - 旧对话/提交：不迁移（避免教师端看到"已完成"的假数据）

用法：
    python migrate_and_seed.py                          # 自动查找源库并迁移
    python migrate_and_seed.py --source path/to/ai_lab.db  # 指定源库
    python migrate_and_seed.py --fresh                  # 全新部署，不迁移
"""
import os
import sys
import json
import sqlite3
import argparse
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app import app, db
from models import (User, Experiment, GradeArchive, LLMProviderConfig)

# ==================== 12次Python程序设计实验定义 ====================
PYTHON_EXPERIMENTS = [
    {
        'title': '实验1：Python环境搭建与基本输入输出',
        'description': '了解Python语言特点与应用领域，搭建Python开发环境，'
                       '掌握变量的定义与命名规范，熟练使用print()和input()进行基本输入输出。',
        'topics': ['python_overview', 'python_io', 'variables_basics'],
        'difficulty': 'easy',
    },
    {
        'title': '实验2：数字类型与字符串操作',
        'description': '掌握Python数字类型的表示与转换，熟练使用字符串的格式化输出，'
                       '掌握字符串切片、查找、替换等常用操作。',
        'topics': ['number_types', 'string_ops', 'operators'],
        'difficulty': 'easy',
    },
    {
        'title': '实验3：列表与元组',
        'description': '理解列表和元组的定义与区别，掌握列表的增删改查操作，'
                       '熟练使用列表推导式，理解元组的不可变特性及其应用场景。',
        'topics': ['list_type', 'tuple_type'],
        'difficulty': 'medium',
    },
    {
        'title': '实验4：字典与集合',
        'description': '理解字典的键值对结构，掌握字典的创建、访问、修改和遍历操作，'
                       '理解集合的无序不重复特性，掌握集合的交并差运算。',
        'topics': ['dict_type', 'set_type'],
        'difficulty': 'medium',
    },
    {
        'title': '实验5：条件语句与分支结构',
        'description': '掌握if-elif-else语句的多种使用形式，熟练运用嵌套条件判断，'
                       '学会使用条件表达式，解决实际的分支逻辑问题。',
        'topics': ['if_statement', 'operators'],
        'difficulty': 'medium',
    },
    {
        'title': '实验6：循环语句与综合控制',
        'description': '掌握for循环与while循环的使用方法和适用场景，'
                       '熟练使用break和continue控制循环流程，掌握循环嵌套的编程技巧。',
        'topics': ['for_loop', 'while_loop', 'loop_control'],
        'difficulty': 'medium',
    },
    {
        'title': '实验7：异常处理',
        'description': '了解Python异常的分类与层次结构，掌握try-except-else-finally语句，'
                       '理解assert断言和raise主动抛出异常的用法。',
        'topics': ['exception_handling', 'assert_raise'],
        'difficulty': 'medium',
    },
    {
        'title': '实验8：函数定义与应用',
        'description': '掌握函数的定义与调用方法，理解各种参数传递方式，'
                       '掌握局部变量与全局变量的区别，了解匿名函数和递归函数。',
        'topics': ['function_def', 'function_params', 'variable_scope', 'lambda_recursive'],
        'difficulty': 'medium',
    },
    {
        'title': '实验9：模块与包',
        'description': '理解模块的概念，掌握import和from...import导入方式，'
                       '熟练使用math、random、datetime等标准模块，学会创建自定义模块和包。',
        'topics': ['module_import', 'standard_modules', 'custom_modules', 'third_party'],
        'difficulty': 'medium',
    },
    {
        'title': '实验10：图形用户界面编程',
        'description': '了解GUI编程基本概念，使用tkinter创建窗口和基本组件，'
                       '掌握三种布局管理器，实现简单的事件驱动交互程序。',
        'topics': ['gui_basics', 'tkinter_widgets', 'layout_managers', 'event_handling'],
        'difficulty': 'hard',
    },
    {
        'title': '实验11：文件处理',
        'description': '掌握文件的打开模式，熟练使用各种读取和写入方法，'
                       '学会使用os模块进行文件和文件夹操作。',
        'topics': ['file_open_close', 'file_read', 'file_write', 'file_operations', 'file_path'],
        'difficulty': 'hard',
    },
    {
        'title': '实验12：类和对象',
        'description': '理解面向对象编程的基本概念，掌握类的定义与对象的创建，'
                       '理解构造和析构方法，掌握类的继承与方法重写。',
        'topics': ['oop_concepts', 'class_def', 'constructor_destructor',
                   'class_static_methods', 'inheritance_polymorphism'],
        'difficulty': 'hard',
    },
]


def find_source_db(args_source):
    candidates = [
        args_source,
        'instance/ai_lab.db',
        '../录课1/instance/ai_lab.db',
        os.path.join(os.path.dirname(__file__), '..', 'original_code', '录课1', 'instance', 'ai_lab.db'),
    ]
    for c in candidates:
        if c and os.path.exists(c):
            return c
    return None


def migrate_from_source(source_db_path):
    """从源数据库迁移：只保留学生账号 + 归档旧成绩"""
    print(f"\n{'='*60}")
    print(f"  从 {source_db_path} 迁移数据")
    print(f"  策略：学生账号 + 旧成绩归档，新实验干净启动")
    print(f"{'='*60}")

    src = sqlite3.connect(source_db_path)
    src.row_factory = sqlite3.Row

    # 1. 迁移教师
    for t in src.execute("SELECT * FROM users WHERE role='teacher'"):
        if not User.query.filter_by(username=t['username']).first():
            u = User(username=t['username'], name=t['name'], role='teacher')
            u.password_hash = t['password_hash']
            db.session.add(u)
            print(f"  [新增] 教师 {t['name']}")

    # 2. 迁移学生
    added = skipped = 0
    for s in src.execute("SELECT * FROM users WHERE role='student'"):
        if User.query.filter_by(username=s['username']).first():
            skipped += 1
        else:
            u = User(
                username=s['username'], name=s['name'], role='student',
                student_id=s['student_id'], class_name=s['class_name'],
                major_category=s['major_category'],
            )
            u.password_hash = s['password_hash']
            db.session.add(u)
            added += 1
    db.session.commit()
    print(f"  学生迁移：新增 {added} 人，跳过 {skipped} 人")

    # 3. 归档旧成绩
    exp_titles = {e['id']: e['title'] for e in src.execute("SELECT id, title FROM experiments")}
    user_info = {}
    for u in src.execute("SELECT id, username, name, student_id, class_name, major_category FROM users WHERE role='student'"):
        user_info[u['id']] = dict(u)

    archived = 0
    for sub in src.execute("SELECT * FROM submissions WHERE status='completed'"):
        uid = sub['student_id']
        uinfo = user_info.get(uid, {})
        ga = GradeArchive(
            student_id=uid,
            student_username=uinfo.get('username', ''),
            student_name=uinfo.get('name', ''),
            student_no=uinfo.get('student_id', ''),
            class_name=uinfo.get('class_name', ''),
            major_category=uinfo.get('major_category', ''),
            source_course='从计算机到人工智能',
            source_experiment_id=sub['experiment_id'],
            source_experiment_title=exp_titles.get(sub['experiment_id'], f'实验{sub["experiment_id"]}'),
            quiz_score=sub['quiz_score'],
            ai_collab_score=sub['ai_collab_score'],
            thinking_score=sub['thinking_score'],
            ethics_score=sub['ethics_score'],
            total_score=sub['total_score'],
            defense_score=sub['defense_score'],
            status=sub['status'],
            completed_at=sub['completed_at'],
            final_evaluation=sub['final_evaluation'],
        )
        db.session.add(ga)
        archived += 1

    db.session.commit()
    print(f"  历史成绩归档：{archived} 条已完成记录")
    src.close()

    # 4. 创建12次新实验
    create_experiments()

    # 5. 创建默认LLM配置
    ensure_llm_config()


def create_experiments():
    """创建12次全新Python实验"""
    teacher = User.query.filter_by(role='teacher').first()
    if not teacher:
        teacher = User(username='teacher', name='演示教师', role='teacher')
        teacher.set_password('teacher123')
        db.session.add(teacher)
        db.session.flush()

    print(f"\n  创建12次Python程序设计实验:")
    for i, exp_def in enumerate(PYTHON_EXPERIMENTS):
        existing = Experiment.query.filter_by(title=exp_def['title']).first()
        if existing:
            print(f"    [跳过] {exp_def['title']}")
            continue
        exp = Experiment(
            title=exp_def['title'],
            description=exp_def['description'],
            teacher_id=teacher.id,
            difficulty=exp_def['difficulty'],
            major_category='general',
            is_active=(i < 4),
        )
        exp.set_topics(exp_def['topics'])
        db.session.add(exp)
        status = '✓激活' if exp.is_active else '○未激活'
        print(f"    [{status}] {exp_def['title']}")

    db.session.commit()


def ensure_llm_config():
    if not LLMProviderConfig.query.first():
        cfg = LLMProviderConfig(
            mode='local',
            local_api_url='http://ascend-llm.local:1234/v1/chat/completions',
            local_model='MiniMax-M2.5',
        )
        db.session.add(cfg)
        db.session.commit()
        print(f"  ✓ 默认LLM配置已创建")


def create_fresh_db():
    """全新部署"""
    print(f"\n{'='*60}")
    print(f"  全新部署（不迁移旧数据）")
    print(f"{'='*60}")

    teacher = User.query.filter_by(username='teacher').first()
    if not teacher:
        teacher = User(username='teacher', name='演示教师', role='teacher')
        teacher.set_password('teacher123')
        db.session.add(teacher)
        db.session.commit()
        print(f"  ✓ 教师账号：teacher / teacher123")

    create_experiments()
    ensure_llm_config()


def main():
    parser = argparse.ArgumentParser(description='Python程序设计课程系统 - 数据迁移与初始化')
    parser.add_argument('--source', '-s', default=None, help='源数据库路径')
    parser.add_argument('--fresh', action='store_true', help='全新部署')
    args = parser.parse_args()

    with app.app_context():
        db.create_all()
        print("✓ 数据库表已创建")

        if args.fresh:
            create_fresh_db()
        else:
            source_db = find_source_db(args.source)
            if source_db:
                migrate_from_source(source_db)
            else:
                print("\n⚠ 未找到源数据库，创建全新数据库")
                create_fresh_db()

    print(f"\n{'='*60}")
    print(f"  ✅ 初始化完成！")
    print(f"{'='*60}")
    print(f"  启动: python app.py")
    print(f"  地址: http://localhost:5000")
    print(f"  教师: teacher / teacher123")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
