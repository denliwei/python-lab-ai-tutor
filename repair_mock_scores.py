"""
Re-enqueue grading tasks for submissions whose scores were written from
temporary mock evaluation results.

Usage:
    python repair_mock_scores.py --experiment-id 8 --enqueue

Run without --enqueue for a dry run.
"""
import argparse
import json

from app import app
from models import AsyncTask, Submission, db
from task_queue import task_service


EVAL_TASKS = (
    'evaluate_quiz',
    'evaluate_ai_collab',
    'evaluate_thinking',
    'evaluate_ethics',
)


def load_json(value, default):
    if not value:
        return default
    try:
        return json.loads(value)
    except Exception:
        return default


def is_mock_payload(value):
    if not value:
        return False
    try:
        payload = json.loads(value)
        return bool(payload.get('_is_mock')) or '评估服务暂时不可用' in json.dumps(payload, ensure_ascii=False)
    except Exception:
        return '评估服务暂时不可用' in str(value)


def is_affected_submission(sub):
    feedbacks = [
        sub.quiz_feedback,
        sub.ai_collab_feedback,
        sub.thinking_feedback,
        sub.ethics_feedback,
    ]
    return any(is_mock_payload(item) for item in feedbacks) or is_mock_payload(sub.final_evaluation)


def has_open_task(submission_id, task_type):
    return AsyncTask.query.filter(
        AsyncTask.submission_id == submission_id,
        AsyncTask.submission_type == 'submission',
        AsyncTask.task_type == task_type,
        AsyncTask.status.in_(('pending', 'running')),
    ).first() is not None


def build_tasks(sub):
    quiz_data = load_json(sub.quiz_questions, {})
    ai_task = load_json(sub.ai_collab_task, {})
    thinking_task = load_json(sub.thinking_task, {})
    ethics_case = load_json(sub.ethics_case, {})
    quiz_answers = load_json(sub.quiz_student_answers, {})

    return {
        'evaluate_quiz': {
            'questions': quiz_data.get('questions', []),
            'answers': quiz_answers,
        },
        'evaluate_ai_collab': {
            'task_description': ai_task.get('task_description', ''),
            'prompt_history': sub.ai_collab_prompt_history or '',
            'final_code': sub.ai_collab_final_code or '',
            'reflection': sub.ai_collab_reflection or '',
        },
        'evaluate_thinking': {
            'task_description': thinking_task.get('task_description', ''),
            'answer': sub.thinking_student_answer or '',
        },
        'evaluate_ethics': {
            'case_description': ethics_case.get('case_description', ''),
            'answer': sub.ethics_student_answer or '',
        },
    }


def reenqueue_experiment(experiment_id, enqueue=False):
    affected = Submission.query.filter_by(experiment_id=experiment_id).all()
    affected = [sub for sub in affected if is_affected_submission(sub)]
    queued = 0

    print(f'实验 {experiment_id} 受影响提交数: {len(affected)}')
    for sub in affected:
        student_no = sub.student.student_id if sub.student else sub.student_id
        print(f'- submission={sub.id} student={student_no} total={sub.total_score}')
        task_params = build_tasks(sub)
        for task_type in EVAL_TASKS:
            if has_open_task(sub.id, task_type):
                print(f'  skip open task: {task_type}')
                continue
            print(f'  enqueue: {task_type}')
            if enqueue:
                task_service.enqueue(
                    task_type,
                    task_params[task_type],
                    submission_id=sub.id,
                    submission_type='submission',
                    priority=1,
                )
                queued += 1

    if enqueue:
        db.session.commit()
    print(f'新入队任务数: {queued}')
    if not enqueue:
        print('当前为 dry run；加 --enqueue 才会真正入队。')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--experiment-id', type=int, required=True)
    parser.add_argument('--enqueue', action='store_true')
    args = parser.parse_args()

    with app.app_context():
        reenqueue_experiment(args.experiment_id, enqueue=args.enqueue)


if __name__ == '__main__':
    main()
