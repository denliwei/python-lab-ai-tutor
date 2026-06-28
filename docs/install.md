# 安装部署手册

## 环境要求
- Python 3.9+
- 依赖：`pip install flask flask-sqlalchemy werkzeug requests`

## 快速启动（含预置数据）
```bash
pip install flask flask-sqlalchemy werkzeug requests
python app.py
```
浏览器打开 http://localhost:5000 ，教师账号 `teacher / teacher123`。
系统已含预初始化数据库 `instance/python_lab.db`（128 名学生 + 12 次实验 + 历史成绩归档，**已脱敏**）。

## 算力端点配置（任选其一）
通过环境变量或「教师端 → 算力配置」设置：
```bash
export LLM_API_URL="http://<昇腾节点地址>:1234/v1/chat/completions"   # 本地/默认端点
export LLM_API_KEY="<your-key>"                                       # 切勿写入代码库
export LLM_MODEL="DeepSeek-R1-Distill-Qwen-32B"
```
运行模式 `local / cloud / hybrid` 在教师端切换，详见 `README.md` 的「算力适配说明」。

## 可选功能开关
```bash
export DEFENSE_AUTO_GENERATE=false        # 提交完成是否自动生成答辩题（默认关）
export DEFENSE_COMPOSITE_WEIGHT=0.3       # 答辩分计入综合的权重（仅展示）
```

## 重新初始化数据
```bash
rm instance/python_lab.db
python migrate_and_seed.py --fresh        # 全新空库
# 或 python migrate_and_seed.py --source /path/to/ai_lab.db   # 从源库迁移
```
