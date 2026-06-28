# 安装部署手册

## 环境要求
- Python 3.9+
- 依赖：`pip install -r requirements.txt`（Flask、Flask-SQLAlchemy、Werkzeug、requests）

## 快速启动
```bash
pip install -r requirements.txt
python migrate_and_seed.py        # 初始化数据库：创建演示教师与 12 次实验结构（不含任何真实学生数据）
python app.py
```
浏览器打开 http://localhost:5000 ，演示教师账号 `teacher / teacher123`
（**仅为演示环境默认账号，正式部署首次登录后必须立即修改密码**）。

> 说明：本开源版**不含任何真实教学数据库**。`instance/` 与 `*.db` 已通过 `.gitignore` 排除；
> 运行 `migrate_and_seed.py` 会生成可用于演示的空库与课程结构，便于他人快速复现。

## 算力端点配置（任选其一）
通过环境变量（参考 `.env.example`）或「教师端 → 算力配置」页设置：
```bash
export LLM_API_URL="https://<国产云端大模型API>/v1/chat/completions"   # 通用 Chat Completions 兼容端点
export LLM_API_KEY="<your-key>"                                          # 切勿写入代码库
export LLM_MODEL="DeepSeek-R1-Distill-Qwen-32B"
```
仅需一个云端 API Key 即可运行，无需自建本地算力；如需「数据不出校」，将 `LLM_API_URL`
指向本地/昇腾私有化推理端点并在教师端切换 `local / hybrid` 模式即可。

## 可选功能开关
```bash
export DEFENSE_AUTO_GENERATE=false        # 提交完成是否自动生成答辩题（默认关）
export DEFENSE_COMPOSITE_WEIGHT=0.3       # 答辩分计入综合的权重（仅展示）
```

## 重新初始化数据
```bash
rm -f instance/python_lab.db
python migrate_and_seed.py --fresh        # 全新空库（演示教师 + 12 次实验）
```

## 安全提示
- 演示默认账号仅用于本地体验，正式部署务必修改密码、设置强 `SECRET_KEY`。
- 学生代码运行采用「黑名单 + 子进程 + 超时」沙箱，适合课程演示；**公网部署时建议以容器 / 低权限用户 / 资源隔离**方式运行学生代码。
