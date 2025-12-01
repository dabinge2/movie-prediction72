
# 更新日志

所有重大变更将记录在此文件中。

## [1.0.0] - 2025-12-01

### ✨ 新增
- 基于 Flask + PySpark 的 Web 分析平台
- Task1: 支持上传 `movies.csv` 并计算 Top 20 高分电影
- Task2: 从 MySQL 读取 `tags` 和 `users` 表，按性别统计热门标签
- Task3: 实现实时评分流处理（监听 `streaming_data/` 目录），通过 Socket.IO 推送统计结果
- 响应式前端模板（基于 Bootstrap 风格，预留扩展空间）
- 自动创建上传/流数据目录

### ⚙️ 依赖
- `flask==2.0.1`
- `pyspark==3.3.0`
- `flask-socketio`
- `mysql-connector-python`
- `pyecharts==1.9.1`（预留可视化扩展）

### 🛠 配置
- 支持通过 `config.py` 管理路径、数据库、Spark 参数
- 数据库初始化脚本 `setup_mysql.py`

> 初始版本发布。
