# 使用 Python 作为基础镜像
FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 复制应用程序代码到容器中
COPY . /app

# 安装应用程序依赖
RUN pip install --no-cache-dir fastapi uvicorn

# Make port 80 available to the world outside this container
EXPOSE 80

# Define environment variable
ENV PORT 80

# 启动 FastAPI 应用程序
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]