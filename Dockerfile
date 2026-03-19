FROM python:3.12-slim
WORKDIR /app
RUN mkdir -p /data
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY bot.py .
ENV DATA_DIR=/data
CMD ["python", "bot.py"]
