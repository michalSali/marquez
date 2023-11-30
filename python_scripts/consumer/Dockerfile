FROM python:3.10
WORKDIR /usr/src/app
COPY . .
COPY ../common_utils.py .
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "./consumer.py"]