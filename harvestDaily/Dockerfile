FROM python:3
ADD . /app
WORKDIR /app
RUN python3 -m pip install -r requirements.txt
CMD ["python","daily_arxiv_harverst.py"]
