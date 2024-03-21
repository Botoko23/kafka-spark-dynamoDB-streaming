FROM jupyter/all-spark-notebook

WORKDIR /home/jovyan/work

COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt
