FROM python:3.7
ENV PYTHONUNBUFFERED 1

RUN mkdir /data_engineering_task
RUN apt update
RUN apt install -y cmake
RUN apt-get install ffmpeg libsm6 libxext6 poppler-utils  -y
WORKDIR /data_engineering_services
RUN ls
COPY . .
RUN pip install -r requirements.txt --no-cache-dir
EXPOSE 3000
CMD python3 -m flask run --host=0.0.0.0

#ENTRYPOINT python app.py
#CMD ["flask", "run", "--host", "0.0.0.0"]

#ENTRYPOINT ["/start.sh"]