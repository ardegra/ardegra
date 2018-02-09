FROM python:3

COPY . /root/app
WORKDIR /root/app

RUN pip install -r requirements.txt

CMD bash