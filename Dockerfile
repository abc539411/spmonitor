FROM python:3.8-slim
# Or any preferred Python version.
ADD main.py /
COPY . /
RUN pip install pytz pandas brotli requests environs astral
RUN pip install python-telegram-bot==12.0.0
CMD [ "python", "-u", "./main.py" ]
# Or enter the name of your unique directory and parameter set.