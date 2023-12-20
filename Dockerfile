FROM python:3.11-slim-buster
WORKDIR /code
COPY . .
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt
CMD [ "python", "scheduled_aresol.py" ]
