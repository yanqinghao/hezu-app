FROM python:3.12-slim-buster

WORKDIR /code

COPY main.py env.py requirements.txt /code/

COPY db/ /code/

RUN apt update && \
    apt install dumb-init netcat curl -y && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean autoclean && \
    apt-get autoremove --yes && \
    pip install --no-cache-dir -r requirements.txt && \
    rm -rf /var/lib/{apt,dpkg,cache,log}/

ENTRYPOINT [ "/usr/bin/dumb-init", "--" ]

CMD [ "python", "main.py" ]
