FROM python:3.12-slim-buster

WORKDIR /code

COPY main.py env.py requirements.txt /code/

COPY db/ /code/

RUN sed -i 's/deb.debian.org/mirrors.ustc.edu.cn/g' /etc/apt/sources.list && \
    sed -i 's|security.debian.org/debian-security|mirrors.ustc.edu.cn/debian-security|g' /etc/apt/sources.list && \
    apt update && \
    apt install dumb-init netcat curl -y && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean autoclean && \
    apt-get autoremove --yes && \
    pip config set global.index-url 'https://pypi.mirrors.ustc.edu.cn/simple' && \
    pip install --no-cache-dir -r requirements.txt && \
    rm -rf /var/lib/{apt,dpkg,cache,log}/

ENTRYPOINT [ "/usr/bin/dumb-init", "--" ]

CMD [ "python", "main.py" ]
