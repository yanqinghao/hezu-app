FROM python:3.12-alpine

WORKDIR /code

COPY main.py env.py log.py requirements.txt /code/

COPY db/ /code/db/

COPY message/ /code/message/

RUN apk update && \
    apk add --no-cache dumb-init netcat-openbsd curl && \
    pip install --no-cache-dir -r requirements.txt && \
    rm -rf /var/cache/apk/*

ENTRYPOINT [ "/usr/bin/dumb-init", "--" ]

CMD [ "python", "main.py" ]
