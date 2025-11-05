FROM public.ecr.aws/unocha/python:3.12-stable

WORKDIR /srv/sdd

COPY . .

RUN apk add --no-cache git  && \
    pip3 install -r requirements.txt

ENTRYPOINT [ "python3", "main.py" ]
