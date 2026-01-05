FROM 803135791119.dkr.ecr.ap-northeast-2.amazonaws.com/deploy/reco/reco-base-image:emr-7.8.0

ENV WORKDIR=/opt/app
WORKDIR $WORKDIR

# AWS
ENV AWS_DEFAULT_REGION=ap-northeast-2
ENV AWS_DEFAULT_OUTPUT=json

RUN echo "export PS1='\[\e[1;32m\]\u@\h:\[\e[1;34m\]\w\[\e[0m\]\$ '" >> /root/.bashrc && \
    echo "alias ls='ls --color=auto'" >> /root/.bashrc

RUN dnf update -y && dnf install vim -y && dnf clean all

COPY requirements.txt $WORKDIR/
RUN source $WORKDIR/venv/bin/activate && \
    pip --python $(which python) install --no-cache-dir -r $WORKDIR/requirements.txt

ARG ARCH=arm64
RUN tar -czf $WORKDIR/venv_arm64.tar.gz \
    --exclude='__pycache__' \
    -C $WORKDIR/venv .

# copy sources
COPY src $WORKDIR/src
RUN cd $WORKDIR/src && zip -r $WORKDIR//src.zip . -x main.py

# copy resources and sql files
COPY res $WORKDIR/res
RUN cd $WORKDIR/res && zip -r $WORKDIR/res.zip .

# copy sciprts
COPY emr-spec.json runner.sh $WORKDIR/

RUN export CURRENT_TIME=$(date) && echo ">> THE DATE OF BUILD: $CURRENT_TIME" >> BUILD_INFO.txt
RUN echo "==============================" >> BUILD_INFO.txt

# Empty ENTRYPOINT for Jenkins docker agent
ENTRYPOINT []

# Use -l option to read `/root/.bashrc`
CMD [ "bash", "-l" ]
