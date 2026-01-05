FROM public.ecr.aws/deep-learning-containers/pytorch-training-arm64:2.7.0-gpu-py312-cu128-ubuntu22.04-ec2-v1.30
# FROM public.ecr.aws/deep-learning-containers/pytorch-training:2.7.1-gpu-py312-cu128-ubuntu22.04-ec2-v1.15

ENV WORKDIR=/opt/app
WORKDIR $WORKDIR

# TZ 환경 변수를 설정하여 컨테이너의 시간대를 명시적으로 지정합니다.
# 여기서는 'Asia/Seoul'로 설정했습니다.
ENV TZ=Asia/Seoul

RUN echo "export PS1='\[\e[1;32m\]\u@\h:\[\e[1;34m\]\w\[\e[0m\]\$ '" >> /root/.bashrc && \
    echo "alias ls='ls --color=auto'" >> /root/.bashrc

RUN apt update && apt install ffmpeg -y  && apt clean

ENV AWS_DEFAULT_REGION=ap-northeast-2
ENV AWS_DEFAULT_OUTPUT=json

COPY requirements.txt $WORKDIR/
RUN pip install --no-cache-dir -r requirements.txt

# copy sciprts
COPY runner.sh $WORKDIR/
COPY aws /root/.aws
COPY src $WORKDIR/src
COPY res $WORKDIR/res

RUN export CURRENT_TIME=$(date) && echo ">> THE DATE OF BUILD: $CURRENT_TIME" >> BUILD_INFO.txt

# Entry
ENTRYPOINT []

# CMD
CMD [ "bash", "-l" ]