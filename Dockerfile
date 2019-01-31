FROM apache/zeppelin:0.7.3
ADD . /tmp/coin
RUN cd /tmp/coin && \
    cp -r coin/resources/2D6286V1Z /zeppelin/notebook/ && \
    python setup.py install
