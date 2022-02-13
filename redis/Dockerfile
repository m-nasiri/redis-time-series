FROM redis:6.2.6

ENV REDIS_MASTER_PASSWORD="a-very-complex-password-here" \
    REDIS_MASTER_PORT=6379 

COPY redis.conf /etc/redis/redis.conf
COPY redistimeseries.so /etc/redis/redistimeseries.so

RUN chown redis:redis /etc/redis/redis.conf && \
    chown redis:redis /etc/redis/redistimeseries.so && \
    chmod +x /etc/redis/redistimeseries.so

EXPOSE 6379

VOLUME /etc/redis

ENTRYPOINT ["redis-server", "/etc/redis/redis.conf"]
