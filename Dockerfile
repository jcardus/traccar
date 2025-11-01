ARG ARCH=x86_64
FROM public.ecr.aws/lambda/java:21-$ARCH

COPY traccar.xml ${LAMBDA_TASK_ROOT}/traccar.xml
COPY traccar/target/tracker-server.jar ${LAMBDA_TASK_ROOT}/lib/
COPY traccar/target/lib/ ${LAMBDA_TASK_ROOT}/lib/
COPY traccar/traccar-web/build ${LAMBDA_TASK_ROOT}/traccar-web

CMD ["Handler::handleRequest"]
