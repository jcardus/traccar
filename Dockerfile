ARG ARCH=x86_64
FROM public.ecr.aws/lambda/java:21-$ARCH

COPY traccar.xml ${LAMBDA_TASK_ROOT}/traccar.xml
COPY target/tracker-server.jar ${LAMBDA_TASK_ROOT}/lib/
COPY target/lib/ ${LAMBDA_TASK_ROOT}/lib/
COPY traccar-web/build ${LAMBDA_TASK_ROOT}/traccar-web
COPY templates ${LAMBDA_TASK_ROOT}/templates

CMD ["Handler::handleRequest"]
