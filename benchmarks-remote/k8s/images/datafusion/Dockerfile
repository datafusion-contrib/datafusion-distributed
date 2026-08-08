FROM public.ecr.aws/amazonlinux/amazonlinux:2023-minimal

RUN dnf install -y ca-certificates \
    && dnf clean all

COPY worker /usr/local/bin/worker
RUN chmod 0755 /usr/local/bin/worker

EXPOSE 9000 9001
ENTRYPOINT ["/usr/local/bin/worker"]
