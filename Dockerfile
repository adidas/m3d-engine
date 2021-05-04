FROM amazonlinux:2.0.20210326.0

ARG JAVA_VERSION=1.8.0

# Installing the SBT repo
RUN curl https://bintray.com/sbt/rpm/rpm | tee /etc/yum.repos.d/bintray-sbt-rpm.repo

# Installing system dependencies
RUN yum update -y && \
    yum install -y java-${JAVA_VERSION}-openjdk java-${JAVA_VERSION}-openjdk-devel sbt shadow-utils && \
    yum clean all && \
    rm -rf /var/cache/yum

RUN groupadd -r m3d && \
    useradd -r -g m3d m3d && \
    mkdir -p /home/m3d && \
    chown m3d:m3d /home/m3d
USER m3d

CMD ["/bin/bash"]