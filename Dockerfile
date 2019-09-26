FROM amazonlinux:2.0.20190508

ARG JAVA_VERSION=1.8.0

# Installing the SBT repo
RUN curl https://bintray.com/sbt/rpm/rpm | tee /etc/yum.repos.d/bintray-sbt-rpm.repo

# Installing system dependencies
RUN yum update -y && \
    yum install -y java-${JAVA_VERSION}-openjdk java-${JAVA_VERSION}-openjdk-devel sbt && \
    yum clean all

CMD ["/bin/bash"]
