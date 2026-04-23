# hash:sha256:c2ac09b3a234182826fe903d6596e982cf5c5f6e402bea2600b5ccc1812a9dd2
FROM registry.codeocean.allenneuraldynamics.org/codeocean/mambaforge3:24.5.0-0-python3.12.4-ubuntu22.04

ARG DEBIAN_FRONTEND=noninteractive
ARG GIT_ASKPASS
COPY git-ask-pass /

ENV AWS_REGION=us-west-2
ENV AWS_DEFAULT_REGION=us-west-2

# ---- Install Azul Zulu JDK 8u462 (with JavaFX) ----
ARG AZUL_JDK_URL="https://cdn.azul.com/zulu/bin/zulu8.88.0.19-ca-fx-jdk8.0.462-linux_x64.tar.gz"
ENV JAVA_HOME=/usr/lib/jvm/zulu-8
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN apt-get update \
 && apt-get install -y --no-install-recommends libblosc1 curl jq ca-certificates maven \
 && rm -rf /var/lib/apt/lists/* \
 # Download JDK
 && curl -fsSL "${AZUL_JDK_URL}" -o /tmp/zulu.tar.gz \
 # Determine extracted folder name from the tarball
 && ZULU_DIR="$(tar -tzf /tmp/zulu.tar.gz | head -1 | cut -f1 -d'/')" \
 # Extract to /usr/lib/jvm and create a stable symlink at $JAVA_HOME
 && mkdir -p /usr/lib/jvm \
 && tar -xzf /tmp/zulu.tar.gz -C /usr/lib/jvm \
 && ln -s "/usr/lib/jvm/${ZULU_DIR}" "${JAVA_HOME}" \
 # Register java/javac with update-alternatives
 && update-alternatives --install /usr/bin/java  java  "${JAVA_HOME}/bin/java"  20000 \
 && update-alternatives --install /usr/bin/javac javac "${JAVA_HOME}/bin/javac" 20000 \
 # Cleanup
 && rm -f /tmp/zulu.tar.gz

RUN pip3 install -U --no-cache-dir \
    awscli==1.33.30 \
    xmltodict==0.13.0 \
    zarr==2.18.7 \
    xarray-multiscale==1.2.0 \
    s3fs==2025.7.0 \
    dask==2025.12.0 \ 
    boto3 \
    aind-data-schema==2.6.0

ADD "https://github.com/coder/code-server/releases/download/v4.116.0/code-server-4.116.0-linux-amd64.tar.gz" /.code-server/code-server.tar.gz

RUN cd /.code-server \
    && tar -xvf code-server.tar.gz \
    && rm code-server.tar.gz \
    && ln -s /.code-server/code-server-4.116.0-linux-amd64/bin/code-server /usr/bin/code-server \
    && mkdir -p /.vscode/extensions
    