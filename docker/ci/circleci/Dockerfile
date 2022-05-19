#while using circle we'll use circle's base image.
FROM cimg/base:2022.05@sha256:087e0c2f4f3702c9080b726e240ec4e649eb3242cc2b453c8a49476a40bc50d1  AS setup_ci

WORKDIR /diem
COPY rust-toolchain /diem/rust-toolchain
COPY cargo-toolchain /diem/cargo-toolchain
COPY scripts/dev_setup.sh /diem/scripts/dev_setup.sh

#Batch mode and all operations tooling
RUN scripts/dev_setup.sh -t -o -b -p -y
USER root
RUN apt-get clean && rm -rf /var/lib/apt/lists/*
USER circleci
ENV PATH "/home/circleci/.cargo/bin:$PATH"

FROM setup_ci as tested_ci

#Compile a small rust tool?  But we already have in dev_setup (sccache/grcov)...?
#Test that all commands we need are installed and on the PATH
RUN [ -x "$(command -v shellcheck)" ] \
    && [ -x "$(command -v hadolint)" ] \
    && [ -x "$(command -v vault)" ] \
    && [ -x "$(command -v terraform)" ] \
    && [ -x "$(command -v kubectl)" ] \
    && [ -x "$(command -v rustup)" ] \
    && [ -x "$(command -v cargo)" ] \
    && [ -x "$(command -v sccache)" ] \
    && [ -x "$(command -v grcov)" ] \
    && [ -x "$(command -v helm)" ] \
    && [ -x "$(command -v aws)" ] \
    && [ -x "$(command -v z3)" ] \
    && [ -x "$(command -v "$HOME/.dotnet/tools/boogie")" ] \
    && [ -x "$(xargs rustup which cargo --toolchain < /diem/rust-toolchain )" ] \
    && [ -x "$(xargs rustup which cargo --toolchain < /diem/cargo-toolchain)" ]

#should be a no-op
# sccache builds fine, but is not executable ??? in alpine, ends up being recompiled.  Wierd.
RUN scripts/dev_setup.sh -b -p

FROM setup_ci as build_environment
