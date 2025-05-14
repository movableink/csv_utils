FROM public.ecr.aws/docker/library/ruby:3.4.2-bullseye

# Install dependencies for clang-16
RUN apt-get update && apt-get install -y curl libclang-16-dev git-core

ENV LIBCLANG_PATH=/usr/lib/llvm-16/lib

# Install rustup and the latest stable Rust
RUN curl -sSf https://sh.rustup.rs > rustup-init.sh && sh rustup-init.sh -y

ENV PATH /root/.cargo/bin:$PATH

RUN gem install bundler

RUN cargo install --locked samply

ADD . /app

WORKDIR /app

RUN bundle install

RUN bin/rake compile