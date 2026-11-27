FROM docker.io/library/python:3.12-slim-trixie

COPY --from=ghcr.io/astral-sh/uv:0.11.19 /uv /uvx /usr/local/bin/

# Install OS tools only. uv installs Python dependencies and tooling from
# pyproject.toml/uv.lock; Debian-compatible wheels avoid native library builds.
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        git \
        nodejs \
        npm \
        openssh-server \
        passwd \
        redis-server \
        sudo \
    && rm -rf /var/lib/apt/lists/*

ENV redis_bin_dir=/usr/bin

# Keep the non-root test user and allow the existing CI command to start SSH.
# Unlock key authentication without retaining a fixed account password.
RUN useradd --create-home --shell /bin/bash firex \
    && passwd --delete firex \
    && printf 'firex ALL=(root) NOPASSWD: /usr/sbin/sshd\n' > /etc/sudoers.d/firex \
    && chmod 0440 /etc/sudoers.d/firex \
    && install -d -m 0755 /run/sshd \
    && printf '%s\n' \
        'PubkeyAuthentication yes' \
        'PasswordAuthentication no' \
        'KbdInteractiveAuthentication no' \
        'UsePAM no' \
        'PermitRootLogin no' \
        'AllowUsers firex' \
        > /etc/ssh/sshd_config \
    && ssh-keygen -A

USER firex
ENV HOME=/home/firex
WORKDIR /home/firex

# Intentionally bake test SSH keys into the image for container-local SSH tests.
RUN install -d -m 0700 "$HOME/.ssh" \
    && ssh-keygen -q -t rsa -N '' -f "$HOME/.ssh/id_rsa" \
    && cp "$HOME/.ssh/id_rsa.pub" "$HOME/.ssh/authorized_keys" \
    && chmod 0600 "$HOME/.ssh/authorized_keys"
