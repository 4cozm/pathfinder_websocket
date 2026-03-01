#!/bin/sh
set -eu

TMP="/var/www/html/pathfinder/tmp"

# tmp 볼륨이 마운트되어 있어야 함 (pf_tmp)
# 없으면 만들어준다 (볼륨이면 생성됨)
mkdir -p "$TMP" || true
mkdir -p "$TMP/standalone_ticket" || true
mkdir -p "$TMP/standalone_ticket_nonce" || true
mkdir -p "$TMP/standalone_nonce" || true

# 권한 보정: www-data/nginx/php-fpm 등이 파일 생성 가능해야 함
# (볼륨은 Dockerfile RUN chmod가 안 먹으니 런타임에 1회 보정)
chmod 777 "$TMP" || true
chmod 777 "$TMP/standalone_ticket" || true
chmod 777 "$TMP/standalone_ticket_nonce" || true
chmod 777 "$TMP/standalone_nonce" || true

# 혹시 기존에 root:root 755로 생긴 파일/폴더가 있으면 같이 풀어줌
chmod -R 777 "$TMP/standalone_ticket" || true
chmod -R 777 "$TMP/standalone_ticket_nonce" || true
chmod -R 777 "$TMP/standalone_nonce" || true

# 원래 실행해야 할 커맨드 실행
exec "$@"