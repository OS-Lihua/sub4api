# 独立 Kubernetes 测试实例

`sub4api-test.yaml` 在 `sub4api-test` 命名空间内定义：

- 1 个应用 Deployment（1 个 Pod）和 ClusterIP Service，端口 8080。
- 独立 PostgreSQL、Redis，各 1 个 StatefulSet Pod。
- 3 块 20Gi 的持久化云盘，共 60Gi；云盘会产生费用。
- 启动依赖检查及应用启动、就绪、存活探针。

这是单副本测试配置，更新应用时采用 Recreate 策略，会有短暂中断。
未配置公网入口、域名、TLS、数据库高可用或自动备份。
StorageClass 使用目标 ACK 集群已有的 `alicloud-disk-topology-alltype`。
该存储类的回收策略为 Delete，删除 PVC 可能删除对应云盘和数据。

## 准备

为 kubectl 指定目标集群的 kubeconfig，例如：

```bash
export KUBECONFIG=/absolute/path/to/kubeconfig
```

从项目根目录构建并推送镜像（先登录你自己的镜像仓库）：

```bash
docker buildx build --platform linux/amd64 \
  -t YOUR_REGISTRY/sub4api:YOUR_VERSION --push .
```

当前 YAML 使用已推送的 `matrix-registry.cn-shanghai.cr.aliyuncs.com/mosshub/sub6api:latest`，
集群内通过同一仓库的 `matrix-registry-vpc.cn-shanghai.cr.aliyuncs.com` 地址拉取，
并固定到 SHA256 摘要，避免 latest 后续更新导致部署版本改变。更新应用时应替换镜像地址及摘要。
PostgreSQL 和 Redis 使用集群现有 ACR 仓库中的镜像，各自启动独立数据库实例。

YAML 已引用集群现有的镜像拉取 Secret 名称。部署前需要确保这些凭据在
`sub4api-test` 命名空间内存在且有效；凭据更新后也需要保持该命名空间内的副本有效。
不要将仓库密码或 Secret 内容写入 Git。

## 初始化密钥

首次运行下面命令生成本机私密文件。文件已存在时会报错退出，防止意外重置密钥。
管理员密码和 JWT、TOTP 密钥需要持久保存，不要提交到仓库。

```bash
python3 - <<'PY'
import os
import secrets
from pathlib import Path

directory = Path.home() / '.config' / 'sub4api-test'
directory.mkdir(parents=True, exist_ok=True, mode=0o700)
path = directory / 'runtime.env'
values = {key: secrets.token_hex(32) for key in (
    'DATABASE_PASSWORD', 'REDIS_PASSWORD', 'JWT_SECRET', 'TOTP_ENCRYPTION_KEY',
    'ADMIN_PASSWORD',
)}
values['ADMIN_EMAIL'] = 'admin@sub4api.local'
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, 'w') as output:
    output.write(''.join(f'{key}={value}\n' for key, value in values.items()))
print(f'Credentials saved to {path}')
PY
```

## 部署与验证

确认镜像地址和私有仓库拉取配置已填写，再执行：

```bash
kubectl create namespace sub4api-test --dry-run=client -o yaml | kubectl apply -f -
kubectl -n sub4api-test create secret generic sub4api-secrets \
  --from-env-file="$HOME/.config/sub4api-test/runtime.env" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f deploy/k8s/sub4api-test.yaml
kubectl -n sub4api-test rollout status statefulset/postgres --timeout=10m
kubectl -n sub4api-test rollout status statefulset/redis --timeout=10m
kubectl -n sub4api-test rollout status deployment/sub4api --timeout=15m
kubectl -n sub4api-test get pods,pvc,service
kubectl -n sub4api-test port-forward --address=127.0.0.1 service/sub4api 18081:8080
```

端口转发运行期间，访问 `http://127.0.0.1:18081`；健康检查为 `/health`。
首次管理员账号来自上述私密文件。应用升级前备份数据库：启动时会执行迁移，
回退镜像不能撤销数据库迁移。

## 已部署状态（2026-10-20）

- 命名空间：`sub4api-test`。
- 应用：`deployment/sub4api`，1 个副本。
- 数据库：`statefulset/postgres`、`statefulset/redis`，各 1 个副本。
- 集群内入口：`sub4api.sub4api-test.svc.cluster.local:8080`。
- 镜像摘要：`sha256:c339ac324e4d5629adcfdb8a21996dd35b8a98e1eb28c5dea1a86d5587039aae`。
- 三块持久化云盘均已绑定，各 20Gi。
- 已验证健康检查、前端页面和静态资源、管理员登录、认证接口及公开设置接口。
- 管理员邮箱：`admin@sub4api.local`；密码保存在本机
  `~/.config/sub4api-test/runtime.env`，不在此仓库中。

目前通过 ClusterIP 和本机端口转发访问，尚未配置公网域名。
新实例没有配置上游模型账号，实际模型请求需要先在管理后台添加账号和分组。

## 本机端口转发自动恢复（2026-10-21）

本机现由 macOS LaunchAgent 管理端口转发，监听 `127.0.0.1:18081`。
任务名称为 `com.sub4api.test-port-forward`，登录后启动，转发进程退出时自动重启。
已经通过主动终止转发进程的方式验证自动恢复，恢复后健康检查及页面均返回 HTTP 200。
电脑休眠、断网或集群连接不可用期间仍会暂时无法访问。

本机文件：

- `~/Library/LaunchAgents/com.sub4api.test-port-forward.plist`：启动配置。
- `~/.config/sub4api-test/kubeconfig`：私密集群连接配置，权限 600，不提交 Git。
- `~/.config/sub4api-test/port-forward.log`：转发日志。

检查状态或停止本机转发：

```bash
launchctl print "gui/$(id -u)/com.sub4api.test-port-forward"
launchctl bootout "gui/$(id -u)/com.sub4api.test-port-forward"
```

停止后需要恢复时：

```bash
launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/com.sub4api.test-port-forward.plist"
```

LaunchAgent 正在运行时，不要再执行上面的手动 port-forward 命令，否则会发生端口占用。
