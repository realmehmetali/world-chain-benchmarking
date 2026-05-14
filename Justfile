ssh_opts := "-o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
ssh_key := "-i terraform/benchmark-key.pem"

default:
    @just --list

# Provision the benchmarking instance and wait for cloud-init to finish
up:
    #!/usr/bin/env bash
    set -euo pipefail

    # Short-circuit if terraform state already has a live instance
    if INSTANCE_ID=$(terraform -chdir=terraform output -raw instance_id 2>/dev/null) && [[ -n "$INSTANCE_ID" ]]; then
        STATE=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" \
            --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null || echo "missing")
        if [[ "$STATE" == "running" || "$STATE" == "pending" ]]; then
            echo "==> Instance $INSTANCE_ID is already $STATE. Use 'just ssh' to connect or 'just down' to destroy."
            exit 0
        fi
    fi

    echo "==> Provisioning instance..."
    terraform -chdir=terraform init -upgrade -input=false > /dev/null
    terraform -chdir=terraform apply -auto-approve

    IP=$(terraform -chdir=terraform output -raw public_ip)
    SSH="ssh {{ssh_opts}} {{ssh_key}} ubuntu@$IP"

    echo "==> Waiting for SSH on $IP..."
    until $SSH true 2>/dev/null; do sleep 2; done

    echo "==> Waiting for cloud-init..."
    $SSH 'cloud-init status --wait' > /dev/null 2>&1

    echo "==> Instance ready. Use 'just ssh' to connect."

# Download a chain snapshot to /data/snapshot on the instance
snapshot url:
    #!/usr/bin/env bash
    set -euo pipefail
    IP=$(terraform -chdir=terraform output -raw public_ip)
    SSH="ssh {{ssh_opts}} {{ssh_key}} ubuntu@$IP"
    SCP="scp {{ssh_opts}} {{ssh_key}}"

    echo "==> Deploying download-snapshot.sh to $IP..."
    $SCP scripts/download-snapshot.sh ubuntu@$IP:/tmp/download-snapshot.sh
    $SSH 'chmod +x /tmp/download-snapshot.sh'

    echo "==> Downloading snapshot..."
    $SSH "/tmp/download-snapshot.sh '{{url}}'"

# Compress a remote folder and upload it to S3 as .tar.zst (detached on the instance).
# Second arg may be empty (auto-named), a bare name (e.g. block-203), or a full s3:// uri.
upload remote_path name_or_uri="":
    #!/usr/bin/env bash
    set -euo pipefail
    IP=$(terraform -chdir=terraform output -raw public_ip)
    BUCKET=$(terraform -chdir=terraform output -raw snapshot_bucket)
    SSH="ssh {{ssh_opts}} {{ssh_key}} ubuntu@$IP"
    SSH_TAIL="ssh {{ssh_opts}} -o ServerAliveInterval=60 -o ServerAliveCountMax=10 {{ssh_key}} ubuntu@$IP"
    SCP="scp {{ssh_opts}} {{ssh_key}}"
    LOG=/data/upload.log
    PIDFILE=/data/upload.pid

    if $SSH "test -f $PIDFILE && kill -0 \$(cat $PIDFILE) 2>/dev/null"; then
        PID=$($SSH "cat $PIDFILE")
        echo "error: an upload is already running (pid $PID). Use 'just upload-status' or 'just upload-cancel'." >&2
        exit 1
    fi

    echo "==> Deploying upload-folder.sh to $IP..."
    $SCP scripts/upload-folder.sh ubuntu@$IP:/tmp/upload-folder.sh
    $SSH 'chmod +x /tmp/upload-folder.sh'

    echo "==> Launching detached upload (log: $LOG)..."
    $SSH "DEFAULT_BUCKET='$BUCKET' setsid nohup /tmp/upload-folder.sh '{{remote_path}}' '{{name_or_uri}}' > $LOG 2>&1 < /dev/null & echo \$! > $PIDFILE"

    echo "==> Tailing $LOG (Ctrl-C to stop tailing; the upload keeps running)"
    sleep 1
    $SSH_TAIL "tail -f $LOG"

# Re-attach to the upload log on the instance
upload-status:
    #!/usr/bin/env bash
    set -euo pipefail
    IP=$(terraform -chdir=terraform output -raw public_ip)
    SSH="ssh {{ssh_opts}} -o ServerAliveInterval=60 -o ServerAliveCountMax=10 {{ssh_key}} ubuntu@$IP"
    $SSH "if [[ -f /data/upload.log ]]; then tail -f /data/upload.log; else echo 'no upload log at /data/upload.log'; exit 1; fi"

# Kill an in-flight upload on the instance
upload-cancel:
    #!/usr/bin/env bash
    set -euo pipefail
    IP=$(terraform -chdir=terraform output -raw public_ip)
    SSH="ssh {{ssh_opts}} {{ssh_key}} ubuntu@$IP"
    PIDFILE=/data/upload.pid

    if ! $SSH "test -f $PIDFILE"; then
        echo "no upload in progress ($PIDFILE not found)"
        exit 0
    fi
    PID=$($SSH "cat $PIDFILE")
    echo "==> Killing upload process group (pid $PID) on $IP..."
    $SSH "kill -TERM -- -$PID 2>/dev/null || kill -TERM $PID 2>/dev/null || true; sleep 2; kill -KILL -- -$PID 2>/dev/null || true; rm -f $PIDFILE"
    echo "done"

# Show instance state and cloud-init status
status:
    #!/usr/bin/env bash
    set -euo pipefail
    INSTANCE_ID=$(terraform -chdir=terraform output -raw instance_id)
    STATE=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" \
        --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null || echo "unknown")
    echo "--- Instance: $STATE ---"
    if [[ "$STATE" != "running" ]]; then
        echo "Instance is not running."
        exit 0
    fi
    IP=$(terraform -chdir=terraform output -raw public_ip)
    SSH="ssh {{ssh_opts}} {{ssh_key}} ubuntu@$IP"
    echo "--- cloud-init ---"
    $SSH 'cloud-init status' 2>/dev/null || echo "ssh not ready"

# SSH into the instance
ssh:
    #!/usr/bin/env bash
    set -euo pipefail
    IP=$(terraform -chdir=terraform output -raw public_ip)
    echo "Waiting for SSH on $IP..."
    until ssh {{ssh_opts}} {{ssh_key}} ubuntu@$IP true 2>/dev/null; do sleep 2; done
    ssh {{ssh_opts}} {{ssh_key}} ubuntu@$IP

# Destroy the instance
down:
    terraform -chdir=terraform destroy -auto-approve
