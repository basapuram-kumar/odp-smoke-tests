#!/usr/bin/env python3
"""
Generic Ambari installer/setup for Kafka Connect, MirrorMaker2, and Cruise Control.

Supports classic KAFKA (kafka2) and KAFKA3 using the same flow used on:
  - kafkaupg  (Kafka2): Connect + MM2 + Cruise Control
  - k3upg     (Kafka3): Connect + MM2 + Cruise Control3

Usage:
  python3 setup_kafka_connect_mm2_cc.py --config setup_kafka_connect_mm2_cc.example.json

  # Override Ambari URL and MM2 destination from CLI
  python3 setup_kafka_connect_mm2_cc.py \\
    --config my.json \\
    --ambari-url http://10.101.11.23:8080 \\
    --mm2-dest rl8kmm2n1:6667 \\
    --flavor kafka

  python3 setup_kafka_connect_mm2_cc.py --config my.json --dry-run
"""

from __future__ import print_function

import argparse
import base64
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request


# ---------------------------------------------------------------------------
# Flavor profiles
# ---------------------------------------------------------------------------

PROFILES = {
    "kafka": {
        "service": "KAFKA",
        "broker_component": "KAFKA_BROKER",
        "connect_component": "KAFKA_CONNECT",
        "mm2_component": "KAFKA_MIRRORMAKER",
        "cc_component": "CRUISE_CONTROL",
        "broker_config": "kafka-broker",
        "connect_config": "kafka-connect-distributed",
        "mm2_config": "kafka-mirrormaker2",
        "cc_config": "cruise-control",
        "env_config": "kafka-env",
        "ranger_plugin_config": "ranger-kafka-plugin-properties",
        "ranger_enabled_key": "ranger-kafka-plugin-enabled",
        "default_broker_port": 6667,
        "default_connect_port": 8083,
        "default_cc_port": 9095,
        "jaas_path": "/usr/odp/current/kafka-broker/config/kafka_jaas.conf",
        "mm2_jaas_path": "/usr/odp/current/kafka-broker/config/kafka_mirrormaker2_jaas.conf",
        "authorizer_class": "kafka.security.auth.SimpleAclAuthorizer",
        "kraft_configs": [],
        "connect_uses_listeners": False,
    },
    "kafka3": {
        "service": "KAFKA3",
        "broker_component": "KAFKA3_BROKER",
        "connect_component": "KAFKA3_CONNECT",
        "mm2_component": "KAFKA3_MIRRORMAKER",
        "cc_component": "CRUISE_CONTROL3",
        "broker_config": "kafka3-broker",
        "connect_config": "kafka3-connect-distributed",
        "mm2_config": "kafka3-mirrormaker2",
        "cc_config": "cruise-control3",
        "env_config": "kafka3-env",
        "ranger_plugin_config": "ranger-kafka3-plugin-properties",
        "ranger_enabled_key": "ranger-kafka3-plugin-enabled",
        "default_broker_port": 6669,
        "default_connect_port": 38083,
        "default_cc_port": 9096,
        "jaas_path": "/usr/odp/current/kafka3-broker/config/kafka3_jaas.conf",
        "mm2_jaas_path": "/usr/odp/current/kafka3-broker/config/kafka3_mirrormaker2_jaas.conf",
        "authorizer_class": "kafka.security.authorizer.AclAuthorizer",
        "kraft_configs": ["kraft-broker", "kraft-controller"],
        "connect_uses_listeners": True,
    },
}


def log(msg):
    print("[INFO] %s" % msg)


def warn(msg):
    print("[WARN] %s" % msg)


def die(msg, code=1):
    print("[ERROR] %s" % msg, file=sys.stderr)
    sys.exit(code)


# ---------------------------------------------------------------------------
# Ambari client
# ---------------------------------------------------------------------------

class AmbariClient(object):
    def __init__(self, url, username, password, verify_ssl=False):
        self.base = url.rstrip("/")
        self.auth = base64.b64encode(("%s:%s" % (username, password)).encode()).decode()
        self.verify_ssl = verify_ssl

    def request(self, method, path, body=None):
        url = self.base + path
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", "Basic %s" % self.auth)
        req.add_header("X-Requested-By", "ambari")
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                raw = resp.read().decode()
                return resp.status, (json.loads(raw) if raw else {})
        except urllib.error.HTTPError as e:
            err = e.read().decode()
            return e.code, err

    def get_json(self, path):
        st, body = self.request("GET", path)
        if st != 200:
            die("GET %s failed (%s): %s" % (path, st, str(body)[:500]))
        return body


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_config(path):
    with open(path) as f:
        return json.load(f)


def deep_get(cfg, *keys, default=None):
    cur = cfg
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def expand_path(path):
    if not path:
        return path
    return os.path.expanduser(path)


def run_ssh(user, key, host, remote_cmd, dry_run=False):
    key = expand_path(key)
    cmd = [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "ConnectTimeout=20",
    ]
    if key:
        cmd.extend(["-i", key])
    cmd.append("%s@%s" % (user, host))
    cmd.append(remote_cmd)
    log("SSH %s@%s : %s" % (user, host, remote_cmd[:120]))
    if dry_run:
        return 0, ""
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    return p.returncode, p.stdout


def scp_file(user, key, src, dest_host, dest_path, dry_run=False):
    key = expand_path(key)
    cmd = [
        "scp",
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
    ]
    if key:
        cmd.extend(["-i", key])
    cmd.extend([src, "%s@%s:%s" % (user, dest_host, dest_path)])
    log("SCP %s -> %s@%s:%s" % (src, user, dest_host, dest_path))
    if dry_run:
        return 0
    return subprocess.run(cmd).returncode


def detect_flavor(ambari, cluster):
    services = ambari.get_json(
        "/api/v1/clusters/%s/services?fields=ServiceInfo/service_name" % cluster
    )
    names = [i["ServiceInfo"]["service_name"] for i in services.get("items", [])]
    has_k3 = "KAFKA3" in names
    has_k2 = "KAFKA" in names
    if has_k3 and not has_k2:
        return "kafka3"
    if has_k2 and not has_k3:
        return "kafka"
    if has_k3 and has_k2:
        die("Both KAFKA and KAFKA3 are installed. Set kafka.flavor explicitly to kafka or kafka3.")
    die("Neither KAFKA nor KAFKA3 service found on cluster %s" % cluster)


def resolve_cluster(ambari, configured):
    if configured:
        return configured
    data = ambari.get_json("/api/v1/clusters")
    items = data.get("items", [])
    if not items:
        die("No clusters found on Ambari")
    if len(items) > 1:
        names = [i["Clusters"]["cluster_name"] for i in items]
        die("Multiple clusters found (%s). Set ambari.cluster_name in config." % ", ".join(names))
    return items[0]["Clusters"]["cluster_name"]


def resolve_host(ambari, cluster):
    data = ambari.get_json(
        "/api/v1/clusters/%s/hosts?fields=Hosts/host_name,Hosts/ip" % cluster
    )
    items = data.get("items", [])
    if not items:
        die("No hosts found in cluster %s" % cluster)
    if len(items) > 1:
        warn("Multi-host cluster detected; using first host for component placement: %s"
             % items[0]["Hosts"]["host_name"])
    h = items[0]["Hosts"]
    return h["host_name"], h.get("ip", "")


def current_service_configs(ambari, cluster, service):
    data = ambari.get_json(
        "/api/v1/clusters/%s/configurations/service_config_versions"
        "?service_name=%s&is_current=true" % (cluster, service)
    )
    out = {}
    for item in data.get("items", []):
        for cfg in item.get("configurations", []):
            out[cfg["type"]] = {
                "properties": dict(cfg.get("properties", {})),
                "properties_attributes": cfg.get("properties_attributes", {}),
            }
    return out


def ensure_component(ambari, cluster, service, host, component, dry_run=False):
    st, _ = ambari.request(
        "GET",
        "/api/v1/clusters/%s/services/%s/components/%s" % (cluster, service, component),
    )
    if st == 404:
        log("Creating service component %s" % component)
        if not dry_run:
            st, body = ambari.request(
                "POST",
                "/api/v1/clusters/%s/services/%s/components/%s" % (cluster, service, component),
                {"components": [{"ServiceComponentInfo": {"component_name": component}}]},
            )
            if st not in (200, 201):
                die("Failed creating service component %s: %s %s" % (component, st, body))
    else:
        log("Service component exists: %s" % component)

    st, _ = ambari.request(
        "GET",
        "/api/v1/clusters/%s/hosts/%s/host_components/%s" % (cluster, host, component),
    )
    if st == 404:
        log("Creating host component %s on %s" % (component, host))
        if not dry_run:
            st, body = ambari.request(
                "POST",
                "/api/v1/clusters/%s/hosts/%s/host_components/%s" % (cluster, host, component),
                {"host_components": [{"HostRoles": {"component_name": component}}]},
            )
            if st not in (200, 201):
                die("Failed creating host component %s: %s %s" % (component, st, body))
    else:
        log("Host component exists: %s" % component)


def wait_request(ambari, cluster, request_id, timeout_sec):
    log("Waiting Ambari request %s ..." % request_id)
    start = time.time()
    while time.time() - start < timeout_sec:
        body = ambari.get_json(
            "/api/v1/clusters/%s/requests/%s"
            "?fields=Requests/request_status,Requests/progress_percent,"
            "Requests/failed_task_count" % (cluster, request_id)
        )
        r = body["Requests"]
        status = r["request_status"]
        log("  status=%s progress=%s%% failed=%s"
            % (status, r.get("progress_percent"), r.get("failed_task_count")))
        if status in ("COMPLETED", "FAILED", "TIMEDOUT", "ABORTED"):
            if status != "COMPLETED":
                dump_failed_tasks(ambari, cluster, request_id)
            return status
        time.sleep(8)
    return "TIMEOUT"


def dump_failed_tasks(ambari, cluster, request_id):
    st, tasks = ambari.request(
        "GET",
        "/api/v1/clusters/%s/requests/%s/tasks"
        "?fields=Tasks/id,Tasks/status,Tasks/stderr,Tasks/stdout,Tasks/command_detail"
        % (cluster, request_id),
    )
    if st != 200 or not isinstance(tasks, dict):
        return
    for item in tasks.get("items", []):
        t = item["Tasks"]
        if t.get("status") == "COMPLETED":
            continue
        warn("Failed task %s %s" % (t.get("id"), t.get("command_detail")))
        err = (t.get("stderr") or "")[-2000:]
        if err:
            print(err)


def set_host_component_state(ambari, cluster, host, component, state, context, timeout_sec, dry_run=False):
    log("%s -> %s" % (context, state))
    if dry_run:
        return "SKIPPED"
    st, resp = ambari.request(
        "PUT",
        "/api/v1/clusters/%s/hosts/%s/host_components/%s" % (cluster, host, component),
        {
            "RequestInfo": {"context": context},
            "HostRoles": {"state": state},
        },
    )
    if st in (200,):
        return "NOOP"
    if st not in (202,) or not isinstance(resp, dict) or "Requests" not in resp:
        die("Failed %s (http=%s): %s" % (context, st, str(resp)[:500]))
    rid = resp["Requests"]["id"]
    status = wait_request(ambari, cluster, rid, timeout_sec)
    if status != "COMPLETED":
        die("%s finished with status %s" % (context, status))
    return status


def apply_desired_configs(ambari, cluster, desired, note, dry_run=False):
    tag = "version%s" % int(time.time() * 1000)
    payload = []
    for i, item in enumerate(desired):
        entry = {
            "type": item["type"],
            "tag": "%s-%s" % (tag, i),
            "service_config_version_note": note,
            "properties": item["properties"],
        }
        if item.get("properties_attributes"):
            entry["properties_attributes"] = item["properties_attributes"]
        payload.append(entry)
        log("Will update config type %s (%s props)" % (item["type"], len(item["properties"])))
    if dry_run:
        return
    st, body = ambari.request(
        "PUT",
        "/api/v1/clusters/%s" % cluster,
        {"Clusters": {"desired_config": payload}},
    )
    if st != 200:
        die("Config update failed (%s): %s" % (st, str(body)[:800]))
    log("Configs applied OK")


def build_mm2_content(profile, host, broker_port, mm2_cfg, realm="ADSRE.COM"):
    src = mm2_cfg.get("source_cluster_alias") or host
    dest = mm2_cfg.get("dest_cluster_alias") or "dest"
    dest_bs = mm2_cfg["dest_bootstrap_servers"]
    dest_proto = mm2_cfg.get("dest_security_protocol") or "PLAINTEXT"
    src_proto = mm2_cfg.get("source_security_protocol") or "SASL_PLAINTEXT"
    topics = mm2_cfg.get("topics") or ".*"
    src_to_dest = "true" if mm2_cfg.get("enable_source_to_dest", True) else "false"
    dest_to_src = "true" if mm2_cfg.get("enable_dest_to_source", False) else "false"
    jaas = profile["mm2_jaas_path"]
    rf = str(mm2_cfg.get("replication_factor") or 1)

    return """

            # Generated by setup_kafka_connect_mm2_cc.py
            clusters={src},{dest}

            {src}->{dest}.enabled={src_to_dest}
            {src}->{dest}.topics={topics}
            {src}.bootstrap.servers={host}:{port}
            {src}.java.security.auth.login.config={jaas}
            {src}.java.security.krb5.kdc={host}
            {src}.java.security.krb5.realm={realm}
            {src}.javax.security.auth.useSubjectCredsOnly=true
            {src}.replication.enabled=true
            {src}.sasl.enabled.mechanisms=GSSAPI
            {src}.sasl.kerberos.service.name=kafka
            {src}.security.protocol={src_proto}
            {src}.sun.security.krb5.debug=true
            {src}.topics.whitelist={topics}

            {dest}->{src}.enabled={dest_to_src}
            {dest}->{src}.topics={topics}
            {dest}.bootstrap.servers={dest_bs}
            {dest}.destination.cluster.name={dest}
            {dest}.java.security.auth.login.config={jaas}
            {dest}.java.security.krb5.kdc=
            {dest}.java.security.krb5.realm=
            {dest}.javax.security.auth.useSubjectCredsOnly=
            {dest}.replication.enabled=false
            {dest}.sasl.enabled.mechanisms=GSSAPI
            {dest}.sasl.kerberos.service.name=kafka
            {dest}.security.protocol={dest_proto}
            {dest}.ssl.key.password=
            {dest}.ssl.keystore.location=
            {dest}.ssl.keystore.password=
            {dest}.ssl.truststore.location=
            {dest}.ssl.truststore.password=
            {dest}.sun.security.krb5.debug=true
            {dest}.topics.whitelist={topics}

            refresh.topics.enabled=true
            refresh.topics.interval.seconds=5
            replication.factor={rf}
            status.storage.replication.factor={rf}
            heartbeats.topic.replication.factor={rf}
            offset-syncs.topic.replication.factor={rf}
            offset.storage.replication.factor={rf}
            config.storage.replication.factor={rf}
            checkpoints.topic.replication.factor={rf}
            config.providers=env
            config.providers.env.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider
""".format(
        src=src,
        dest=dest,
        src_to_dest=src_to_dest,
        dest_to_src=dest_to_src,
        topics=topics,
        host=host,
        port=broker_port,
        jaas=jaas,
        src_proto=src_proto,
        dest_bs=dest_bs,
        dest_proto=dest_proto,
        realm=realm,
        rf=rf,
    )


def patch_configs(cfgs, profile, host, cfg):
    """Mutate config maps in-place; return list of desired config dicts to apply."""
    kafka_cfg = cfg.get("kafka", {})
    mm2_cfg = dict(cfg.get("mm2", {}))
    connect_cfg = cfg.get("connect", {})
    cc_cfg = cfg.get("cruise_control", {})
    realm = kafka_cfg.get("kerberos_realm") or "ADSRE.COM"

    rf = str(kafka_cfg.get("replication_factor", 1))
    min_isr = str(kafka_cfg.get("min_insync_replicas", 1))
    broker_port = kafka_cfg.get("broker_port") or profile["default_broker_port"]
    connect_port = connect_cfg.get("rest_port") or profile["default_connect_port"]
    cc_port = cc_cfg.get("webserver_http_port") or profile["default_cc_port"]
    authorizer = kafka_cfg.get("authorizer_class") or profile["authorizer_class"]
    mm2_cfg["replication_factor"] = mm2_cfg.get("replication_factor") or rf

    desired = []

    # kafka-env / kafka3-env: MM2 keytab/principal (reuse broker keytab)
    env_type = profile["env_config"]
    if env_type in cfgs:
        env = cfgs[env_type]["properties"]
        keytab_key = "kafka_mirrormaker2_keytab"
        princ_key = "kafka_mirrormaker2_principal_name"
        if "kafka_keytab" in env:
            env[keytab_key] = env.get(keytab_key) or env["kafka_keytab"]
        else:
            env[keytab_key] = env.get(keytab_key) or "/etc/security/keytabs/kafka.service.keytab"
        if "kafka_principal_name" in env:
            env[princ_key] = env.get(princ_key) or env["kafka_principal_name"]
        else:
            env[princ_key] = env.get(princ_key) or ("kafka/_HOST@%s" % realm)
        desired.append({"type": env_type, "properties": env,
                        "properties_attributes": cfgs[env_type].get("properties_attributes", {})})

    # broker (+ optional kraft)
    broker_types = [profile["broker_config"]] + list(profile.get("kraft_configs") or [])
    for btype in broker_types:
        if btype not in cfgs:
            continue
        props = cfgs[btype]["properties"]
        props["default.replication.factor"] = rf
        props["min.insync.replicas"] = min_isr
        props["offsets.topic.replication.factor"] = rf
        props["transaction.state.log.replication.factor"] = rf
        props["transaction.state.log.min.isr"] = min_isr
        if btype == profile["broker_config"] and kafka_cfg.get("disable_ranger_plugin", True):
            props["authorizer.class.name"] = authorizer
            props["cruise.control.metrics.reporter.authorizer.class.name"] = ""
        props["cruise.control.metrics.reporter.bootstrap.servers"] = "%s:%s" % (host, broker_port)
        desired.append({"type": btype, "properties": props,
                        "properties_attributes": cfgs[btype].get("properties_attributes", {})})

    # connect
    ctype = profile["connect_config"]
    if ctype in cfgs:
        props = cfgs[ctype]["properties"]
        props["bootstrap.servers"] = "%s:%s" % (host, broker_port)
        props["consumer.bootstrap.servers"] = "%s:%s" % (host, broker_port)
        props["producer.bootstrap.servers"] = "%s:%s" % (host, broker_port)
        props["security.protocol"] = "SASL_PLAINTEXT"
        props["consumer.security.protocol"] = "SASL_PLAINTEXT"
        props["producer.security.protocol"] = "SASL_PLAINTEXT"
        props["security.inter.broker.protocol"] = "SASL_PLAINTEXT"
        props["java.security.krb5.kdc"] = host
        props["java.security.krb5.realm"] = realm
        props["java.security.auth.login.config"] = profile["jaas_path"]
        props["javax.security.auth.useSubjectCredsOnly"] = "true"
        props["sasl.enabled.mechanisms"] = "GSSAPI"
        props["sasl.mechanism"] = "GSSAPI"
        props["sasl.kerberos.service.name"] = "kafka"
        props["config.storage.replication.factor"] = rf
        props["offset.storage.replication.factor"] = rf
        props["status.storage.replication.factor"] = rf
        if profile["connect_uses_listeners"]:
            props["listeners"] = "HTTP://localhost:%s" % connect_port
            props["rest.advertised.listener"] = "HTTP"
        else:
            props["rest.port"] = str(connect_port)
        desired.append({"type": ctype, "properties": props,
                        "properties_attributes": cfgs[ctype].get("properties_attributes", {})})

    # mm2
    mtype = profile["mm2_config"]
    if mtype in cfgs:
        props = dict(cfgs[mtype]["properties"])
        props["content"] = build_mm2_content(profile, host, broker_port, mm2_cfg, realm=realm)
        desired.append({"type": mtype, "properties": props,
                        "properties_attributes": cfgs[mtype].get("properties_attributes", {})})

    # cruise control
    cctype = profile["cc_config"]
    if cctype in cfgs:
        props = cfgs[cctype]["properties"]
        props["bootstrap.servers"] = "%s:%s" % (host, broker_port)
        if profile["service"] == "KAFKA3":
            props["zookeeper.connect"] = "%s:2181/kafka3" % host
        else:
            props["zookeeper.connect"] = "%s:2181" % host
        props["zookeeper.security.enabled"] = "true"
        props["security.protocol"] = "SASL_PLAINTEXT"
        props["security.inter.broker.protocol"] = "SASL_PLAINTEXT"
        props["sample.store.topic.replication.factor"] = rf
        if "webserver.http.port" in props:
            props["webserver.http.port"] = str(cc_port)
        desired.append({"type": cctype, "properties": props,
                        "properties_attributes": cfgs[cctype].get("properties_attributes", {})})

    # ranger plugin off when requested (clusters without Ranger)
    rtype = profile["ranger_plugin_config"]
    if kafka_cfg.get("disable_ranger_plugin", True) and rtype in cfgs:
        props = cfgs[rtype]["properties"]
        key = profile["ranger_enabled_key"]
        # Preserve existing capitalization style when possible
        cur = str(props.get(key, "no"))
        props[key] = "No" if cur[:1].isupper() else "no"
        if "zookeeper.connect" in props:
            if profile["service"] == "KAFKA3":
                props["zookeeper.connect"] = "%s:2181/kafka3" % host
            else:
                props["zookeeper.connect"] = "%s:2181" % host
        desired.append({"type": rtype, "properties": props,
                        "properties_attributes": cfgs[rtype].get("properties_attributes", {})})

    return desired, broker_port


def host_prep(cfg, ambari_host_ip, dry_run=False):
    hs = cfg.get("host_setup", {})
    if not hs.get("enabled", True):
        log("host_setup.enabled=false; skipping SSH host prep")
        return

    user = hs.get("ssh_user") or "acceldata"
    key = hs.get("ssh_key") or ""
    host = hs.get("ssh_host") or ambari_host_ip
    if not host:
        warn("No SSH host available; skip host prep")
        return

    # /etc/hosts entry for MM2 dest
    entry = (hs.get("dest_hosts_entry") or "").strip()
    if entry:
        hostname = entry.split()[1] if len(entry.split()) > 1 else ""
        script = (
            "if ! grep -q '%s' /etc/hosts 2>/dev/null; then "
            "echo '%s' | sudo tee -a /etc/hosts >/dev/null; fi; "
            "getent hosts %s || true"
            % (hostname, entry, hostname)
        )
        rc, out = run_ssh(user, key, host, script, dry_run=dry_run)
        if rc != 0:
            warn("Failed adding hosts entry (rc=%s): %s" % (rc, out[-300:]))
        else:
            log("Hosts entry OK: %s" % entry)

    # CredentialUtil.jar for cruise-control-env SSL password lookups
    src_host = (hs.get("copy_credential_util_from") or "").strip()
    if src_host:
        local_tmp = "/tmp/CredentialUtil.jar.%s" % os.getpid()
        remote_jar = "/var/lib/ambari-agent/cred/lib/CredentialUtil.jar"
        fetch = [
            "scp", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
        ]
        if key:
            fetch.extend(["-i", expand_path(key)])
        fetch.extend(["%s@%s:%s" % (user, src_host, remote_jar), local_tmp])
        log("Fetching CredentialUtil.jar from %s" % src_host)
        if dry_run:
            log("DRY-RUN: would copy CredentialUtil.jar from %s to %s" % (src_host, host))
        else:
            r = subprocess.run(fetch, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            if r.returncode != 0:
                warn("Could not fetch CredentialUtil.jar: %s" % r.stdout[-300:])
            else:
                scp_file(user, key, local_tmp, host, "/tmp/CredentialUtil.jar", dry_run=False)
                run_ssh(
                    user, key, host,
                    "sudo mkdir -p /var/lib/ambari-agent/cred/lib && "
                    "sudo cp /tmp/CredentialUtil.jar %s && sudo chmod 755 %s && ls -la %s"
                    % (remote_jar, remote_jar, remote_jar),
                    dry_run=False,
                )
                try:
                    os.remove(local_tmp)
                except OSError:
                    pass


def print_final_states(ambari, cluster, service):
    body = ambari.get_json(
        "/api/v1/clusters/%s/services/%s/components"
        "?fields=ServiceComponentInfo/component_name,host_components/HostRoles/state"
        % (cluster, service)
    )
    log("Final component states:")
    for item in body.get("items", []):
        name = item["ServiceComponentInfo"]["component_name"]
        hcs = item.get("host_components") or []
        state = hcs[0]["HostRoles"]["state"] if hcs else "?"
        print("  %s: %s" % (name, state))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Install/configure Kafka(2|3) Connect, MirrorMaker2, and Cruise Control via Ambari"
    )
    p.add_argument("--config", required=True, help="Path to JSON config file")
    p.add_argument("--ambari-url", help="Override ambari.url")
    p.add_argument("--cluster-name", help="Override ambari.cluster_name")
    p.add_argument("--mm2-dest", help="Override mm2.dest_bootstrap_servers (example: rl8kmm2n1:6667)")
    p.add_argument("--flavor", choices=["auto", "kafka", "kafka3"], help="Override kafka.flavor")
    p.add_argument("--ssh-host", help="Override host_setup.ssh_host")
    p.add_argument("--ssh-key", help="Override host_setup.ssh_key")
    p.add_argument("--dry-run", action="store_true", help="Print actions without changing the cluster")
    p.add_argument("--skip-host-setup", action="store_true", help="Skip SSH /etc/hosts and CredentialUtil steps")
    return p.parse_args()


def main():
    args = parse_args()
    cfg = load_config(args.config)

    if args.ambari_url:
        cfg.setdefault("ambari", {})["url"] = args.ambari_url
    if args.cluster_name:
        cfg.setdefault("ambari", {})["cluster_name"] = args.cluster_name
    if args.mm2_dest:
        cfg.setdefault("mm2", {})["dest_bootstrap_servers"] = args.mm2_dest
    if args.flavor:
        cfg.setdefault("kafka", {})["flavor"] = args.flavor
    if args.ssh_host:
        cfg.setdefault("host_setup", {})["ssh_host"] = args.ssh_host
    if args.ssh_key:
        cfg.setdefault("host_setup", {})["ssh_key"] = args.ssh_key
    if args.skip_host_setup:
        cfg.setdefault("host_setup", {})["enabled"] = False

    ambari_cfg = cfg.get("ambari", {})
    url = ambari_cfg.get("url")
    if not url:
        die("ambari.url is required (config or --ambari-url)")
    if not deep_get(cfg, "mm2", "dest_bootstrap_servers"):
        die("mm2.dest_bootstrap_servers is required (config or --mm2-dest)")

    ambari = AmbariClient(
        url,
        ambari_cfg.get("username", "admin"),
        ambari_cfg.get("password", "admin"),
        ambari_cfg.get("verify_ssl", False),
    )

    cluster = resolve_cluster(ambari, ambari_cfg.get("cluster_name") or "")
    host, host_ip = resolve_host(ambari, cluster)
    log("Cluster=%s host=%s ip=%s ambari=%s" % (cluster, host, host_ip, url))

    flavor = (deep_get(cfg, "kafka", "flavor") or "auto").lower()
    if flavor == "auto":
        flavor = detect_flavor(ambari, cluster)
    if flavor not in PROFILES:
        die("Unsupported flavor: %s" % flavor)
    profile = PROFILES[flavor]
    service = profile["service"]
    log("Using flavor=%s service=%s" % (flavor, service))

    ops = cfg.get("operations", {})
    timeout = int(ops.get("wait_timeout_sec", 900))
    dry = args.dry_run

    # Host prep (DNS for dest + CredentialUtil)
    ssh_target = deep_get(cfg, "host_setup", "ssh_host") or host_ip or host
    host_prep(cfg, ssh_target, dry_run=dry)

    # Ensure components exist
    for comp in (profile["connect_component"], profile["mm2_component"], profile["cc_component"]):
        ensure_component(ambari, cluster, service, host, comp, dry_run=dry)

    # Configs
    if ops.get("apply_configs", True):
        cfgs = current_service_configs(ambari, cluster, service)
        desired, broker_port = patch_configs(cfgs, profile, host, cfg)
        note = (
            "setup_kafka_connect_mm2_cc: flavor=%s mm2_dest=%s rf=%s"
            % (flavor, cfg["mm2"]["dest_bootstrap_servers"], deep_get(cfg, "kafka", "replication_factor", default=1))
        )
        apply_desired_configs(ambari, cluster, desired, note, dry_run=dry)
        log("Broker port used for configs: %s" % broker_port)
    else:
        log("Skipping config apply")

    # Install new components
    if ops.get("install_components", True):
        for comp in (profile["connect_component"], profile["mm2_component"], profile["cc_component"]):
            set_host_component_state(
                ambari, cluster, host, comp, "INSTALLED",
                "Install %s" % comp, timeout, dry_run=dry,
            )

    # Restart broker so CC metrics reporter / RF / authorizer take effect
    if ops.get("restart_broker", True):
        set_host_component_state(
            ambari, cluster, host, profile["broker_component"], "INSTALLED",
            "Stop %s" % profile["broker_component"], timeout, dry_run=dry,
        )
        set_host_component_state(
            ambari, cluster, host, profile["broker_component"], "STARTED",
            "Start %s" % profile["broker_component"], timeout, dry_run=dry,
        )

    # Start Connect / MM2 / CC
    if ops.get("start_components", True):
        for comp in (profile["connect_component"], profile["mm2_component"], profile["cc_component"]):
            set_host_component_state(
                ambari, cluster, host, comp, "STARTED",
                "Start %s" % comp, timeout, dry_run=dry,
            )

    if not dry:
        print_final_states(ambari, cluster, service)
    log("Done")


if __name__ == "__main__":
    main()
