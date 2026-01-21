from ..common import *
from . import section

@section
def _(outer_panels: Panels):
    panels = outer_panels
    return [
        outer_panels.row_collapsed(
            "Cluster Alerts",
            [
                panels.subheader(
                    "Streaming Alerts",
                    """[Alert Reference]
- Too Many Barriers: there are too many uncommitted barriers generated. This means the streaming graph is stuck.
  Check the following panels to follow-up:
  - Streaming Backfill: Check if there's any throughput in the panels, if yes, backfill is in progress. If throughput is high, it could lead to additional pressure on the stream graph.
  - Storage Alerts: Look at the alerts in the storage section, specifically `Write Stall`. That will cause backpressure and the stream graph being stuck.
  - Cluster Resource: If Relative CPU or Memory usage is high, it can lead to the stream graph being stuck.
  - Barrier Latency: Get the latency of the barrier, it should be high.
  - Streaming Relations: Look at the TopN relations by CPU usage and busy rate. These relations are likely to be the bottleneck.
  - Streaming Operators by Operator: Look at the alerts in the streaming operators by operator section, the following panels are more likely to be the bottleneck:
    - Merger Barrier Align: If the merger barrier align is high, it means the merger is not able to align the barriers in time.
    - Join Amplification: If the join amplification is high, it means the join is not able to process the data in time.
- Recovery Triggered: cluster recovery is triggered. Check 'Errors by Type' / 'Node Count' panels to find the root cause. Check the error logs as well.
""",
                    height=5,
                ),
                panels.timeseries_count(
                    "Streaming Alerts",
                    "",
                    [
                        panels.target(
                            f"({metric('all_barrier_nums')} >= bool 200) > 0",
                            "Too Many Barriers {{database_id}}",
                        ),
                        panels.target(
                            f"((sum(rate({metric('recovery_latency_count')}[$__rate_interval])) by (recovery_type) + "
                            + f"sum(rate({metric('recovery_failure_cnt')}[$__rate_interval])) by (recovery_type)) > bool 0) > 0",
                            "Recovery Triggered {{recovery_type}}",
                        ),
                    ],
                    ["last"],
                ),
                panels.subheader(
                    "Cluster Resource Alerts",
                    """[Alert Reference]
- CPU Saturation: the average CPU utilization per core is too high, and the system may be throttled.
- Unexpected Termination: components are exiting unexpectedly (OOMKilled, Error, etc). Check the termination reasons in the error dashboard.
""",
                    height=4,
                ),
                panels.timeseries_count(
                    "Cluster Resource Alerts",
                    "",
                    [
                        panels.target(
                            f"((sum(rate({metric('process_cpu_seconds_total')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / "
                            + f"avg({metric('process_cpu_core_num')}) by ({COMPONENT_LABEL}, {NODE_LABEL})) > bool 0.9) > 0",
                            "CPU Saturation (avg/core) - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            '(((sum(rate(container_cpu_usage_seconds_total{namespace=~"$namespace",container=~"$component",pod=~"$pod"}[$__rate_interval])) by (namespace, pod)) / '
                            + '(sum(kube_pod_container_resource_limits{namespace=~"$namespace",pod=~"$pod",container=~"$component", resource="cpu"}) by (namespace, pod))) > bool 0.9) > 0',
                            "CPU Saturation (k8s limit) - {{namespace}}/{{pod}}",
                        ),
                        panels.target(
                            "(changes(("
                            + 'kube_pod_container_status_last_terminated_timestamp{cluster=~"$cluster",namespace=~"$namespace",pod=~"$pod"} '
                            + "* on (namespace,pod,container) group_left (reason) "
                            + 'kube_pod_container_status_last_terminated_reason{cluster=~"$cluster",namespace=~"$namespace",pod=~"$pod",reason!~"Completed"}'
                            + ")[$__rate_interval]) > bool 0) > 0",
                            "[{{reason}}] {{container}} {{pod}}",
                        ),
                    ],
                    ["last"],
                ),
                panels.subheader(
                    "Storage Alerts",
                    """[Alert Reference]
- Lagging Version: the checkpointed or pinned version id is lagging behind the current version id. Check 'Hummock Manager' section in dev dashboard.
- Lagging Compaction: there are too many ssts in L0. This can be caused by compactor failure or lag of compactor resource. Check 'Compaction' section in dev dashboard, and take care of the type of 'Commit Flush Bytes' and 'Compaction Throughput', whether the throughput is too low.
- Lagging Vacuum: there are too many stale files waiting to be cleaned. This can be caused by compactor failure or lag of compactor resource. Check 'Compaction' section in dev dashboard.
- Abnormal Meta Cache Memory: the meta cache memory usage is too large, exceeding the expected 10 percent.
- Abnormal Block Cache Memory: the block cache memory usage is too large, exceeding the expected 10 percent.
- Abnormal Uploading Memory Usage: uploading memory is more than 70 percent of the expected, and is about to spill.
- Write Stall: Compaction cannot keep up. Stall foreground write, Check 'Compaction' section in dev dashboard.
- Abnormal Version Size: the size of the version is too large, exceeding the expected 300MB. Check 'Hummock Manager' section in dev dashboard.
- Abnormal Delta Log Number: the number of delta logs is too large, exceeding the expected 5000. Check 'Hummock Manager' and `Compaction` section in dev dashboard and take care of the type of 'Compaction Success Count', whether the number of trivial-move tasks spiking.
- Abnormal Pending Event Number: the number of pending events is too large, exceeding the expected 10000000. Check 'Hummock Write' section in dev dashboard and take care of the 'Event handle latency', whether the time consumed exceeds the barrier latency.
- Abnormal Object Storage Failure: object storage failures are occurring. Check 'Object Storage' section in dev dashboard and take care of the 'Object Storage Failure Rate', whether the rate is too high.
""",
                    height=10,
                ),
                panels.timeseries_count(
                    "Storage Alerts",
                    "",
                    [
                        panels.target(
                            f"(({metric('storage_current_version_id')} - {metric('storage_checkpoint_version_id')}) >= bool 100) > 0",
                            "Lagging Version (checkpoint)",
                        ),
                        panels.target(
                            f"(({metric('storage_current_version_id')} - {metric('storage_min_pinned_version_id')}) >= bool 100) > 0",
                            "Lagging Version (pinned)",
                        ),
                        panels.target(
                            f"((sum(label_replace({metric('storage_level_total_file_size')}, 'L0', 'L0', 'level_index', '.*_L0') unless "
                            + f"{metric('storage_level_total_file_size')}) by (L0)) >= bool 52428800) > 0",
                            "Lagging Compaction",
                        ),
                        panels.target(
                            f"({metric('storage_stale_object_count')} >= bool 200) > 0",
                            "Lagging Vacuum",
                        ),
                        panels.target(
                            f"({metric('state_store_meta_cache_usage_ratio')} >= bool 1.1) > 0",
                            "Abnormal Meta Cache Memory",
                        ),
                        panels.target(
                            f"({metric('state_store_block_cache_usage_ratio')} >= bool 1.1) > 0",
                            "Abnormal Block Cache Memory",
                        ),
                        panels.target(
                            f"({metric('state_store_uploading_memory_usage_ratio')} >= bool 0.7) > 0",
                            "Abnormal Uploading Memory Usage",
                        ),
                        panels.target(
                            f"({metric('storage_write_stop_compaction_groups')} > bool 0) > 0",
                            "Write Stall (group {{compaction_group_id}})",
                        ),
                        panels.target(
                            f"({metric('storage_version_size')} >= bool 314572800) > 0",
                            "Abnormal Version Size",
                        ),
                        panels.target(
                            f"({metric('storage_delta_log_count')} >= bool 5000) > 0",
                            "Abnormal Delta Log Number",
                        ),
                        panels.target(
                            f"({metric('state_store_event_handler_pending_event')} >= bool 10000000) > 0",
                            "Abnormal Pending Event Number",
                        ),
                        panels.target(
                            f"(sum(rate({metric('object_store_failure_count')}[$__rate_interval])) by (type) > bool 0) > 0",
                            "Abnormal Object Storage Failure ({{type}})",
                        ),
                    ],
                    ["last"],
                ),
            ],
        )
    ]
