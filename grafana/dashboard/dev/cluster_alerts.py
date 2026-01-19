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
                            f"{metric('all_barrier_nums')} >= bool 200",
                            "Too Many Barriers {{database_id}}",
                        ),
                        panels.target(
                            f"sum(rate({metric('recovery_latency_count')}[$__rate_interval])) > bool 0 + sum({metric('recovery_failure_cnt')}) > bool 0",
                            "Recovery Triggered {{recovery_type}}",
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
- Abnormal Object Storage Failure: the number of object storage failures is too large, exceeding the expected 50. Check 'Object Storage' section in dev dashboard and take care of the 'Object Storage Failure Rate', whether the rate is too high.
""",
                    height=10,
                ),
                panels.timeseries_count(
                    "Storage Alerts",
                    "",
                    [
                        panels.target(
                            f"(({metric('storage_current_version_id')} - {metric('storage_checkpoint_version_id')}) >= bool 100) + "
                            + f"(({metric('storage_current_version_id')} - {metric('storage_min_pinned_version_id')}) >= bool 100)",
                            "Lagging Version",
                        ),
                        panels.target(
                            f"sum(label_replace({metric('storage_level_total_file_size')}, 'L0', 'L0', 'level_index', '.*_L0') unless "
                            + f"{metric('storage_level_total_file_size')}) by (L0) >= bool 52428800",
                            "Lagging Compaction",
                        ),
                        panels.target(
                            f"{metric('storage_stale_object_count')} >= bool 200",
                            "Lagging Vacuum",
                        ),
                        panels.target(
                            f"{metric('state_store_meta_cache_usage_ratio')} >= bool 1.1",
                            "Abnormal Meta Cache Memory",
                        ),
                        panels.target(
                            f"{metric('state_store_block_cache_usage_ratio')} >= bool 1.1",
                            "Abnormal Block Cache Memory",
                        ),
                        panels.target(
                            f"{metric('state_store_uploading_memory_usage_ratio')} >= bool 0.7",
                            "Abnormal Uploading Memory Usage",
                        ),
                        panels.target(
                            f"{metric('storage_write_stop_compaction_groups')} > bool 0",
                            "Write Stall",
                        ),
                        panels.target(
                            f"{metric('storage_version_size')} >= bool 314572800",
                            "Abnormal Version Size",
                        ),
                        panels.target(
                            f"{metric('storage_delta_log_count')} >= bool 5000",
                            "Abnormal Delta Log Number",
                        ),
                        panels.target(
                            f"{metric('state_store_event_handler_pending_event')} >= bool 10000000",
                            "Abnormal Pending Event Number",
                        ),
                        panels.target(
                            f"{metric('object_store_failure_count')} >= bool 50",
                            "Abnormal Object Storage Failure",
                        ),
                    ],
                    ["last"],
                ),
            ],
        )
    ]
