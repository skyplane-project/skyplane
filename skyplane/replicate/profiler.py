from typing import Dict, List

from skyplane.chunk import ChunkState


def status_df_to_traceevent(log_df) -> List[Dict]:
    events = []
    valid_states = [ChunkState.download_in_progress, ChunkState.downloaded, ChunkState.upload_in_progress, ChunkState.upload_complete]
    log_df = log_df[log_df["state"].isin(valid_states)]
    start_time = log_df.time.min()
    for _, row in log_df.iterrows():
        if "receiver_id" in row and row["receiver_id"] == "random":
            continue
        if row["state"] == ChunkState.download_in_progress:
            events.append(
                {
                    "name": f"Download {row['chunk_id']}",
                    "ts": (row["time"] - start_time).total_seconds() * 1e6,
                    "pid": f"{row['region']}:{row['instance']}",
                    "tid": row.get("receiver_id"),
                    "ph": "B",
                }
            )
        elif row["state"] == ChunkState.downloaded:
            events.append(
                {
                    "name": f"Download {row['chunk_id']}",
                    "ts": (row["time"] - start_time).total_seconds() * 1e6,
                    "pid": f"{row['region']}:{row['instance']}",
                    "tid": row.get("receiver_id"),
                    "ph": "E",
                }
            )
        elif row["state"] == ChunkState.upload_in_progress:
            events.append(
                {
                    "name": f"Upload {row['chunk_id']}",
                    "ts": (row["time"] - start_time).total_seconds() * 1e6,
                    "pid": f"{row['region']}:{row['instance']}",
                    "tid": row.get("sender_id"),
                    "ph": "B",
                }
            )
        elif row["state"] == ChunkState.upload_complete:
            events.append(
                {
                    "name": f"Upload {row['chunk_id']}",
                    "ts": (row["time"] - start_time).total_seconds() * 1e6,
                    "pid": f"{row['region']}:{row['instance']}",
                    "tid": row.get("sender_id"),
                    "ph": "E",
                }
            )
    return events
