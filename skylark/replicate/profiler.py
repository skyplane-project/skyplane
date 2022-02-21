from typing import Dict, List
from skylark.chunk import ChunkState


def status_df_to_traceevent(log_df) -> List[Dict]:
    events = []
    start_time = log_df.time.min()
    for _, row in log_df.iterrows():
        if row["receiver_id"] == "random":
            continue
        if row["state"] == ChunkState.download_in_progress:
            events.append(
                {
                    "name": f"Download {row['chunk_id']}",
                    "ts": (row["time"] - start_time).total_seconds() * 1e6,
                    "pid": f"{row['region']}:{row['instance']}",
                    "tid": row["receiver_id"],
                    "ph": "B",
                }
            )
        elif row["state"] == ChunkState.downloaded:
            events.append(
                {
                    "name": f"Download {row['chunk_id']}",
                    "ts": (row["time"] - start_time).total_seconds() * 1e6,
                    "pid": f"{row['region']}:{row['instance']}",
                    "tid": row["receiver_id"],
                    "ph": "E",
                }
            )
        elif row["state"] == ChunkState.upload_in_progress:
            events.append(
                {
                    "name": f"Upload {row['chunk_id']}",
                    "ts": (row["time"] - start_time).total_seconds() * 1e6,
                    "pid": f"{row['region']}:{row['instance']}",
                    "tid": row["sender_id"],
                    "ph": "B",
                }
            )
        elif row["state"] == ChunkState.upload_complete:
            events.append(
                {
                    "name": f"Upload {row['chunk_id']}",
                    "ts": (row["time"] - start_time).total_seconds() * 1e6,
                    "pid": f"{row['region']}:{row['instance']}",
                    "tid": row["sender_id"],
                    "ph": "E",
                }
            )
    return events
