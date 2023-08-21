from http import HTTPStatus
import os
import signal
from fastapi import FastAPI, BackgroundTasks, Response
from pydantic import BaseModel
from skyplane.api.usage import UsageClient
from skyplane.cli.cli_transfer import SkyplaneCLI
from skyplane.cli.impl.progress_bar import ProgressBarTransferHook
from skyplane.obj_store.storage_interface import StorageInterface
from skyplane.utils.path import parse_path
from skyplane.utils import logger

app = FastAPI()
dp = None
transfer_id = None


class TransferBody(BaseModel):
    src: str
    dst: str
    cmd: str
    recursive: bool
    debug: bool
    multipart: bool
    confirm: bool
    max_instances: int
    max_connections: int
    solver: str
    transfer_id: str


@app.post("/start")
async def start_transfer(
    transfer_body: TransferBody,
    background_tasks: BackgroundTasks,
):
    global transfer_id
    transfer_id = transfer_body.transfer_id
    background_tasks.add_task(_run_transfer, transfer_body)
    return {"transfer_id": transfer_id}


@app.post("/stop")
async def stop_transfer():
    if dp is None:
        return Response(status_code=HTTPStatus.BAD_REQUEST)
    raise KeyboardInterrupt


@app.get("/status")
async def get_transfer_status():
    if dp is None:
        return Response(status_code=HTTPStatus.BAD_REQUEST)
    return {"transfer_id": transfer_id, "status": UsageClient.read_usage_data(), "logs": dp.get_all_logs()}


@app.get("/server/status")
async def get_server_status():
    return Response(status_code=HTTPStatus.OK)


def _run_transfer(transfer_body: TransferBody):
    # 1. Define the source and destination
    src, dst = transfer_body.src, transfer_body.dst
    provider_src, bucket_src, path_src = parse_path(src)
    provider_dst, bucket_dst, path_dst = parse_path(dst)
    src_region_tag = StorageInterface.create(f"{provider_src}:infer", bucket_src).region_tag()  # type: ignore
    dst_region_tag = StorageInterface.create(f"{provider_dst}:infer", bucket_dst).region_tag()  # type: ignore

    # 2. Create SkyplaneCLI and pipeline - don't need to do the usual checks
    # since they were already handled before tracker vm was called
    solver, max_instances, recursive = transfer_body.solver, transfer_body.max_instances, transfer_body.recursive
    cli = SkyplaneCLI(src_region_tag=src_region_tag, dst_region_tag=dst_region_tag, args=transfer_body.model_dump())
    pipeline = cli.make_pipeline(planning_algorithm=solver, max_instances=max_instances)
    if cli.args["cmd"] == "cp":
        pipeline.queue_copy(src, dst, recursive=recursive)
    else:
        pipeline.queue_sync(src, dst)

    # 3. Create dataplane and run the transfer
    global dp
    debug = transfer_body.debug
    dp = pipeline.create_dataplane(debug=debug)
    with dp.auto_deprovision():
        try:
            dp.provision(spinner=True)
            dp.run(pipeline.jobs_to_dispatch, hooks=ProgressBarTransferHook(dp.topology.dest_region_tags))
        except KeyboardInterrupt:
            logger.fs.warning("Transfer cancelled by user (KeyboardInterrupt).")
            try:
                dp.copy_gateway_logs()
                _force_deprovision()
            except Exception as e:
                logger.fs.exception(e)
                UsageClient.log_exception("cli_cp", e, transfer_body.model_dump(), cli.src_region_tag, [cli.dst_region_tag])
        except Exception as e:
            logger.fs.exception(e)
            UsageClient.log_exception("cli_query_objstore", e, transfer_body.model_dump(), cli.src_region_tag, [cli.dst_region_tag])
            _force_deprovision()
        finally:
            # TODO: Upload all logs to the source bucket with a special status file
            # We can then access these status files to download to the local device
            all_dp_logs = dp.get_all_logs()
            all_usage_logs = UsageClient.read_usage_data()
            all_client_logs = logger.get_logs()


def _shutdown():
    os._exit(1)


def _force_deprovision():
    assert dp is not None, "Must initialize dataplane before deprovisioning"
    s = signal.signal(signal.SIGINT, signal.SIG_IGN)
    dp.deprovision(spinner=True)
    signal.signal(signal.SIGINT, s)
