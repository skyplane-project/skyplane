from concurrent.futures import Future
import mimetypes
import os
from loguru import logger
import tempfile
import hashlib
from tqdm import tqdm

from awscrt.s3 import S3Client, S3RequestType, S3RequestTlsMode
from awscrt.io import ClientBootstrap, DefaultHostResolver, EventLoopGroup
from awscrt.auth import AwsCredentialsProvider
from awscrt.http import HttpHeaders, HttpRequest

from skylark.utils import Timer


class S3Interface:
    def __init__(self, aws_region, bucket_name, use_tls=True):
        self.aws_region = aws_region
        self.bucket_name = bucket_name
        self.pending_downloads, self.completed_downloads = 0, 0
        self.pending_uploads, self.completed_uploads = 0, 0
        event_loop_group = EventLoopGroup(num_threads=os.cpu_count(), cpu_group=None)
        host_resolver = DefaultHostResolver(event_loop_group)
        bootstrap = ClientBootstrap(event_loop_group, host_resolver)
        credential_provider = AwsCredentialsProvider.new_default_chain(bootstrap)
        self._s3_client = S3Client(
            bootstrap=bootstrap,
            region=aws_region,
            credential_provider=credential_provider,
            throughput_target_gbps=100,
            part_size=None,
            tls_mode=S3RequestTlsMode.ENABLED if use_tls else S3RequestTlsMode.DISABLED,
        )

    def _on_done_download(self, **kwargs):
        self.completed_downloads += 1
        self.pending_downloads -= 1

    def _on_done_upload(self, **kwargs):
        self.completed_uploads += 1
        self.pending_uploads -= 1


    def reset_stats(self):
        self.stats_download = ThroughputStatistics()
        self.stats_upload = ThroughputStatistics()

    def get_stats(self):
        return self.stats.bytes_peak(), self.stats.bytes_avg()

    def download_object(self, src_object_name, dst_file_path) -> Future:
        download_headers = HttpHeaders([("host", self.bucket_name + ".s3." + self.aws_region + ".amazonaws.com")])
        request = HttpRequest("GET", src_object_name, download_headers)
        
        def _on_body_download(offset, chunk, **kwargs):
            if not os.path.exists(dst_file_path):
                open(dst_file_path, 'a').close()
            with open(dst_file_path, 'rb+') as f:
                f.seek(offset)
                f.write(chunk)
        
        return self._s3_client.make_request(
            recv_filepath=dst_file_path,
            request=request,
            type=S3RequestType.GET_OBJECT,
            on_done=self._on_done_download,
            on_body=_on_body_download,
        ).finished_future

    def upload_object(self, src_file_path, dst_object_name, content_type="infer") -> Future:
        content_len = os.path.getsize(src_file_path)
        if content_type == "infer":
            content_type = mimetypes.guess_type(src_file_path)[0] or "application/octet-stream"
        upload_headers = HttpHeaders()
        upload_headers.add("host", self.bucket_name + ".s3." + self.aws_region + ".amazonaws.com")
        upload_headers.add("Content-Type", content_type)
        upload_headers.add("Content-Length", str(content_len))
        request = HttpRequest("PUT", dst_object_name, upload_headers)
        return self._s3_client.make_request(
            send_filepath=src_file_path,
            request=request,
            type=S3RequestType.PUT_OBJECT,
            on_done=self._on_done_upload,
        ).finished_future


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="S3 Interface")
    parser.add_argument("--region", type=str, default="us-east-1", help="AWS region")
    parser.add_argument("--bucket", type=str, default="sky-us-east-1", help="S3 bucket name")
    parser.add_argument("--obj-name", type=str, default="test.txt", help="S3 object name")
    parser.add_argument("--use-tls", type=bool, default=True, help="Use TLS")
    parser.add_argument("--file-size-mb", type=int, default=10, help="File size in MB")
    args = parser.parse_args()

    s3_interface = S3Interface(args.region, args.bucket, args.use_tls)
    obj_name = "/" + args.obj_name

    with tempfile.NamedTemporaryFile() as tmp:
        fpath = tmp.name

        with open(fpath, "wb") as f:
            for i in tqdm(range(args.file_size_mb), leave=False, unit="MB", desc="Generate random file"):
                f.write(os.urandom(1024 * 1024))
        logger.debug(f"Generated random file {fpath}")
        file_md5 = hashlib.md5(open(fpath, "rb").read()).hexdigest()
        logger.debug(f"File md5: {file_md5}")

        # upload
        with Timer() as t:
            upload_future = s3_interface.upload_object(fpath, obj_name)
            ul_result = upload_future.result()
        gbps = args.file_size_mb * 8 * 1e6 / t.elapsed / 1e9
        logger.info(f"UL {fpath} -> s3://{args.region}/{args.bucket}{obj_name} in {t.elapsed:.2f}s (~{gbps:.2f}Gbps)")

    # download object
    with tempfile.NamedTemporaryFile() as tmp:
        fpath = tmp.name
        if os.path.exists(fpath):
            os.remove(fpath)
        with Timer() as t:
            download_future = s3_interface.download_object(obj_name, fpath)
            dl_result = download_future.result()
        gbps = args.file_size_mb * 8 * 1e6 / t.elapsed / 1e9
        logger.info(f"DL s3://{args.region}/{args.bucket}{obj_name} -> {fpath} in {t.elapsed:.2f}s (~{gbps:.2f}Gbps)")

        # check md5
        dl_file_md5 = hashlib.md5(open(fpath, "rb").read()).hexdigest()
        if dl_file_md5 != file_md5:
            logger.error(f"MD5 mismatch: uploaded {file_md5} != downloaded {dl_file_md5}")
        else:
            logger.info(f"MD5 match: uploaded {file_md5} == downloaded {dl_file_md5}")
