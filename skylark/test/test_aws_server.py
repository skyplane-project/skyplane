import argparse
import os
import tempfile

from loguru import logger

from skylark import skylark_root
from skylark.compute.aws.aws_server import AWSServer
from skylark.utils import common_excludes

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", required=True)
    parser.add_argument("--instance-id", required=True)
    parser.add_argument("--command-log", default=None)
    args = parser.parse_args()

    server = AWSServer(args.region, args.instance_id, command_log_file=args.command_log)
    logger.debug(f"Created server {server}")
    server.wait_for_ready()

    # test run_command
    logger.info("Test: run_command")
    server.log_comment("Test: run_command")
    assert server.run_command("echo hello")[0] == "hello\n"
    server.flush_command_log()

    # test file copy
    logger.info("Test: copy_file")
    server.log_comment("Test: copy_file")
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpfile = os.path.join(tmpdir, "test.txt")
        with open(tmpfile, "w") as f:
            f.write("hello world")
        server.copy_file(tmpfile, "/tmp/test.txt")
        assert open(tmpfile).read() == server.run_command("cat /tmp/test.txt")[0]
    server.flush_command_log()

    # test sync_directory
    logger.info("Test: sync_directory")
    server.log_comment("Test: sync_directory")
    remote_tmpdir = "/tmp/test_sync_directory"
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir, "test1.txt"), "w") as f:
            f.write("hello world")
        with open(os.path.join(tmpdir, "test2.txt"), "w") as f:
            f.write("hello world")

        server.sync_directory(tmpdir, "/tmp/test_sync_directory", delete_remote=True)
        server.flush_command_log()

        assert open(os.path.join(tmpdir, "test1.txt")).read() == server.run_command(f"cat {remote_tmpdir}/test1.txt")[0]
        assert open(os.path.join(tmpdir, "test2.txt")).read() == server.run_command(f"cat {remote_tmpdir}/test2.txt")[0]
        assert server.run_command(f"ls {remote_tmpdir}")[0].strip().split() == [
            "test1.txt",
            "test2.txt",
        ]
        server.run_command(f"rm -rf {remote_tmpdir}")
    server.flush_command_log()

    # test copy source directory
    logger.info("Test: sync_directory source")
    server.log_comment("Test: sync_directory source")
    with tempfile.TemporaryDirectory() as tmpdir:
        source_dir = skylark_root.resolve().absolute()
        ignore_full_path = common_excludes()
        server.sync_directory(
            source_dir,
            "/tmp/test_copy_source/src",
            delete_remote=True,
            ignore_globs=ignore_full_path,
        )
    server.flush_command_log()

    # clean up
    server.close_server()
    logger.info("All tests passed")
