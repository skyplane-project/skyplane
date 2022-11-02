from typing import Dict, List


class SkyplaneException(Exception):
    def pretty_print_str(self):
        err = f"[bold][red]SkyplaneException: {str(self)}[/red][/bold]"
        return err


class SkyplaneGatewayException(SkyplaneException):
    def __init__(self, message, errors: Dict[str, List[str]]):
        super().__init__(message)
        self.errors = errors

    def pretty_print_str(self):
        err = f"[bold][red]SkyplaneGatewayException: {str(self)}[/red][/bold]"
        for node, errors in self.errors.items():
            err += f"\t[bold][red]Errors on node {node}[/red][/bold]"
            for error in errors:
                err += f"\t\t* {error}"
        return err


class PermissionsException(SkyplaneException):
    def pretty_print_str(self):
        err = f"[bold][red]PermissionsException: {str(self)}[/red][/bold]"
        return err


class MissingBucketException(PermissionsException):
    def pretty_print_str(self):
        err = f"[red][bold]:x: MissingBucketException:[/bold] {str(self)}[/red]"
        err += "\n[red][bold]Please ensure that the bucket exists and is accessible.[/bold] See https://skyplane.org/en/latest/faq.html#TroubleshootingMissingBucketException.[/red]"
        return err


class MissingObjectException(SkyplaneException):
    def pretty_print_str(self):
        err = f"[red][bold]:x: MissingObjectException:[/bold] {str(self)}[/red]"
        err += "\n[bold][red]Please ensure that the object exists and is accessible.[/red][/bold]"
        return err


class ChecksumMismatchException(SkyplaneException):
    def pretty_print_str(self):
        err = f"[red][bold]:x: ChecksumMismatchException:[/bold] {str(self)}[/red]"
        err += "\n[bold][red]Please re-run the transfer due to checksum mismatch at the destination object store.[/red][/bold]"
        return err


class InsufficientVCPUException(SkyplaneException):
    def pretty_print_str(self):
        err = f"[red][bold]:x: InsufficientVCPUException:[/bold] {str(self)}[/red]"
        err += "\n[bold][red]Please ensure that you have enough vCPUs in the given region.[/red][/bold]"
        # todo print link to a documentation page to request more vCPUs
        return err


class TransferFailedException(SkyplaneException):
    def __init__(self, message, failed_objects=None):
        super().__init__(message)
        self.failed_objects = failed_objects

    def pretty_print_str(self):
        err = f"[red][bold]:x: TransferFailedException:[/bold] {str(self)}[/red]"
        if self.failed_objects and len(self.failed_objects) > 0:
            err += "\n[bold][red]Transfer failed. The following objects were not found at the destination:[/red][/bold] " + str(
                self.failed_objects
            )
        return err


class NoSuchObjectException(SkyplaneException):
    pass


class BadConfigException(SkyplaneException):
    pass
