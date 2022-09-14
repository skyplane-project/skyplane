class SkyplaneException(Exception):
    def pretty_print_str(self):
        err = f"[bold][red]SkyplaneException: {str(self)}[/red][/bold]"
        return err


class MissingBucketException(SkyplaneException):
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


class NoSuchObjectException(Exception):
    pass


class BadConfigException(Exception):
    pass
