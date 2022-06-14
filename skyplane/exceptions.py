class SkyplaneException(Exception):
    def pretty_print_str(self):
        err = f"[bold][red]SkyplaneException: {str(self)}[/red][/bold]"
        return err


class MissingBucketException(SkyplaneException):
    def pretty_print_str(self):
        err = f"[red][bold]:x: MissingBucketException:[/bold] {str(self)}[/red]"
        err += "\n[bold][red]Please ensure that the bucket exists and is accessible.[/red][/bold]"
        return err


class MissingObjectException(SkyplaneException):
    def pretty_print_str(self):
        err = f"[red][bold]:x: MissingObjectException:[/bold] {str(self)}[/red]"
        err += "\n[bold][red]Please ensure that the object exists and is accessible.[/red][/bold]"
        return err


class InsufficientVCPUException(SkyplaneException):
    def pretty_print_str(self):
        err = f"[red][bold]:x: InsufficientVCPUException:[/bold] {str(self)}[/red]"
        err += "\n[bold][red]Please ensure that you have enough vCPUs in the given region.[/red][/bold]"
        # todo print link to a documentation page to request more vCPUs
        return err
