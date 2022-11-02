from importlib.resources import path

from skyplane.utils import logger

try:
    import pandas as pd
except ImportError:
    pd = None
    logger.warning("pandas not installed, will not be able to load transfer costs")


class AWSPricing:
    def __init__(self):
        self._transfer_df = None

    @property
    def transfer_df(self):
        if pd:
            if self._transfer_df is None:
                with path("skyplane.data", "aws_transfer_costs.csv") as transfer_cost_path:
                    self._transfer_df = pd.read_csv(transfer_cost_path).set_index(["src", "dst"])

            return self._transfer_df
        else:
            return None

    def get_transfer_cost(self, src_key, dst_key, premium_tier=True):
        assert premium_tier, "AWS transfer cost is only available for premium tier"
        transfer_df = self.transfer_df
        if transfer_df is None:
            return None
        else:
            src_provider, src = src_key.split(":")
            dst_provider, dst = dst_key.split(":")

            assert src_provider == "aws"
            if dst_provider == "aws":
                if (src, dst) in transfer_df.index:
                    return transfer_df.loc[src, dst]["cost"]
                else:
                    logger.warning(f"No transfer cost found for {src_key} -> {dst_key}, using max of {src}")
                    src_rows = transfer_df.loc[src]
                    src_rows = src_rows[src_rows.index != "internet"]
                    return src_rows.max()["cost"]
            else:
                return transfer_df.loc[src, "internet"]["cost"]
