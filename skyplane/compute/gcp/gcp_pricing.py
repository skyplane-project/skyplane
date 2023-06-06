class GCPPricing:
    @staticmethod
    def get_transfer_cost(src_key, dst_key, premium_tier=True):
        src_provider, src = src_key.split(":")
        dst_provider, dst = dst_key.split(":")
        assert src_provider == "gcp"
        src_continent, src_region, src_zone = src.split("-")
        if dst_provider == "gcp":
            dst_continent, dst_region, dst_zone = dst.split("-")
            if src_continent == dst_continent and src_region == dst_region and src_zone == dst_zone:
                return 0.0
            elif src_continent == dst_continent and src_region == dst_region:
                return 0.01
            elif src_continent == dst_continent:
                if src_continent == "northamerica" or src_continent == "us":
                    return 0.01
                elif src_continent == "europe":
                    return 0.02
                elif src_continent == "asia":
                    return 0.05
                elif src_continent == "southamerica":
                    return 0.08
                elif src_continent == "australia":
                    return 0.08
                else:
                    raise Exception(f"Unknown continent {src_continent}")
            elif src.startswith("asia-southeast2") or src_continent == "australia":
                return 0.15
            elif dst.startswith("asia-southeast2") or dst_continent == "australia":
                return 0.15
            else:
                return 0.08
        elif dst_provider in ["aws", "azure", "cloudflare"] and premium_tier:
            is_dst_australia = (dst == "ap-southeast-2") if dst_provider == "aws" else (dst.startswith("australia"))
            # singapore or tokyo or osaka
            if src_continent == "asia" and (src_region == "southeast2" or src_region == "northeast1" or src_region == "northeast2"):
                return 0.19 if is_dst_australia else 0.14
            # jakarta
            elif (src_continent == "asia" and src_region == "southeast1") or (src_continent == "australia"):
                return 0.19
            # seoul
            elif src_continent == "asia" and src_region == "northeast3":
                return 0.19 if is_dst_australia else 0.147
            else:
                return 0.19 if is_dst_australia else 0.12
        elif dst_provider in ["aws", "azure", "cloudflare"] and not premium_tier:
            if src_continent == "us" or src_continent == "europe" or src_continent == "northamerica":
                return 0.085
            elif src_continent == "southamerica" or src_continent == "australia":
                return 0.12
            elif src_continent == "asia":
                return 0.11
            else:
                raise ValueError("Unknown src_continent: {}".format(src_continent))
