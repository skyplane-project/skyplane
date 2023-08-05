import requests
import re


def get_ip() -> str:
    """Get the IP address of the current machine."""
    try:
        ip = requests.get("https://api.ipify.org").text
    except:
        return ""
    return ip


def get_cloud_region(ip: str, provider: str = "aws") -> str:
    """Get the cloud region which is hosting the current machine
    or closest to the current machine."""
    # todo: implement cloest region
    default_region = {"aws": "us-east-1", "azure": "eastus", "gcp": "us-east1"}
    try:
        if provider == "aws":
            region = requests.get(f"https://ip-ranges.amazonaws.com/ip-ranges.json").json()
            for prefix in region["prefixes"]:
                if re.match(prefix["ip_prefix"], ip):
                    return prefix["region"]
        elif provider == "azure":
            user_agent = {
                "User-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95"
            }
            body = requests.get(f"https://www.microsoft.com/en-us/download/confirmation.aspx?id=56519", headers=user_agent).content
            matches = re.search(b'downloadretry" href="([^"]*)"', body)
            if matches is None:
                return default_region[provider]
            region_url = matches.groups(0)[0]
            region = requests.get(region_url).json()
            for prefix in region["values"]:
                if re.match(prefix["properties"]["addressPrefix"], ip):
                    return prefix["properties"]["region"]
        elif provider == "gcp":
            region = requests.get(f"https://www.gstatic.com/ipranges/cloud.json").json()
            for prefix in region["prefixes"]:
                if re.match(prefix["ipv4Prefix"], ip):
                    return prefix["region"]
    except:
        return default_region[provider]
    return default_region[provider]
