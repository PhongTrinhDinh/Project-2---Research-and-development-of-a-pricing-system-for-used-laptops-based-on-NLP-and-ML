import random

class RotateIdentityMiddleware:
    PROFILES = [
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        },
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "sec-ch-ua": '"Chromium";v="121", "Not A(Brand";v="99", "Google Chrome";v="121"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
        },
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
            "sec-ch-ua": None,
            "sec-ch-ua-mobile": None,
            "sec-ch-ua-platform": None,
        }
    ]
    
    def process_request(self, request, spider=None):
        profile = random.choice(self.PROFILES)
        
        request.headers["User-Agent"] = profile["User-Agent"]
        request.headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
        request.headers["Accept-Language"] = "en-US,en;q=0.9,vi;q=0.8"
        request.headers["Upgrade-Insecure-Requests"] = "1"
        
        if profile.get("sec-ch-ua"):
            request.headers["sec-ch-ua"] = profile["sec-ch-ua"]
            request.headers["sec-ch-ua-mobile"] = profile["sec-ch-ua-mobile"]
            request.headers["sec-ch-ua-platform"] = profile["sec-ch-ua-platform"]
        else:
            for header in ["sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform"]:
                request.headers.pop(header, None)
                
class RotateProxyMiddleware:
    PROXIES = [
        "http://160.30.204.24:50035",
        "http://123.58.199.232:8168",
        "http://115.79.70.69:8470",
        "http://14.225.2.98:808"
    ]

    def process_request(self, request, spider=None):
        if self.PROXIES:
            proxy = random.choice(self.PROXIES)
            request.meta["proxy"] = proxy