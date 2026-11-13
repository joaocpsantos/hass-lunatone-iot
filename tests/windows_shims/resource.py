"""Minimal resource stand-in so the HA test harness imports on Windows (tests only)."""
RLIMIT_NOFILE = 7
RLIM_INFINITY = -1
def getrlimit(which):
    return (8192, 8192)
def setrlimit(which, limits):
    return None
