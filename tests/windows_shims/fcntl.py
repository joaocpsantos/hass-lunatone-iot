"""Minimal fcntl stand-in so the HA test harness imports on Windows (tests only)."""
LOCK_EX = 2
LOCK_NB = 4
LOCK_UN = 8
def flock(fd, op):
    return None
def fcntl(fd, cmd, arg=0):
    return 0
