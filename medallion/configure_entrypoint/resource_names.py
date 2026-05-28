"""
Shared naming + constants for Pub/Sub, Cloud Run and Scheduler resources.

Used by both the GCP deploy script and the docker-compose generator so a given
config.yml produces the same resource names in production and in local dev.
"""

# Pub/Sub minimum (valid range 5-100). The application's LISTENER_MAX_RETRIES
# is the authoritative retry budget; this dead-letter policy is a backstop.
DEAD_LETTER_MAX_DELIVERY_ATTEMPTS = 5


def topic_name(repo: str, queue: str) -> str:
    return f"{repo}-{queue}"


def dlq_topic_name(repo: str, queue: str) -> str:
    return f"{repo}-{queue}-dlq"


def transformer_subscription_name(repo: str, transformer: str) -> str:
    return f"{repo}-{transformer}-sub"


def storage_subscription_name(repo: str, queue: str) -> str:
    return f"{repo}-{queue}-storage-sub"


def service_name(repo: str, processor: str) -> str:
    return f"{repo}-{processor}"


def scheduler_job_name(repo: str, processor: str, schedule: str) -> str:
    return f"{repo}-{processor}-{schedule}"
