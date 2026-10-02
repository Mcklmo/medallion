# Medallion

## Glossary

**RateLimited**: A step's signal that the source rejected a call because of its rate. The message goes back on the queue and the step keeps its cadence. Several in a row stop the run.

**RetryLater**: A step's signal that the source is unavailable (an outage). Every thread of the step pauses and polls with growing backoff until a call succeeds.

_Avoid_: using **RetryLater** for a rate limit; it pauses the cadence and hides where the limit lies.
