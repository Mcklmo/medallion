# Medallion

## Glossary

**Pipeline run**: One execution of an extractor and everything downstream that its output causes. A **local pipeline run** happens on a developer's machine; a **deployed pipeline run** is started by a schedule or trigger and spreads across the deployed steps.

_Avoid_: "run" alone when it matters whether it is local or deployed.

**Processor**: An extractor, transformer or store, as named in `config.yml` with its kind prefix (`extract-`, `transform-`, `store-`), the name its Cloud Run service carries; its class is the code it runs, and one class can back several processors. Deployed or in docker-compose, each processor runs as its own process; in a local pipeline run, they share one. Every log line names the processor it came from.

_Avoid_: "service" for a processor; the class name for a processor's name.

**RateLimited**: A streaming step's signal that the source rejected a call because of its rate. The message goes back on the queue and the step keeps its cadence. Several in a row stop a local pipeline run, so the log shows where the source's limit lies. A deployed pipeline run does not stop; a message rate-limited too often goes to the dead-letter queue, which is the alarm.

**RetryLater**: A step's signal that the source is unavailable (an outage). Every thread of the step pauses and polls with growing backoff until a call succeeds.

_Avoid_: using **RetryLater** for a rate limit; it pauses the cadence and hides where the limit lies.
