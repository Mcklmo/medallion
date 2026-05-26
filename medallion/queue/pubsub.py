import json
import logging
import queue as _q
import threading
from typing import Any, Iterator

from google.cloud import pubsub_v1

from medallion.queue.base import Message, Queue


class PubSubQueue(Queue):
    """A `Queue` backed by Google Cloud Pub/Sub.

    Set `PUBSUB_EMULATOR_HOST=host:port` in the environment to point the
    underlying client at a local emulator — the API surface is identical.

    Constructor args:
      project_id:      GCP project (or the fake one the emulator uses).
      topic_id:        topic to publish to. Omit when only consuming.
      subscription_id: subscription to pull from. Omit when only publishing.

    The consumer side bridges Pub/Sub's async callback-based streaming pull
    onto the synchronous iterator that `Listener.run` expects: incoming
    messages are pushed into an in-memory queue and yielded by `messages()`.
    """

    def __init__(
        self,
        project_id: str,
        logger: logging.Logger,
        topic_id: str | None = None,
        subscription_id: str | None = None,
    ) -> None:
        self._project_id = project_id
        self._topic_id = topic_id
        self._subscription_id = subscription_id

        self._publisher: pubsub_v1.PublisherClient | None = None
        self._subscriber: pubsub_v1.SubscriberClient | None = None
        self._pull_future = None

        self._inbox: _q.Queue[Message] = _q.Queue()
        self._closed = False
        self.logger = logger

        if self._topic_id is not None:
            self._publisher = pubsub_v1.PublisherClient(
                publisher_options=pubsub_v1.types.PublisherOptions(
                    enable_message_ordering=True,
                ),
            )

    def __enter__(self) -> "PubSubQueue":
        if self._subscription_id is not None:
            self._subscriber = pubsub_v1.SubscriberClient()
            sub_path = self._subscriber.subscription_path(
                self._project_id,
                self._subscription_id,
            )
            self._pull_future = self._subscriber.subscribe(
                sub_path,
                callback=self._on_message,
            )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._closed = True
        self.logger.debug(
            "Closing PubSubQueue subscription_id=%s", self._subscription_id
        )
        if self._pull_future is not None:
            self._pull_future.cancel()
            try:
                self._pull_future.result(timeout=5)
            except Exception:
                pass
        if self._subscriber is not None:
            self._subscriber.close()
        self.logger.debug(
            "Closed PubSubQueue subscription_id=%s", self._subscription_id
        )

    def _on_message(self, pubsub_message) -> None:
        # Attributes are str→str on the wire; the rest of the pipeline expects
        # rich types (lists, numbers) in `args`, so we round-trip via JSON
        # the same way the publish side encodes them.
        args: dict[str, Any] = {}
        for k, v in pubsub_message.attributes.items():
            try:
                args[k] = json.loads(v)
            except (json.JSONDecodeError, TypeError):
                args[k] = v
        self._inbox.put(
            Message(
                data=pubsub_message.data,
                args=args,
                raw_message=pubsub_message,
                delivery_attempt=pubsub_message.delivery_attempt,
            ),
        )

    def read_stream(self) -> Iterator[Message]:
        while True:
            try:
                yield self._inbox.get(timeout=0.5)
            except _q.Empty:
                if self._closed:
                    return

    def close(self) -> None:
        self._closed = True

    def ack(self, message: Message) -> None:
        self.logger.debug(
            "PubSubQueue ack start subscription_id=%s thread=%s args=%s",
            self._subscription_id,
            threading.current_thread().name,
            message.args,
        )
        assert message.raw_message is not None, "Cannot ack message without raw handle"
        message.raw_message.ack()
        self.logger.debug(
            "PubSubQueue ack queued subscription_id=%s thread=%s args=%s",
            self._subscription_id,
            threading.current_thread().name,
            message.args,
        )

    def nack(self, message: Message) -> None:
        self.logger.debug(
            "PubSubQueue nack start subscription_id=%s thread=%s args=%s",
            self._subscription_id,
            threading.current_thread().name,
            message.args,
        )
        assert message.raw_message is not None, "Cannot nack message without raw handle"
        message.raw_message.nack()
        self.logger.debug(
            "PubSubQueue nack queued subscription_id=%s thread=%s args=%s",
            self._subscription_id,
            threading.current_thread().name,
            message.args,
        )

    def write(
        self,
        data: bytes,
        args: dict[str, Any],
        ordering_key: str,
    ) -> None:
        assert (
            self._publisher is not None and self._topic_id is not None
        ), "PubSubQueue.publish requires topic_id at construction time."

        topic_path = self._publisher.topic_path(
            self._project_id,
            self._topic_id,
        )
        attrs = {k: v if isinstance(v, str) else json.dumps(v) for k, v in args.items()}
        future = self._publisher.publish(
            topic_path,
            data,
            ordering_key=ordering_key,
            **attrs,
        )
        future.result(timeout=30)
