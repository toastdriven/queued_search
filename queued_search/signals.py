from queues import queues
from django.db import models
from haystack.signals import BaseSignalProcessor
from haystack.utils import default_get_identifier
from queued_search.utils import get_queue_name, rec_getattr


class QueuedSignalProcessor(BaseSignalProcessor):
    def setup(self):
        models.signals.post_save.connect(self.enqueue_save)
        models.signals.post_delete.connect(self.enqueue_delete)

    def teardown(self):
        models.signals.post_save.disconnect(self.enqueue_save)
        models.signals.post_delete.disconnect(self.enqueue_delete)

    def enqueue_save(self, sender, instance, **kwargs):
        try:
            filter_fields = instance.queue_filter()
        except AttributeError:
            filter_fields = {}
        # Make sure filter fields are all set to acceptable value, otherwise delete
        for filter_field, filter_values in filter_fields.items():
            if rec_getattr(instance, filter_field) not in filter_values:
                return self.enqueue('delete', instance)

        try:
            exclude_fields = instance.queue_exclude()
        except AttributeError:
            exclude_fields = {}
        # Make sure exclude fields are not set to unacceptable value, otherwise delete
        for exclude_field, exclude_values in exclude_fields.items():
            if rec_getattr(instance, exclude_field) in exclude_values:
                return self.enqueue('delete', instance)

        return self.enqueue('update', instance)

    def enqueue_delete(self, sender, instance, **kwargs):
        return self.enqueue('delete', instance)

    def enqueue(self, action, instance):
        """
        Shoves a message about how to update the index into the queue.

        This is a standardized string, resembling something like::

            ``update:notes.note.23``
            # ...or...
            ``delete:weblog.entry.8``
        """
        message = "%s:%s" % (action, default_get_identifier(instance))
        queue = queues.Queue(get_queue_name())
        return queue.write(message)
