import logging
from queues import queues
from django.conf import settings
from django.db import models
from haystack import connections
from haystack.exceptions import NotHandled
from haystack.signals import BaseSignalProcessor
from haystack.utils import default_get_identifier
from queued_search.utils import get_queue_name, rec_getattr


LOG_LEVEL = getattr(settings, 'SEARCH_QUEUE_LOG_LEVEL', logging.ERROR)

logging.basicConfig(
    level=LOG_LEVEL
)
logger = logging.getLogger('queued_search')

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
        # Make sure filter fields are all set to acceptable value, otherwise delete (unless new object, in which case just don't index)
        for filter_field, filter_values in filter_fields.items():
            if rec_getattr(instance, filter_field) not in filter_values:
                if kwargs['created']:
                    return True
                else:
                    return self.enqueue('delete', instance)

        try:
            exclude_fields = instance.queue_exclude()
        except AttributeError:
            exclude_fields = {}
        # Make sure exclude fields are not set to unacceptable value, otherwise delete (unless new object, in which case just don't index)
        for exclude_field, exclude_values in exclude_fields.items():
            if rec_getattr(instance, exclude_field) in exclude_values:
                if kwargs['created']:
                    return True
                else:
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
        
        """But first check if the model even has a ``SearchIndex`` implementation."""
        try:
            connections['default'].get_unified_index().get_index(instance.__class__)
        except NotHandled:
            return False
        
        instance_id = default_get_identifier(instance)
        logger.info("Queueing %s to %s", instance_id, action)
        message = "%s:%s" % (action, instance_id)
        queue = queues.Queue(get_queue_name())
        return queue.write(message)
