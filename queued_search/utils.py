from django.conf import settings


def get_queue_name():
    """
    Standized way to fetch the queue name.

    Can be overridden by specifying ``SEARCH_QUEUE_NAME`` in your settings.

    Given that the queue name is used in disparate places, this is primarily
    for sanity.
    """
    return getattr(settings, 'SEARCH_QUEUE_NAME', 'haystack_search_queue')


def rec_getattr(obj, attr):
    """Get object's attribute. May use dot notation.

    >>> class C(object): pass
    >>> a = C()
    >>> a.b = C()
    >>> a.b.c = 4
    >>> rec_getattr(a, 'b.c')
    4
    """
    if '.' not in attr:
        return getattr(obj, attr)
    else:
        L = attr.split('.')
        return rec_getattr(getattr(obj, L[0]), '.'.join(L[1:]))
