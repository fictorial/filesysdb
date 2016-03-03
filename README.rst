filesysdb
=========

This is a filesystem-based database supporting

- logical collections;
- JSON for serialization;
- LRU cache per collection;
- unique and/or case-aware indexing on one or more fields; and
- iteration of stored objects and object ids.

Objects or Records
------------------

- an "object" or "record" is one entity that is distinctly stored
  on the filesystem
- an object is always a part of some larger named, logical collection
  of objects
- an object is a Python dictionary (or ``aadict`` for convenience)
- an object *must* have a collection-unique ``id`` key

Installation
------------

.. code:: bash

    pip install filesysdb

Usage
-----

.. code:: python

    import filesysdb as db
    from aadict import aadict
    from shortuuid import uuid

    # Add a named, logical collection of objects

    db.add_collection('widgets', cache_size=100)

    # Initialize the database

    db.prepare(base_path='data')

    # Create a multi-field index for the collection

    fields = ['part_no', 'size']

    db.add_index('widgets',
                 fields,
                 unique=False,
                 case_insensitive=False)

    # Create and save an object to the collection

    w = db.save_object('widgets', {
        'id': uuid(),
        'part_no': 1234,
        'size': 'xl'
    })

    # Get a stored object and update it.

    wp = db.get_object('widgets', id=w.id)
    assert w.id == wp.id  # same id, not the same object necessarily
    wp.size = 'small'     # the update
    db.save_object('widgets', wp)

    # Delete an object

    db.delete_object('widgets', w)

    # Get the count of objects in a collection

    n = db.object_count('widgets')

    # Iterate objects in a collection

    for o in db.each_object('widgets'):
        pass

    # Iterate object IDs in a collection

    for o in db.each_object_id('widgets'):
        pass

    # Iterate objects indexed by the given fields

    for o in db.each_indexed_object('widgets', fields):
        pass

    # Find out where an object's backing file is located

    path = db.object_path('widgets', wp)

Caveats
-------

A stored object lives in its own file on local disk and is cached in memory.
Indexed data lives in main memory.

The number of objects in main memory depends on the size of each collection's
in-memory cache as well as usage patterns.

Performance depends on the size of your objects and how fast your filesystem
and disk I/O is.
