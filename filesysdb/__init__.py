from aadict import aadict
from cachetools import LRUCache
import ujson as json
import regex
from shortuuid import uuid


from functools import wraps
from glob import glob
from time import time
import logging
import os
import shutil
import unicodedata


_logger = logging.getLogger(__name__)
_basepath = None
_serialize = None
_deserialize = None
_ext = None
_db = aadict()


class UniqueConstraintError(ValueError):
    pass


def normalize_text(text, lcase=True):
    text = str(text).strip()
    if lcase: text = text.lower()
    text = unicodedata.normalize('NFKD', text)
    text = regex.subn(r'\p{P}+', '', text)[0]
    return text.encode('ascii', 'ignore').decode()


def bench(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        start = time()
        ret = fn(*args, **kwargs)
        end = time()
        _logger.debug('function %s took %g secs',
            fn.__name__, end - start)
        return ret
    return wrapper


def object_path(collection, id):
    """Returns path to the backing file of the object
    with the given ``id`` in the given ``collection``.
    Note that the ``id`` is made filesystem-safe by
    "normalizing" its string representation."""
    _logger.debug(type(id))
    _logger.debug(id)
    if isinstance(id, dict) and 'id' in id:
        id = id['id']
    normalized_id = normalize_text(str(id), lcase=False)
    return os.path.join(_basepath, collection,
        '%s.%s' % (normalized_id, _ext))


def collection_path(collection):
    """Returns the base path to the ``collection``"""
    return os.path.join(_basepath, collection)


def load_object_at_path(path):
    """Load an object from disk at explicit path"""
    with open(path, 'r') as f:
        data = _deserialize(f.read())
        return aadict(data)


def load_object(collection, id):
    """Load an object from disk at path based on its
    ``collection`` and ``id``."""
    path = object_path(collection, id)
    return load_object_at_path(path)


def get_object(collection, id):
    """Get an object by its ``collection``-unique ``id``"""
    return _db[collection].cache[id]


def add_collection(collection,
                   cache_size=1000,
                   cache_cls=LRUCache,
                   **cache_args):
    """Add a collection named ``collection``."""
    assert collection not in _db
    cache = cache_cls(maxsize=cache_size,
                      missing=lambda id: load_object(collection, id),
                      **cache_args)
    _db[collection] = aadict(cache=cache, indexes={})


def _clear():
    _db.clear()


def prepare(base_path='data',
            serialize=json.dumps,
            deserialize=json.loads,
            file_ext='json'):
    """After you have added your collections, prepare the database
    for use."""
    global _basepath, _deserialize, _serialize, _ext
    _basepath = base_path
    assert callable(serialize)
    assert callable(deserialize)
    _serialize = serialize
    _deserialize = deserialize
    _ext = file_ext
    _logger.debug('preparing with base path %s and file ext %s',
        _basepath, _ext)
    assert len(_db)
    for collection in _db.keys():
        c_path = collection_path(collection)
        os.makedirs(c_path, exist_ok=True)
        _logger.info('collection "%s": %d objects',
            collection, object_count(collection))


def object_count(collection):
    """Returns the number of objects in the given ``collection``."""
    return len(glob('%s/*.%s' % (collection_path(collection), _ext)))


def each_object(collection):
    """Yields each object in the given ``collection``.
    The objects are loaded from cache and failing that,
    from disk."""
    c_path = collection_path(collection)
    paths = glob('%s/*.%s' % (c_path, _ext))
    for path in paths:
        yield load_object_at_path(path)


def each_object_id(collection):
    """Yields each object ID in the given ``collection``.
    The objects are not loaded."""
    c_path = collection_path(collection)
    paths = glob('%s/*.%s' % (c_path, _ext))
    for path in paths:
        match = regex.match(r'.+/(.+)\.%s$' % _ext, path)
        yield match.groups()[0]


@bench
def save_object(collection, obj):
    """Save an object ``obj`` to the given ``collection``.

    ``obj.id`` must be unique across all other existing objects in
    the given collection.  If ``id`` is not present in the object, a
    *UUID* is assigned as the object's ``id``.

    Indexes already defined on the ``collection`` are updated after
    the object is saved.

    Returns the object.
    """
    if 'id' not in obj:
        obj.id = uuid()
    id = obj.id
    path = object_path(collection, id)
    temp_path = '%s.temp' % path
    with open(temp_path, 'w') as f:
        data = _serialize(obj)
        f.write(data)
    shutil.move(temp_path, path)
    if id in _db[collection].cache:
        _db[collection].cache[id] = obj
    _update_indexes_for_mutated_object(collection, obj)
    return obj


@bench
def delete_object(collection, obj):
    try:
        os.remove(object_path(collection, obj))
        del _db[collection].cache[obj.id]
    except:
        pass
    _update_indexes_for_deleted_object(collection, obj)


def indexed_value(index, obj):
    values = [obj.get(f) for f in index.fields]
    if callable(index.transformer):
        values = index.transformer(values)
    k = json.dumps(values)
    return k.lower() if index.case_insensitive else k


@bench
def add_index(collection,
              name,
              fields,
              transformer=None,
              unique=False,
              case_insensitive=False):
    """
    Add a secondary index for a collection ``collection`` on one or
    more ``fields``.

    The values at each of the ``fields`` are loaded from existing
    objects and their object ids added to the index.

    You can later iterate the objects of an index via
    ``each_indexed_object``.

    If you update an object and call ``save_object``, the index will
    be updated with the latest values from the updated object.

    If you delete an object via ``delete_object``, the object will
    be removed from any indexes on the object's collection.

    If a function is provided for ``transformer``, the values
    extracted from each object in the collection will be passed to
    the ``transformer``.  The ``transformer`` should return a list
    of values that will go into the index.

    If ``unique`` is true, then there may only be at most one object
    in the collection with a unique set of values for each the
    ``fields`` provided.

    If ``case_insensitive`` is true, then the value stored in the
    index will be lower-cased and comparisons thereto will be
    lower-cased as well.
    """
    assert len(name) > 0
    assert len(fields) > 0
    indexes = _db[collection].indexes
    index = indexes.setdefault(name, aadict())
    index.transformer = transformer
    index.value_map = {}  # json([value]) => set(object_id)
    index.unique = unique
    index.case_insensitive = case_insensitive
    index.fields = fields
    for obj in each_object(collection):
        _add_to_index(index, obj)
    _logger.info('added %s, %s index to collection %s on fields: %s',
        'unique' if unique else 'non-unique',
        'case-insensitive' if case_insensitive else 'case-sensitive',
        collection, ', '.join(fields))


def _add_to_index(index, obj):
    """Adds the given object ``obj`` to the given ``index``"""
    id_set = index.value_map.setdefault(indexed_value(index, obj), set())
    if index.unique:
        if len(id_set) > 0:
            raise UniqueConstraintError()
    id_set.add(obj.id)


def _remove_from_index(index, obj):
    """Removes object ``obj`` from the ``index``."""
    try:
        index.value_map[indexed_value(index, obj)].remove(obj.id)
    except KeyError:
        pass


def each_indexed_object(collection, index_name, **where):
    """Yields each object indexed by the index with
    name ``name`` with ``values`` matching on indexed
    field values."""
    index = _db[collection].indexes[index_name]
    for id in index.value_map.get(indexed_value(index, where), []):
        yield get_object(collection, id)


def _update_indexes_for_mutated_object(collection, obj):
    """If an object is updated, this will simply remove
    it and re-add it to the indexes defined on the
    collection."""
    for index in _db[collection].indexes.values():
        _remove_from_index(index, obj)
        _add_to_index(index, obj)


def _update_indexes_for_deleted_object(collection, obj):
    """If an object is deleted, it should no longer be
    indexed so this removes the object from all indexes
    on the given collection."""
    for index in _db[collection].indexes.values():
        _remove_from_index(index, obj)
