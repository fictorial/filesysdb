import os
import logging
import shutil
import tempfile


import pytest
from aadict import aadict
import ujson as json


import filesysdb as db


fsdb_logger = logging.getLogger('filesysdb')
fsdb_logger.addHandler(logging.StreamHandler())
fsdb_logger.setLevel(logging.DEBUG)


def make_base_path():
    return tempfile.mkdtemp()


def test_basic():
    try:
        base = tempfile.mkdtemp()
        with pytest.raises(AssertionError):
            db.prepare(base)  # no collections added
    finally:
        shutil.rmtree(base)


def test_add_collection():
    base = make_base_path()
    try:
        db._clear()
        db.add_collection('a')
        db.prepare(base)
    finally:
        shutil.rmtree(base)


def test_collection_path():
    base = make_base_path()
    try:
        db._clear()
        db.add_collection('a')
        db.prepare(base)
        assert db.collection_path('a') == '%s/a' % base
    finally:
        shutil.rmtree(base)


def test_multiple_collection_paths():
    base = make_base_path()
    try:
        db._clear()
        db.add_collection('a')
        db.add_collection('b')
        db.prepare(base)
        assert db.collection_path('a') == '%s/a' % base
        assert db.collection_path('b') == '%s/b' % base
    finally:
        shutil.rmtree(base)


def test_normalize_text():
    assert db.normalize_text('some/Id/with/path/seps') == 'someidwithpathseps'
    assert db.normalize_text('/../some\\Id\\with\\path\\seps', lcase=False) == 'someIdwithpathseps'


def test_save():
    base = make_base_path()
    try:
        u = aadict(name='brian')
        db._clear()
        db.add_collection('users')
        db.prepare(base)
        u = db.save_object('users', u)
        assert 'id' in u
        assert type(u.id) is str
        assert len(u.id)
        path = db.object_path('users', u)
        assert path == '%s/users/%s.json' % (
            base, db.normalize_text(u.id, lcase=False))
        assert os.access(path, os.F_OK)
        assert os.path.getsize(path) > 0
        with open(path, 'r') as f:
            data = json.load(f)
            assert data
            assert type(data) is dict
            assert 'id' in data
            assert data['id'] == u.id
            assert data['name'] == u.name
    finally:
        shutil.rmtree(base)


def test_get():
    base = make_base_path()
    try:
        u = aadict(name='brian')
        db._clear()
        db.add_collection('users')
        db.prepare(base)
        u = db.save_object('users', u)
        up = db.get_object('users', u.id)
        assert up.id == u.id
        assert up.name == u.name
        # should be cached since get reads through cache
        assert up.id in db._db.users.cache
    finally:
        shutil.rmtree(base)


def test_update():
    base = make_base_path()
    try:
        u = aadict(name='brian')
        db._clear()
        db.add_collection('users')
        db.prepare(base)
        u = db.save_object('users', u)
        up = db.get_object('users', u.id)
        up.name = 'tom'
        id_before = up.id
        db.save_object('users', up)
        assert up.id == id_before
        assert up.id in db._db.users.cache
        path = db.object_path('users', up)
        assert path == '%s/users/%s.json' % (
            base, db.normalize_text(up.id, lcase=False))
        assert os.access(path, os.F_OK)
        assert os.path.getsize(path) > 0
        with open(path, 'r') as f:
            data = json.load(f)
            assert data
            assert type(data) is dict
            assert 'id' in data
            assert data['id'] == up.id
            assert data['name'] == up.name
            assert data['name'] == 'tom'
    finally:
        shutil.rmtree(base)


def test_delete():
    base = make_base_path()
    try:
        db._clear()
        db.add_collection('users')
        db.prepare(base)
        u = aadict(name='brian')
        u = db.save_object('users', u)
        # get will put in cache
        assert db.get_object('users', u.id).id == u.id
        path = db.object_path('users', u)
        assert os.access(path, os.F_OK)
        assert os.path.getsize(path) > 0
        db.delete_object('users', u)
        # make sure no longer in cache
        assert u.id not in db._db.users.cache
        assert not os.access(path, os.F_OK)
        with pytest.raises(FileNotFoundError):
            assert os.path.getsize(path) == 0
        assert db.object_count('users') == 0
    finally:
        shutil.rmtree(base)


def test_delete_uncached():
    base = make_base_path()
    try:
        db._clear()
        db.add_collection('users')
        db.prepare(base)
        u = aadict(name='brian')
        u = db.save_object('users', u)
        path = db.object_path('users', u)
        assert os.access(path, os.F_OK)
        assert os.path.getsize(path) > 0
        db.delete_object('users', u)
        assert u.id not in db._db.users.cache
        assert not os.access(path, os.F_OK)
        with pytest.raises(FileNotFoundError):
            assert os.path.getsize(path) == 0
        assert db.object_count('users') == 0
    finally:
        shutil.rmtree(base)


def test_each_object():
    base = make_base_path()
    try:
        db._clear()
        db.add_collection('users')
        db.prepare(base)
        a = db.save_object('users', aadict(name='brian'))
        b = db.save_object('users', aadict(name='joe'))
        objs = list(db.each_object('users'))
        assert len(objs) == 2
        assert a in objs
        assert b in objs
    finally:
        shutil.rmtree(base)


def test_each_object_id():
    base = make_base_path()
    try:
        db._clear()
        db.add_collection('users')
        db.prepare(base)
        a = db.save_object('users', aadict(name='brian'))
        b = db.save_object('users', aadict(name='joe'))
        ids = list(db.each_object_id('users'))
        assert len(ids) == 2
        assert a.id in ids
        assert b.id in ids
    finally:
        shutil.rmtree(base)


def test_add_index():
    base = make_base_path()
    try:
        db._clear()
        db.add_collection('users')
        db.prepare(base)
        db.add_index('users', name='name index', fields=['name'])
        assert len(db._db.users.indexes) == 1
    finally:
        shutil.rmtree(base)


def test_add_index_with_existing_objects():
    base = make_base_path()
    try:
        db._clear()
        db.add_collection('users')
        db.prepare(base)
        a = db.save_object('users', aadict(name='brian'))
        db.add_index('users', name='name index', fields=['name'])
        assert len(db._db.users.indexes) == 1
        objs = list(db.each_indexed_object('users',
            index_name='name index', name='brian'))
        assert len(objs) == 1
        assert objs[0].id == a.id
    finally:
        shutil.rmtree(base)


def test_add_index_with_value_transformer():
    base = make_base_path()
    try:
        db._clear()
        db.add_collection('users')
        db.prepare(base)
        a = db.save_object('users', aadict(name='brian'))
        db.add_index('users', name='name index', fields=['name'],
            transformer=lambda vals: [db.normalize_text(vals[0])] + vals[1:])
        assert len(db._db.users.indexes) == 1
        objs = list(db.each_indexed_object('users',
            index_name='name index', name='brian'))
        assert len(objs) == 1
        assert objs[0].id == a.id
    finally:
        shutil.rmtree(base)


def test_add_unique_case_sensitive_index():
    base = make_base_path()
    try:
        db._clear()
        db.add_collection('users')
        db.prepare(base)
        a = db.save_object('users', aadict(name='brian'))
        db.add_index('users', name='name index',
            fields=['name'], unique=True)
        assert len(db._db.users.indexes) == 1
        objs = list(db.each_indexed_object('users',
            index_name='name index', name='brian'))
        assert len(objs) == 1
        assert objs[0].id == a.id
        with pytest.raises(db.UniqueConstraintError):
            db.save_object('users', aadict(name='brian'))
        db.save_object('users', aadict(name='aasdjfajsdfl'))
    finally:
        shutil.rmtree(base)


def test_add_unique_case_insensitive_index():
    base = make_base_path()
    try:
        db._clear()
        db.add_collection('users')
        db.prepare(base)
        a = db.save_object('users', aadict(name='brian'))
        db.add_index('users', name='name index', fields=['name'],
            unique=True, case_insensitive=True)
        assert len(db._db.users.indexes) == 1
        objs = list(db.each_indexed_object('users',
            index_name='name index', name='brian'))
        assert len(objs) == 1
        assert objs[0].id == a.id
        with pytest.raises(db.UniqueConstraintError):
            db.save_object('users', aadict(name='bRiAn'))
        with pytest.raises(db.UniqueConstraintError):
            db.save_object('users', aadict(name='brian'))
        db.save_object('users', aadict(name='aasdjfajsdfl'))
    finally:
        shutil.rmtree(base)


def test_add_unique_index_then_delete():
    base = make_base_path()
    try:
        db._clear()
        db.add_collection('users')
        db.prepare(base)
        a = db.save_object('users', aadict(name='brian'))
        db.add_index('users', name='name index', fields=['name'],
            unique=True, case_insensitive=True)
        assert len(db._db.users.indexes) == 1
        objs = list(db.each_indexed_object('users',
            index_name='name index', name='brian'))
        assert len(objs) == 1
        assert objs[0].id == a.id
        db.delete_object('users', a)
        objs = list(db.each_indexed_object('users',
            index_name='name index', name='brian'))
        assert len(objs) == 0
    finally:
        shutil.rmtree(base)
