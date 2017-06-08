from __future__ import print_function
from __future__ import unicode_literals

import os
import os.path
import time
import logging

import sqlalchemy
import sqlalchemy.ext.declarative
import sqlalchemy.orm

from utils import mkdir_p


# There is a lag between an archive being created and the archive
# appearing on an inventory. Even if the inventory has an InventoryDate
# of after the archive was created, it still doesn't necessarily appear.
# So only warn of a missing archive if the archive still hasn't appeared
# on an inventory created INVENTORY_LAG seconds after the archive was
# uploaded successfully.
INVENTORY_LAG = 24 * 60 * 60 * 3

logger = logging.getLogger(__name__)

Base = sqlalchemy.ext.declarative.declarative_base()

class Cache(object):
    class Archive(Base):
        __tablename__ = 'archive'
        id = sqlalchemy.Column(sqlalchemy.String(255), primary_key=True)
        name = sqlalchemy.Column(sqlalchemy.String(255))
        size = sqlalchemy.Column(sqlalchemy.Integer)
        vault = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
        key = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
        last_seen_upstream = sqlalchemy.Column(sqlalchemy.Integer)
        created_here = sqlalchemy.Column(sqlalchemy.Integer)
        deleted_here = sqlalchemy.Column(sqlalchemy.Integer)

        def __init__(self, *args, **kwargs):
            self.created_here = time.time()
            super(Cache.Archive, self).__init__(*args, **kwargs)

    Session = sqlalchemy.orm.sessionmaker()

    def __init__(self, key, db_driver):
        self.key = key
        if 'sqlite://' in db_driver:
            db_path = db_driver[len('sqlite://'):]
            mkdir_p(os.path.dirname(db_path))
            self.engine = sqlalchemy.create_engine('sqlite:///%s' % db_path)
        else:
            self.engine = sqlalchemy.create_engine(db_driver)
        Base.metadata.create_all(self.engine)
        self.Session.configure(bind=self.engine)
        self.session = self.Session()

    def add_archive(self, vault_name, name, size, archive):
        self.session.add(self.Archive(key=self.key,
                                      vault=vault_name, name=name, size=size,
                                      id=archive.id))
        self.session.commit()

    def _get_archive_query_by_ref(self, vault, ref):
        if ref.startswith('id:'):
            filter = {'id': ref[3:]}
        elif ref.startswith('name:'):
            filter = {'name': ref[5:]}
        else:
            filter = {'name': ref}
        return self.session.query(self.Archive).filter_by(
                key=self.key, vault=vault, deleted_here=None, **filter)

    def get_archive_id(self, vault, ref):
        try:
            result = self._get_archive_query_by_ref(vault, ref).one()
        except sqlalchemy.orm.exc.NoResultFound:
            raise KeyError(ref)
        return result.id

    def get_archive_name(self, vault, ref):
        try:
            result = self._get_archive_query_by_ref(vault, ref).one()
        except sqlalchemy.orm.exc.NoResultFound:
            raise KeyError(ref)
        return result.name

    def get_archive_last_seen(self, vault, ref):
        try:
            result = self._get_archive_query_by_ref(vault, ref).one()
        except sqlalchemy.orm.exc.NoResultFound:
            raise KeyError(ref)
        return result.last_seen_upstream or result.created_here

    def delete_archive(self, vault, ref):
        try:
            result = self._get_archive_query_by_ref(vault, ref).one()
        except sqlalchemy.orm.exc.NoResultFound:
            raise KeyError(name)
        result.deleted_here = time.time()
        self.session.commit()

    @staticmethod
    def _archive_ref(archive, force_id=False):
        if archive.name and not force_id:
            if (archive.name.startswith('name:') or
                    archive.name.startswith('id:')):
                return "name:%s" % archive.name
            else:
                return archive.name
        else:
            return 'id:' + archive.id

    def _get_archive_list_objects(self, vault):
        for archive in (
                self.session.query(self.Archive).
                             filter_by(key=self.key,
                                       vault=vault,
                                       deleted_here=None).
                             order_by(self.Archive.name)):
            yield archive

    def get_archive_list(self, vault):
        def force_id(archive):
            return "\t".join([
                self._archive_ref(archive, force_id=True),
                "%s" % archive.name
                ])

        for archive_name, archive_iterator in (
                itertools.groupby(
                    self._get_archive_list_objects(vault),
                    lambda archive: archive.name)):
            # Yield self._archive_ref(..., force_id=True) if there is more than
            # one archive with the same name; otherwise use force_id=False.
            first_archive = next(archive_iterator)
            try:
                second_archive = next(archive_iterator)
            except StopIteration:
                yield self._archive_ref(first_archive, force_id=False)
            else:
                yield force_id(first_archive)
                yield force_id(second_archive)
                for subsequent_archive in archive_iterator:
                    yield force_id(subsequent_archive)

    def get_archive_list_with_ids(self, vault):
        for archive in self._get_archive_list_objects(vault):
            yield "\t".join([
                self._archive_ref(archive, force_id=True),
                "%s" % archive.name,
                ])

    def mark_seen_upstream(
            self, vault, id, name, size, upstream_creation_date,
            upstream_inventory_date, upstream_inventory_job_creation_date,
            fix=False):

        # Inventories don't get recreated unless the vault has changed.
        # See: https://forums.aws.amazon.com/thread.jspa?threadID=106541
        #
        # The cache's last_seen_upstream is supposed to contain a point in time
        # at which we know for sure that an archive existed, but this can fall
        # too far behind if a vault doesn't change. So assume that an archive
        # that appears in an inventory that hasn't been updated recently
        # nevertheless existed at around the time the inventory _could_ have
        # been regenerated, ie. at some point prior to the date that we
        # requested the inventory retrieval job.
        #
        # This is preferred over using the job completion date as an archive
        # could in theory be deleted while an inventory job is in progress and
        # would still appear in that inventory.
        #
        # Making up a date prior to the inventory job's creation could mean
        # that last_seen_upstream ends up claiming that an archive existed even
        # before it was created, but this will not cause a problem. Better that
        # it's too far back in time than too far ahead.
        #
        # With thanks to Wolfgang Nagele.

        last_seen_upstream = max(
            upstream_inventory_date,
            upstream_inventory_job_creation_date - INVENTORY_LAG
            )

        try:
            archive = self.session.query(self.Archive).filter_by(
                key=self.key, vault=vault, id=id).one()
        except sqlalchemy.orm.exc.NoResultFound:
            self.session.add(
                self.Archive(
                    key=self.key, vault=vault, name=name, size=size, id=id,
                    last_seen_upstream=last_seen_upstream
                    )
                )
        else:
            if not archive.name:
                archive.name = name
            elif archive.name != name:
                if fix:
                    logger.warn('archive %r appears to have changed name from %r ' %
                         (archive.id, archive.name) + 'to %r (fixed)' % (name))
                    archive.name = name
                else:
                    logger.warn('archive %r appears to have changed name from %r ' %
                         (archive.id, archive.name) + 'to %r' % (name))
            if not archive.size:
                archive.size = size
            elif archive.size != size:
                if fix:
                    logger.warn('archive %r appears to have changed size from %r ' %
                         (archive.id, archive.size) + 'to %r (fixed)' % (size))
                    archive.size = size
                else:
                    logger.warn('archive %r appears to have changed size from %r ' %
                         (archive.id, archive.size) + 'to %r' % (size))
            if archive.deleted_here:
                archive_ref = self._archive_ref(archive)
                if archive.deleted_here < upstream_inventory_date:
                    logger.warn('archive %r marked deleted but still present' %
                         archive_ref)
                else:
                    logger.warn('archive %r deletion not yet in inventory' %
                         archive_ref)
            archive.last_seen_upstream = last_seen_upstream

    def mark_only_seen(self, vault, inventory_date, ids, fix=False):
        upstream_ids = set(ids)
        our_ids = set([r[0] for r in
                self.session.query(self.Archive.id)
                            .filter_by(key=self.key, vault=vault).all()])
        missing_ids = our_ids - upstream_ids
        for id in missing_ids:
            archive = (self.session.query(self.Archive)
                                   .filter_by(key=self.key,
                                              vault=vault, id=id)
                                   .one())
            archive_ref = self._archive_ref(archive)
            if archive.deleted_here and archive.deleted_here < inventory_date:
                self.session.delete(archive)
                logger.info('deleted archive %r has left inventory; ' % archive_ref +
                     'removed from cache')
            elif not archive.deleted_here and (
                  archive.last_seen_upstream or
                    (archive.created_here and
                     archive.created_here < inventory_date - INVENTORY_LAG)):
                if fix:
                    self.session.delete(archive)
                    logger.warn('archive disappeared: %r (removed from cache)' %
                         archive_ref)
                else:
                    logger.warn('archive disappeared: %r' % archive_ref)
            else:
                logger.warn('new archive not yet in inventory: %r' % archive_ref)

    def mark_commit(self):
        self.session.commit()
