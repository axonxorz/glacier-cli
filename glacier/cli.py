#!/usr/bin/env python

# Copyright (c) 2012 Robie Basak
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish, dis-
# tribute, sublicense, and/or sell copies of the Software, and to permit
# persons to whom the Software is furnished to do so, subject to the fol-
# lowing conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABIL-
# ITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
# SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.

from __future__ import print_function
from __future__ import unicode_literals

import argparse
import calendar
import errno
import itertools
import os
import os.path
import sys
import time
import json

import botocore
import boto3
import iso8601

from wrappedfile import WrappedFile
from configuration import configuration, get_user_cache_dir
from models import Cache


PROGRAM_NAME = 'glacier'

class ConsoleError(RuntimeError):
    def __init__(self, m):
        super(ConsoleError, self).__init__(m)


class RetryConsoleError(ConsoleError): pass


def info(message):
    print(insert_prefix_to_lines('%s: info: ' % PROGRAM_NAME, message),
          file=sys.stderr)


def warn(message):
    print(insert_prefix_to_lines('%s: warning: ' % PROGRAM_NAME, message),
          file=sys.stderr)


def verbose(message):
    pass


def real_verbose(message):
    print(insert_prefix_to_lines('%s: verbose: ' % PROGRAM_NAME, message),
          file=sys.stderr)


def insert_prefix_to_lines(prefix, lines):
    return "\n".join([prefix + line for line in lines.split("\n")])


def iso8601_to_unix_timestamp(iso8601_date_str):
    return calendar.timegm(iso8601.parse_date(iso8601_date_str).utctimetuple())




def get_cache_key():
    """Return some account key associated with the session.

    This is used to key a cache, so that the same cache can serve multiple
    accounts. The only requirement is that multiple namespaces of vaults and/or
    archives can never collide for connections that return the same key with
    this function. The cache will more more efficient if the same Glacier
    namespace sets always result in the same key.
    """
    # Note: the boto3 default session is used,so get the AWS access key from there
    return boto3.DEFAULT_SESSION.get_credentials().access_key


def find_retrieval_jobs(vault, archive_id):
    return [job for job in vault.jobs.all() if job.archive_id == archive_id]


def find_inventory_jobs(vault, max_age_hours=0):
    if max_age_hours:
        def recent_enough(job):
            if not job.completed:
                return True

            completion_date = iso8601_to_unix_timestamp(job.completion_date)
            return completion_date > time.time() - max_age_hours * 60 * 60
    else:
        def recent_enough(job):
            return not job.completed

    return [job for job in vault.jobs.all()
            if job.action == 'InventoryRetrieval' and recent_enough(job)]


def find_complete_job(jobs):
    for job in sorted(filter(lambda job: job.completed, jobs), key=lambda job: iso8601.parse_date(job.completion_date), reverse=True):
        return job


def has_pending_job(jobs):
    return any(filter(lambda job: not job.completed, jobs))


def update_job_list(jobs):
    for i, job in enumerate(jobs):
        job.reload()


def job_oneline(resource, cache, vault, job):
    action_letter = {'ArchiveRetrieval': 'a',
                     'InventoryRetrieval': 'i'}[job.action]
    status_letter = {'InProgress': 'p',
                     'Succeeded': 'd',
                     'Failed': 'e'}[job.status_code]
    date = job.completion_date
    if not date:
        date = job.creation_date
    if job.action == 'ArchiveRetrieval':
        try:
            name = cache.get_archive_name(vault.name, 'id:' + job.archive_id)
        except KeyError:
            name = None
        if name is None:
            name = 'id:' + job.archive_id
    elif job.action == 'InventoryRetrieval':
        name = ''
    return '{action_letter}/{status_letter} {date} {vault.name:10} {name}'.format(
            **locals())


def wait_until_job_completed(jobs, sleep=600, tries=144):
    max_tries = tries
    update_job_list(jobs)
    job = find_complete_job(jobs)
    while not job:
        tries -= 1
        if tries < 0:
            raise RuntimeError('Timed out waiting for job completion')
        verbose('Job not completed, sleeping for {} seconds (Wait {} of {})'.format(sleep, max_tries-tries, max_tries))
        time.sleep(sleep)
        update_job_list(jobs)
        job = find_complete_job(jobs)

    return job


class App(object):
    def write_default_config(self):
        configuration.write_default()

    def job_list(self):
        for vault in self.resource.vaults.all():
            job_list = [job_oneline(self.resource,
                                    self.cache,
                                    vault,
                                    job)
                        for job in vault.jobs.all()]
            if job_list:
                print(*job_list, sep="\n")

    def vault_list(self):
        print(*[vault.name for vault in self.resource.vaults.all()],
                sep="\n")

    def vault_create(self):
        self.resource.create_vault(vaultName=self.args.name)

    def vault_delete(self):
        all_vaults = self.resource.vaults.all()
        for vault in all_vaults:
            if vault.name == self.args.name:
                vault.delete()
                return True
        raise RuntimeError('Could not find vault {}'.format(self.args.name))

    def _vault_sync_reconcile(self, vault, job, fix=False):
        job_output = job.get_output()
        response = job_output['body'].read()
        response = json.loads(response)
        inventory_date = iso8601_to_unix_timestamp(response['InventoryDate'])
        job_creation_date = iso8601_to_unix_timestamp(job.creation_date)
        seen_ids = []
        for archive in response['ArchiveList']:
            id = archive['ArchiveId']
            name = archive['ArchiveDescription']
            creation_date = iso8601_to_unix_timestamp(archive['CreationDate'])
            self.cache.mark_seen_upstream(
                vault=vault.name,
                id=id,
                name=name,
                upstream_creation_date=creation_date,
                upstream_inventory_date=inventory_date,
                upstream_inventory_job_creation_date=job_creation_date,
                fix=fix)
            seen_ids.append(id)
        self.cache.mark_only_seen(vault.name, inventory_date, seen_ids,
                                  fix=fix)
        self.cache.mark_commit()

    def _vault_sync(self, vault_name, max_age_hours, fix, wait):
        vault = self.resource.Vault('-', vault_name)
        inventory_jobs = find_inventory_jobs(vault,
                                             max_age_hours=max_age_hours)

        complete_job = find_complete_job(inventory_jobs)
        if complete_job:
            self._vault_sync_reconcile(vault, complete_job, fix=fix)
        elif has_pending_job(inventory_jobs):
            if wait:
                complete_job = wait_until_job_completed(inventory_jobs)
            else:
                raise RetryConsoleError('job still pending for inventory on %r' %
                                        vault.name)
        else:
            job_id = vault.initiate_inventory_retrieval()
            job = vault.Job(job_id)
            if wait:
                wait_until_job_completed([job])
                self._vault_sync_reconcile(vault, job, fix=fix)
            else:
                raise RetryConsoleError('queued inventory job for %r' %
                        vault.name)

    def vault_sync(self):
        return self._vault_sync(vault_name=self.args.name,
                                max_age_hours=self.args.max_age_hours,
                                fix=self.args.fix,
                                wait=self.args.wait)

    def archive_list(self):
        if self.args.force_ids:
            archive_list = list(self.cache.get_archive_list_with_ids(
                self.args.vault))
        else:
            archive_list = list(self.cache.get_archive_list(self.args.vault))

        if archive_list:
            print(*archive_list, sep="\n")

    def archive_upload(self):
        # XXX: "Leading whitespace in archive descriptions is removed."
        # XXX: "The description must be less than or equal to 1024 bytes. The
        #       allowable characters are 7 bit ASCII without control codes,
        #       specifically ASCII values 32-126 decimal or 0x20-0x7E
        #       hexadecimal."
        if self.args.name is not None:
            name = self.args.name
        else:
            try:
                full_name = self.args.file.name
            except:
                raise RuntimeError('Archive name not specified. Use --name')
            name = os.path.basename(full_name)

        file = self.args.file
        multipart_size = self.args.multipart_size
        verbose('Uploading archive with multipart size={}'.format(multipart_size))
        file.seek(0, 2)  # move to end of file
        file_size = file.tell()
        file.seek(0)
        file_tree_hash = botocore.utils.calculate_tree_hash(file)
        file.seek(0)

        vault = self.resource.Vault('-', self.args.vault)
        if file_size < multipart_size:
            verbose('Uploading in single upload')
            archive = vault.upload_archive(
                archiveDescription=name,
                body=file
            )
            self.cache.add_archive(self.args.vault, name, archive)
        else:
            multipart = None
            try:
                verbose('Uploading in multi-part upload')
                multipart = vault.initiate_multipart_upload(
                    archiveDescription=name,
                    partSize=str(multipart_size)
                )

                def _upload(start_byte, end_byte, chunk_num):
                    verbose('Uploading bytes {}-{} (Chunk {} of {})'.format(start_byte, end_byte - 1, chunk_num, chunks))
                    wrapped_reader = WrappedFile(file, start_byte, end_byte)
                    multipart.upload_part(
                        range='bytes {}-{}/*'.format(start_byte, end_byte - 1),
                        body=wrapped_reader
                    )

                whole_parts = file_size // multipart_size
                chunks = whole_parts
                remainder = file_size % multipart_size
                if remainder:
                    chunks += 1
                for chunk_num, first_byte in enumerate(xrange(0, whole_parts * multipart_size,
                                                             multipart_size)):
                    _upload(first_byte, first_byte + multipart_size, chunk_num=chunk_num+1)
                if remainder:
                    _upload(file_size-remainder, file_size, chunk_num=chunks)

                response = multipart.complete(
                    archiveSize=str(file_size),
                    checksum=file_tree_hash
                )
                archive = vault.Archive(response['archiveId'])
                self.cache.add_archive(self.args.vault, name, archive)
                verbose('Multipart upload complete')
            except Exception, e:
                warn('Unhandled exception during multi-part upload: {} {}'.format(type(e), e))
                if multipart:
                    multipart.abort()
                    verbose('Multipart upload aborted')

    @staticmethod
    def _write_archive_retrieval_job(args, f, job, multipart_size):
        if job.archive_size_in_bytes > multipart_size:

            def fetch(start, end, chunk_num):
                byte_range = start, end-1
                verbose('Fetching multipart byte range {}-{} (Chunk {} of {})'.format(byte_range[0], byte_range[1], chunk_num, chunks))
                response = job.get_output(range='bytes={}-{}'.format(*byte_range))
                data = response['body'].read()
                f.write(data)

            whole_parts = job.archive_size_in_bytes // multipart_size
            chunks = whole_parts
            remainder = job.archive_size_in_bytes % multipart_size
            if remainder:
                chunks += 1
            for chunk_num, first_byte in enumerate(xrange(0, whole_parts * multipart_size, multipart_size)):
                fetch(first_byte, first_byte + multipart_size, chunk_num=chunk_num+1)
            if remainder:
                fetch(job.archive_size_in_bytes - remainder, job.archive_size_in_bytes, chunk_num=chunks)
        else:
            verbose('Fetching entire byte range')
            response = job.get_output()
            f.write(response['body'].read())

        # Make sure that the file now exactly matches the downloaded archive,
        # even if the file existed before and was longer.
        try:
            f.truncate(job.archive_size_in_bytes)
        except IOError as e:
            # Allow ESPIPE, since the "file" couldn't have existed before in
            # this case.
            if e.errno != errno.ESPIPE:
                raise

        f.flush()

        # Verify tree hash to make sure we have the full content uncorrupted
        if args.output_filename != '-':
            if botocore.utils.calculate_tree_hash(open(f.name, 'rb')) != job.sha256_tree_hash:
                raise ConsoleError('SHA256 Tree Hash does not match Glacier Archive. Download is likely corrupt.')
        else:
            warn("File saved to stdout cannot have it's SHA256 Tree Hash verified")


    @classmethod
    def _archive_retrieve_completed(cls, args, job, name):
        if args.output_filename == '-':
            cls._write_archive_retrieval_job(
                args, sys.stdout, job, args.multipart_size)
        else:
            if args.output_filename:
                filename = args.output_filename
            else:
                filename = os.path.basename(name)
            with open(filename, 'wb') as f:
                cls._write_archive_retrieval_job(args, f, job, args.multipart_size)

    def archive_retrieve_one(self, name):
        try:
            archive_id = self.cache.get_archive_id(self.args.vault, name)
        except KeyError:
            raise ConsoleError('archive %r not found' % name)

        vault = self.resource.Vault('-', self.args.vault)
        retrieval_jobs = find_retrieval_jobs(vault, archive_id)

        complete_job = find_complete_job(retrieval_jobs)
        if complete_job:
            self._archive_retrieve_completed(self.args, complete_job, name)
        elif has_pending_job(retrieval_jobs):
            if self.args.wait:
                complete_job = wait_until_job_completed(retrieval_jobs)
                self._archive_retrieve_completed(self.args, complete_job, name)
            else:
                raise RetryConsoleError('job still pending for archive %r' % name)
        else:
            # create an archive retrieval job
            archive = vault.Archive(archive_id)
            job = archive.initiate_archive_retrieval()
            if self.args.wait:
                wait_until_job_completed([job])
                self._archive_retrieve_completed(self.args, job, name)
            else:
                raise RetryConsoleError('queued retrieval job for archive %r' % name)

    def archive_retrieve(self):
        if len(self.args.names) > 1 and self.args.output_filename:
            raise ConsoleError('cannot specify output filename with multi-archive retrieval')
        success_list = []
        retry_list = []
        for name in self.args.names:
            try:
                self.archive_retrieve_one(name)
            except RetryConsoleError as e:
                retry_list.append(e.message)
            else:
                success_list.append('retrieved archive %r' % name)
        if retry_list:
            message_list = success_list + retry_list
            raise RetryConsoleError("\n".join(message_list))

    def archive_delete(self):
        try:
            archive_id = self.cache.get_archive_id(
                self.args.vault, self.args.name)
        except KeyError:
            raise ConsoleError('archive %r not found' % self.args.name)
        vault = self.resource.Vault('-', self.args.vault)
        vault.Archive(archive_id).delete()
        self.cache.delete_archive(self.args.vault, self.args.name)

    def archive_checkpresent(self):
        try:
            last_seen = self.cache.get_archive_last_seen(
                self.args.vault, self.args.name)
        except KeyError:
            if self.args.wait:
                last_seen = None
            else:
                if not self.args.quiet:
                    print(
                        'archive %r not found' % self.args.name,
                        file=sys.stderr)
                return

        def too_old(last_seen):
            return (not last_seen or
                    not self.args.max_age_hours or
                    (last_seen <
                        time.time() - self.args.max_age_hours * 60 * 60))

        if too_old(last_seen):
            # Not recent enough
            try:
                self._vault_sync(vault_name=self.args.vault,
                                 max_age_hours=self.args.max_age_hours,
                                 fix=False,
                                 wait=self.args.wait)
            except RetryConsoleError:
                pass
            else:
                try:
                    last_seen = self.cache.get_archive_last_seen(
                        self.args.vault, self.args.name)
                except KeyError:
                    if not self.args.quiet:
                        print(('archive %r not found, but it may ' +
                                           'not be in the inventory yet')
                                           % self.args.name, file=sys.stderr)
                    return

        if too_old(last_seen):
            if not self.args.quiet:
                print(('archive %r found, but has not been seen ' +
                                   'recently enough to consider it present') %
                                   self.args.name, file=sys.stderr)
            return

        print(self.args.name)


    def parse_args(self, args=None):
        parser = argparse.ArgumentParser()
        parser.add_argument('--config', help='configuration INI file to use', default=None)
        parser.add_argument('--region', default=None)
        parser.add_argument('--verbose', action='store_true')
        subparsers = parser.add_subparsers()
        config_subparser = subparsers.add_parser('config').add_subparsers()
        config_subparser.add_parser('write_default').set_defaults(func=self.write_default_config)
        vault_subparser = subparsers.add_parser('vault').add_subparsers()
        vault_subparser.add_parser('list').set_defaults(func=self.vault_list)
        vault_create_subparser = vault_subparser.add_parser('create')
        vault_create_subparser.set_defaults(func=self.vault_create)
        vault_create_subparser.add_argument('name')
        vault_delete_subparser = vault_subparser.add_parser('delete')
        vault_delete_subparser.set_defaults(func=self.vault_delete)
        vault_delete_subparser.add_argument('name')
        vault_sync_subparser = vault_subparser.add_parser('sync')
        vault_sync_subparser.set_defaults(func=self.vault_sync)
        vault_sync_subparser.add_argument('name', metavar='vault_name')
        vault_sync_subparser.add_argument('--wait', action='store_true')
        vault_sync_subparser.add_argument('--fix', action='store_true')
        vault_sync_subparser.add_argument('--max-age', type=int, default=24,
                                          dest='max_age_hours')
        archive_subparser = subparsers.add_parser('archive').add_subparsers()
        archive_list_subparser = archive_subparser.add_parser('list')
        archive_list_subparser.set_defaults(func=self.archive_list)
        archive_list_subparser.add_argument('--force-ids', action='store_true')
        archive_list_subparser.add_argument('vault')
        archive_upload_subparser = archive_subparser.add_parser('upload')
        archive_upload_subparser.set_defaults(func=self.archive_upload)
        archive_upload_subparser.add_argument('vault')
        archive_upload_subparser.add_argument('file',
                                              type=argparse.FileType('rb'))
        archive_upload_subparser.add_argument('--name')
        archive_upload_subparser.add_argument('--multipart-size', type=int,
                default=(32*1024*1024))
        archive_retrieve_subparser = archive_subparser.add_parser('retrieve')
        archive_retrieve_subparser.set_defaults(func=self.archive_retrieve)
        archive_retrieve_subparser.add_argument('vault')
        archive_retrieve_subparser.add_argument('names', nargs='+',
                                                metavar='name')
        archive_retrieve_subparser.add_argument('--multipart-size', type=int,
                default=(8*1024*1024))
        archive_retrieve_subparser.add_argument('-o', dest='output_filename',
                                                metavar='OUTPUT_FILENAME')
        archive_retrieve_subparser.add_argument('--wait', action='store_true')
        archive_delete_subparser = archive_subparser.add_parser('delete')
        archive_delete_subparser.set_defaults(func=self.archive_delete)
        archive_delete_subparser.add_argument('vault')
        archive_delete_subparser.add_argument('name')
        archive_checkpresent_subparser = archive_subparser.add_parser(
                'checkpresent')
        archive_checkpresent_subparser.set_defaults(
                func=self.archive_checkpresent)
        archive_checkpresent_subparser.add_argument('vault')
        archive_checkpresent_subparser.add_argument('name')
        archive_checkpresent_subparser.add_argument('--wait',
                                                    action='store_true')
        archive_checkpresent_subparser.add_argument('--quiet',
                                                    action='store_true')
        archive_checkpresent_subparser.add_argument(
                '--max-age', type=int, default=80, dest='max_age_hours')
        job_subparser = subparsers.add_parser('job').add_subparsers()
        job_subparser.add_parser('list').set_defaults(func=self.job_list)
        return parser.parse_args(args)

    def __init__(self, args=None, resource=None, cache=None):
        global verbose
        args = self.parse_args(args)

        configuration.read(args.config)

        if args.verbose:
            verbose = real_verbose

        if resource is None:
            resource = boto3.resource('glacier', region_name=args.region)

        if cache is None:
            cache = Cache(get_cache_key(), configuration['database']['driver'])

        self.resource = resource
        self.cache = cache
        self.args = args

    def main(self):
        try:
            self.args.func()
        except KeyboardInterrupt:
            sys.exit(130)  # Interrupted with CTRL+C
        except RetryConsoleError as e:
            message = insert_prefix_to_lines(PROGRAM_NAME + ': ', str(e))
            print(message, file=sys.stderr)
            # From sysexits.h:
            #     "temp failure; user is invited to retry"
            sys.exit(75)  # EX_TEMPFAIL
        except ConsoleError as e:
            message = insert_prefix_to_lines(PROGRAM_NAME + ': ', str(e))
            print(message, file=sys.stderr)
            sys.exit(1)
        except RuntimeError as e:
            message = insert_prefix_to_lines(PROGRAM_NAME + ': ', str(e))
            print(message, file=sys.stderr)
            sys.exit(1)


def main():
    App().main()


if __name__ == '__main__':
    main()
