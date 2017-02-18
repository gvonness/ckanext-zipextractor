# encoding: utf-8
import sys

from ckan import model
from ckan.lib import cli
from ckan.plugins import toolkit
from pylons import config

from ckanext.zipextractor.helpers import log
from ckanext.zipextractor.logic.action import extract_resource, delete_orphaned_resources


class SpatialIngestorCommand(cli.CkanCommand):
    '''Perform commands in the zipextractor
    Usage:
        purge <pkgname> - Purges zip child resources from pkgname
        purgeall - Purges zip child resources from all packages
        reextract <pkgname> - Reextract child resources from pkgname
        reextractall - Reextract all resources from all packages
    '''

    summary = __doc__.split('\n')[0]
    usage = __doc__

    def command(self):
        if self.args and self.args[0] == 'purge':
            if len(self.args) != 2:
                print "This command requires an argument\n"
                print self.usage
                sys.exit(1)

            self._load_config()
            self._purge(self.args[1])
        elif self.args and self.args[0] == 'purgeall':
            self._confirm_or_abort()

            self._load_config()
            self._purge_all()
        elif self.args and self.args[0] == 'reextract':
            self._confirm_or_abort()

            if len(self.args) != 2:
                print "This command requires an argument\n"
                print self.usage
                sys.exit(1)

            self._load_config()
            self._reingest(self.args[1])
        elif self.args and self.args[0] == 'reextractall':
            self._confirm_or_abort()

            self._load_config()
            self._reingest_all()
        else:
            print self.usage

    def _purge(self, pkg_id):
        pkg_dict = model.Package.get(pkg_id).as_dict()

        log.info("Purging spatially ingested resources from package {0}...".format(pkg_dict['name']))

        context = {'user': toolkit.get_action('user_show')({'ignore_auth': True}, {
            'id': config.get('ckan.zipextractor.ckan_user', 'default')
        })}

        pkg_dict['resource_ids_to_delete'] = [r['id'] for r in pkg_dict['resources']]
        delete_orphaned_resources(context, pkg_dict)

    def _purge_all(self):
        context = {'user': toolkit.get_action('user_show')({'ignore_auth': True}, {
            'id': config.get('ckan.zipextractor.ckan_user', 'default')
        })}

        pkg_ids = [r[0] for r in model.Session.query(model.Package.id).filter(model.Package.state != 'deleted').all()]

        log.info("Purging zip extracted resources from all packages...")

        total_packages = len(pkg_ids)
        for counter, pkg_id in enumerate(pkg_ids):
            sys.stdout.write(
                "\rPurging zip extracted resources from dataset {0}/{1}".format(counter + 1, total_packages))
            sys.stdout.flush()
            pkg_dict = model.Package.get(pkg_id).as_dict()
            try:
                pkg_dict['resource_ids_to_delete'] = [r['id'] for r in pkg_dict['resources']]
                delete_orphaned_resources(context, pkg_dict)
            except Exception, e:
                log.error("Processing {0} failed with error {1}, continuing...".format(pkg_dict['name'], str(e)))

        sys.stdout.write("\n>>> Process complete\n")

    def _reingest(self, pkg_id):
        pkg_dict = model.Package.get(pkg_id).as_dict()

        log.info("Re-extracting zip resources for package {0}...".format(pkg_dict['name']))

        context = {'user': toolkit.get_action('user_show')({'ignore_auth': True}, {
            'id': config.get('ckan.zipextractor.ckan_user', 'default')
        })}

        for res in pkg_dict['resources']:
            resource_dict = model.Resource.get(res['id']).as_dict()
            extract_resource(context, resource_dict)

    def _reextract_all(self):
        context = {'user': toolkit.get_action('user_show')({'ignore_auth': True}, {
            'id': config.get('ckan.zipextractor.ckan_user', 'default')
        })}

        pkg_ids = [r[0] for r in model.Session.query(model.Package.id).filter(model.Package.state != 'deleted').all()]

        log.info("Re-extracting zip resources for all packages...")

        total_packages = len(pkg_ids)
        for counter, pkg_id in enumerate(pkg_ids):
            sys.stdout.write("\rRe-extracting zip resources for dataset {0}/{1}".format(counter + 1, total_packages))
            sys.stdout.flush()
            pkg_dict = model.Package.get(pkg_id).as_dict()
            try:
                for res in pkg_dict['resources']:
                    resource_dict = model.Resource.get(res['id']).as_dict()
                    extract_resource(context, resource_dict)
            except Exception, e:
                log.error("Processing {0} failed with error {1}, continuing...".format(pkg_dict['name'], str(e)))

        sys.stdout.write("\n>>> Process complete\n")
