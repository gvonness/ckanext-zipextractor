import logging

from ckan import model
from ckan.plugins import toolkit
from pylons import config

log = logging.getLogger('ckanext_zipextractor')


def get_microservice_metadata():
    symbols = {
        'customary': ('b', 'k', 'm', 'g', 't', 'p', 'e', 'z', 'y'),
        'customary_ext': ('byte', 'kilo', 'mega', 'giga', 'tera', 'peta', 'exa',
                          'zetta', 'iotta'),
        'iec': ('bi', 'ki', 'mi', 'gi', 'ti', 'pi', 'ei', 'zi', 'yi'),
        'iec_ext': ('byte', 'kibi', 'mebi', 'gibi', 'tebi', 'pebi', 'exbi',
                    'zebi', 'yobi'),
        'standard': ('b', 'kb', 'mb', 'gb', 'tb', 'pb', 'eb', 'zb', 'yi'),
    }

    def human2bytes(s):
        num = ""
        while s and s[0:1].isdigit() or s[0:1] == '.':
            num += s[0]
            s = s[1:]
        num = float(num)
        suffix = s.strip().lower()

        for name, sset in symbols.items():
            if suffix in sset:
                return int(num * (1 << sset.index(suffix) * 10))

        return int(num)

    for config_option in ('ckan.zipextractor.target_formats',):
        if not config.get(config_option):
            raise Exception(
                'Config option `{0}` must be set to use the Zip Extractor.'.format(config_option))

    return {
        'max_zip_resource_filesize': human2bytes(config.get('ckan.zipextractor.max_zip_resource_filesize', '100MB')),
        'target_zip_formats': list(
            set([x.upper() for x in toolkit.aslist(config.get('ckan.zipextractor.target_formats', []))]))
        }


def is_resource_blacklisted(resource):
    package = toolkit.get_action('package_show')({'ignore_auth': True}, {
        'id': resource['package_id'],
    })

    org_blacklist = list(set(toolkit.aslist(config.get('ckan.zipextractor.org_blacklist', []))))
    pkg_blacklist = list(set(toolkit.aslist(config.get('ckan.zipextractor.pkg_blacklist', []))))
    user_blacklist = list(
        set(map(lambda x: model.User.get(x).id, toolkit.aslist(config.get('ckan.zipextractor.user_blacklist', [])))))

    if package['organization']['name'] in org_blacklist:
        log.error("{0} in organization blacklist".format(package['organization']['name']))
        return True
    elif package['name'] in pkg_blacklist:
        log.error("{0} in package blacklist".format(package['name']))
        return True
    else:
        activity_list = toolkit.get_action('package_activity_list')({'ignore_auth': True}, {
            'id': package['id'],
        })

        last_user = package['creator_user_id']
        if activity_list:
            last_user = activity_list[0]['user_id']

        if last_user in user_blacklist:
            log.error("{0} was last edited by blacklisted user".format(activity_list[0]['user_id']))
            return True

    return False


def get_zip_input_format(resource):
    check_string = resource.get('__extras', {}).get('format', resource.get('format', resource.get('url', ''))).upper()
    if check_string.endswith("ZIP"):
        return 'ZIP'
    else:
        return None


def is_zip_extractable_resource(resource):
    return get_zip_input_format(resource) and not is_resource_blacklisted(resource)


def zipextractor_status_description(status):
    _ = toolkit._

    if status.get('status'):
        captions = {
            'complete': _('Complete'),
            'pending': _('Pending'),
            'submitting': _('Submitting'),
            'error': _('Error'),
        }

        return captions.get(status['status'], status['status'].capitalize())
    else:
        return _('Not Uploaded Yet')
