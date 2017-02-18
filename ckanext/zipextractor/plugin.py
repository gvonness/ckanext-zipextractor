from ckan import logic, model, plugins
from ckan.common import _
from ckan.lib import base
from ckan.lib import helpers as core_helpers
from ckan.plugins import toolkit
from pylons import config

from ckanext.zipextractor import helpers
from ckanext.zipextractor.logic import auth, action


class ZipExtractorPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.IAuthFunctions)
    plugins.implements(plugins.IResourceUrlChange)
    plugins.implements(plugins.IDomainObjectModification, inherit=True)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IRoutes, inherit=True)

    legacy_mode = False
    resource_show_action = None
    d_type = model.domain_object.DomainObjectOperation

    def update_config(self, config):
        toolkit.add_template_directory(config, 'templates')

    def notify(self, entity, operation=None):
        if isinstance(entity, model.Resource):
            resource_dict = model.Resource.get(entity.id).as_dict()
            if helpers.is_zip_extractable_resource(resource_dict):
                d_type = model.domain_object.DomainObjectOperation
                auto_extract = toolkit.asbool(config.get('ckan.zipextractor.auto_extract', 'False'))
                is_zip_parent = toolkit.asbool(resource_dict.get('zip_parent', 'False'))
                marked_to_extract = toolkit.asbool(resource_dict.get('zip_extract', 'False'))
                if operation == d_type.deleted or entity.state == 'deleted':

                    package_dict = model.Package.get(resource_dict['package_id']).as_dict()
                    if package_dict['state'] != 'deleted':
                        helpers.log.error(">>>>>>> Registered Orphan Delete Trigger")
                        package_dict['resource_ids_to_delete'] = [entity.id]
                        toolkit.get_action('zipextractor_delete_orphaned_resources')({}, package_dict)
                elif (is_zip_parent and (operation == d_type.changed or not operation)) or (
                                operation == d_type.new and (auto_extract or marked_to_extract)):
                    helpers.log.error(">>>>>>> Registered Ingest Trigger")
                    toolkit.get_action('zipextractor_extract_resource')({}, resource_dict)

    def before_map(self, m):
        m.connect(
            'resource_zipextract', '/resource_zipextract/{resource_id}',
            controller='ckanext.zipextractor.plugin:ResourceZipController',
            action='resource_zipextract', ckan_icon='cloud-upload')
        return m

    def get_actions(self):
        return {'zipextractor_job_submit': action.zipextractor_job_submit,
                'zipextractor_hook': action.zipextractor_hook,
                'zipextractor_status': action.zipextractor_status,
                'zipextractor_extract_resource': action.extract_resource,
                'zipextractor_delete_orphaned_resources': action.delete_orphaned_resources}

    def get_auth_functions(self):
        return {'zipextractor_job_submit': auth.zipextractor_job_submit,
                'zipextractor_status': auth.zipextractor_status}

    def get_helpers(self):
        return {'zipextractor_status_description': helpers.zipextractor_status_description,
                'zipextractor_is_zip_extractable_resource': helpers.is_zip_extractable_resource}


class ResourceZipController(base.BaseController):
    def resource_zipextract(self, resource_id):
        if toolkit.request.method == 'POST':
            try:
                resource_dict = toolkit.get_action('resource_show')({}, {'id': resource_id})
                toolkit.get_action('zipextractor_extract_resource')({}, resource_dict)
            except logic.ValidationError:
                pass

            base.redirect(core_helpers.url_for(
                controller='ckanext.zipextractor.plugin:ResourceZipController',
                action='resource_zipextract',
                resource_id=resource_id)
            )
        try:
            toolkit.c.resource = toolkit.get_action('resource_show')(
                None, {'id': resource_id}
            )
            toolkit.c.pkg_dict = toolkit.get_action('package_show')(
                None, {'id': toolkit.c.resource['package_id']}
            )
        except logic.NotFound:
            base.abort(404, _('Resource not found'))
        except logic.NotAuthorized:
            base.abort(401, _('Unauthorized to edit this resource'))

        try:
            zipextractor_status = toolkit.get_action('zipextractor_status')(None, {
                'resource_id': resource_id,
                'job_type': 'zip_extract'
            })
        except logic.NotFound:
            zipextractor_status = {}
        except logic.NotAuthorized:
            base.abort(401, _('Not authorized to see this page'))

        return base.render('package/resource_zipextract.html',
                           extra_vars={'status': zipextractor_status})
