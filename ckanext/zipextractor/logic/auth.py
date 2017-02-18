from ckan.logic import get_or_bust
from ckan.logic.auth import create as auth_create, get as auth_get


def spatialingestor_job_submit(context, data):
    res_id = get_or_bust(data, 'resource_id')

    return auth_create.resource_create(context, {'id': res_id})


def spatialingestor_status(context, data):
    res_id = get_or_bust(data, 'resource_id')

    return auth_get.resource_show(context, {'id': res_id})