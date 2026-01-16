import json
from django import template

register = template.Library()

@register.filter
def split(value, delimiter):
    if not value:
        return []
    return value.split(delimiter)

@register.filter
def json_load(value):
    try:
        return json.loads(value)
    except Exception:
        return {}
