try:
    from waterbutler import settings
except ImportError:
    settings = {}

config = settings.get('SHARELATEX_PROVIDER_CONFIG', {})


TEMP_URL_SECS = config.get('TEMP_URL_SECS', 100)
