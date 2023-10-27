import re
from functools import wraps


class ValidationError(Exception):

    def __init__(self, message):
        super(ValidationError, self).__init__(message)


def _validate(validator, grab, task):
    if isinstance(validator, str):
        validation_result = validator in grab.doc.body
    elif callable(validator):
        validation_result = validator(grab, task)
    else:
        raise ValueError("Unsupported validator argument type")
    return validation_result


def validate_response(validator=None, save_path=None, raise_exception=True):
    def wrap(fn):
        @wraps(fn)
        def wraper(self, grab, task):
            if not self.check_valid_domain(task, grab.doc.url):
                print('Redirect to another domain [task {} -> {}]'.format(task, grab.doc.url))
                return

            if grab.doc.code in [404]:
                return

            _validator = validator

            if not _validator:
                if hasattr(self, 'validation_text'):
                    _validator = getattr(self, 'validation_text')
                if hasattr(self, 'validator'):
                    _validator = getattr(self, 'validator')
            else:
                _validator = _validator

            if not isinstance(_validator, list):
                _validator = [_validator]

            if True not in [_validate(v, grab, task) for v in _validator]:
                if grab.doc.code in [301, 302]:
                    print('Invalid response: code=%s, url=%s -> %s' % (
                        grab.doc.code, task.url, grab.doc.headers['Location']))
                else:
                    print('Invalid response: code=%s, url=%s' % (grab.doc.code, task.url))

                if not raise_exception:
                    return

                raise ValidationError('invalid response')

            try:
                for task in fn(self, grab, task) or ():
                    yield task
            except:
                raise

        return wraper

    return wrap