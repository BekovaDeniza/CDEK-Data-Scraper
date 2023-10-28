import logging
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
        def wrapper(self, grab, task):
            logger = logging.getLogger(__name__)

            if not self.check_valid_domain(task, grab.doc.url):
                logger.info('Redirect to another domain [task %s -> %s]', task, grab.doc.url)
                return

            if grab.doc.code in [404]:
                return

            _validator = validator or getattr(self, 'validation_text', None) or getattr(self, 'validator', None)

            if not isinstance(_validator, list):
                _validator = [_validator]

            if not any(_validate(v, grab, task) for v in _validator):
                if grab.doc.code in [301, 302]:
                    logger.info('Invalid response: code=%s, url=%s -> %s',
                                grab.doc.code, task.url, grab.doc.headers['Location'])
                else:
                    logger.info('Invalid response: code=%s, url=%s', grab.doc.code, task.url)

                if not raise_exception:
                    return

                raise ValidationError('Invalid response')

            try:
                for task in fn(self, grab, task) or ():
                    yield task
            except Exception as e:
                logger.error('Error in the decorated function: %s', str(e))
                raise

        return wrapper

    return wrap
