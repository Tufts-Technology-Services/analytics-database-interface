
class APIError(Exception):
    code = 500
    description = 'Unexpected Error'


class NoMatchFoundError(APIError):
    code = 404
    description = 'No match found'


class InvalidEmailAddressError(APIError):
    code = 400
    description = 'Invalid Email Address'


class InvalidCourseCatalogNumberError(APIError):
    code = 400
    description = 'Malformed Course Catalog Number'


class FileUploadError(APIError):
    code = 400
    description = 'File upload error'