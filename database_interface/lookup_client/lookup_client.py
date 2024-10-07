from database_interface import DatabaseInterface
from .errors import InvalidEmailAddressError, InvalidCourseCatalogNumberError, APIError, NoMatchFoundError


class LookupClient:
    """
    class for looking up data in the database
    """
    def __init__(self, database_interface: DatabaseInterface):
        self.dbi = database_interface        

    def get_email(self, utln):
        r = self.dbi.fetch(f"SELECT pr_identity_email FROM pr_fis_vw WHERE pr_identity_utln = '{utln}'")
        if len(r) == 0:
            raise NoMatchFoundError(f'No match found for utln {utln}')
        return r[0][0]

    def utln_lookup(self, utln):
        fields = [{"src": "pr_identity_utln", "tgt": "utln", "type": "keyword"}, 
                  {"src": "pr_identity_email", "tgt": "email", "type": "keyword"},
                  {"src": "pr_identity_firstname", "tgt": "first_name", "type": "keyword"},
                  {"src": "pr_identity_lastname", "tgt": "last_name", "type": "keyword"},
                  {"src": "user_primary_affiliation", "tgt": "primary_affiliation", "type": "keyword"},
                  {"src": "user_primary_title", "tgt": "primary_title", "type": "keyword"},
                  {"src": "user_primary_division", "tgt": "division", "type": "keyword"},
                  {"src": "user_primary_dept_prog", "tgt": "dept_prog", "type": "keyword"},
                  {"src": "user_primary_campus", "tgt": "campus", "type": "keyword"},
                  {"src": "user_profile_link", "tgt": "profile_link", "type": "keyword"},
                  {"src": "user_pr_status", "tgt": "is_active", "type": "boolean"}]

        sql = f"""
          SELECT pr_identity_utln, pr_identity_email, pr_identity_firstname, pr_identity_lastname, 
          user_primary_affiliation, pr_hr_title, pr_med_title, user_primary_division, user_primary_dept_prog, 
          user_primary_campus, user_profile_link, user_pr_status FROM pr_fis WHERE pr_identity_utln = '{utln}'
          """
        r = self.dbi.fetch(sql)
        if len(r) == 0:
            raise NoMatchFoundError(f'No match found for utln {utln}')
        return r[0]

    def email_lookup(self, email):
        r = self.dbi.fetch(f"SELECT pr_identity_utln as utln, pr_identity_email as email FROM pr_fis_vw WHERE pr_identity_email = '{email}'")
        if len(r) == 0:
            raise NoMatchFoundError(f'No match found for email {email}')
        return r[0][0]

    def course_lookup(self, course_num):
        """
        [
  {
    "src_name": "subject_cat_nbr",
    "tgt_name": "catalog_no",
    "tgt_type": "keyword",
    "normalizer": "std_normalizer"
  },
  {
    "src_name": "title",
    "tgt_name": "title",
    "tgt_type": "keyword",
    "normalizer": "std_normalizer"
  },
  {
    "src_name": "course_school",
    "tgt_name": "school",
    "tgt_type": "keyword",
    "normalizer": "std_normalizer"
  },
  {
    "src_name": "course_dept_prog",
    "tgt_name": "dept_prog",
    "tgt_type": "keyword",
    "normalizer": "std_normalizer"
  },
  {
    "src_name": "campus_ld",
    "tgt_name": "campus_long",
    "tgt_type": "keyword",
    "normalizer": "std_normalizer"
  },
  {
    "src_name": "campus_std",
    "tgt_name": "campus",
    "tgt_type": "keyword",
    "normalizer": "std_normalizer"
  },
  {
    "src_name": "course_title",
    "tgt_name": "title_display",
    "tgt_type": "keyword",
    "normalizer": "std_normalizer"
  }
]
        """
        return self.dbi.fetch(f"SELECT pr_identity_utln as utln, pr_identity_email as email FROM pr_fis_vw WHERE pr_identity_email = '{course_num}'")

    def jira_tag_from_mapping(self, tag):
        pass

    def get_tag_mappings(self):
        return []

    def get_tag_from_techconnect(self, tag):
        tags = self.get_tag_mappings()
        match = [i for i in tags if i['snow_tag'] == tag]
        if len(match) == 0:
            return None
        elif len(match) == 1:
            return match[0]
        else:
            raise APIError('too many matches in tag set.. please review')




def normalize_email_address(email_address):
    """
    normalize email addresses passed to the API. lowercases and checks for the right number of parts, tufts.edu domain
    :param email_address:
    :return:
    """
    em_parts = [] if email_address is None else email_address.split('@')
    if len(em_parts) != 2 or len(em_parts[0]) == 0 or len(em_parts[0]) > 50:
        raise InvalidEmailAddressError('Malformed email address')
    if len(em_parts[1].split('.')) < 2 or len(em_parts[1]) > 50:
        raise InvalidEmailAddressError('Malformed email domain')
    return f'{em_parts[0].strip().lower()}@{em_parts[1].strip().lower()}'


def normalize_course_cat_no(course_catalog_no):
    """
    normalize course catalog numbers passed to the API.
    :param course_catalog_no:
    :return:
    """
    if course_catalog_no is None:
        course_catalog_no = ''
    pieces = course_catalog_no.strip().split()
    if len(pieces) >= 2:
        return ' '.join(pieces[:2]).lower()
    else:
        pieces = course_catalog_no.strip().split('_')
        if len(pieces) >=2:
            return ' '.join(pieces[:2]).lower()
    raise InvalidCourseCatalogNumberError('Malformed Course Catalog number')