from database_interface import DatabaseInterface
from .errors import InvalidEmailAddressError, InvalidCourseCatalogNumberError


class LookupClient:
    """
    class for looking up data in the database
    """
    def __init__(self, database_interface: DatabaseInterface):
        self.dbi = database_interface        

    def get_email(self, utln):
        return self.dbi.fetch(f"SELECT pr_identity_email as email FROM pr_fis_vw WHERE pr_identity_utln = '{utln}'")

    def utln_lookup(self, utln):
        """
        def clean_strings(df, col_name):
    df[col_name] = df[col_name].str.replace('-', ' - ')
    df[col_name] = df[col_name].str.replace(r'\s+', ' ', regex=True)
    df[col_name] = df[col_name].str.replace('  ', ' ')
    df[col_name] = df[col_name].str.replace('_', ' ').str.strip()
    return df

    "filter": ["lowercase", "asciifolding"]
        [
  {
    "src_name": "pr_identity_utln",
    "tgt_name": "utln",
    "tgt_type": "keyword"
  },
  {
    "src_name": "pr_identity_prid",
    "tgt_name": "pr_id",
    "tgt_type": "keyword"
  },
  {
    "src_name": "fis_proprietary_id",
    "tgt_name": "fis_id",
    "tgt_type": "keyword"
  },
  {
    "src_name": "pr_identity_email",
    "tgt_name": "email",
    "tgt_type": "keyword",
    "normalizer": "std_normalizer"
  },
  {
    "src_name": "pr_identity_firstname",
    "tgt_name": "first_name",
    "tgt_type": "keyword",
    "normalizer": "std_normalizer"
  },
  {
    "src_name": "pr_identity_middlename",
    "tgt_name": "middle_name",
    "tgt_type": "keyword"
  },
  {
    "src_name": "pr_identity_lastname",
    "tgt_name": "last_name",
    "tgt_type": "keyword",
    "normalizer": "std_normalizer"
  },
  {
    "src_name": "user_primary_affiliation",
    "tgt_name": "primary_affiliation",
    "tgt_type": "keyword",
    "normalizer": "std_normalizer"
  },
  {
    "src_name": "user_primary_title",
    "tgt_name": "primary_title",
    "tgt_type": "keyword",
    "normalizer": "std_normalizer"
  },
  {
    "src_name": "user_primary_division",
    "tgt_name": "division",
    "tgt_type": "keyword",
    "normalizer": "std_normalizer"
  },
  {
    "src_name": "user_primary_dept_prog",
    "tgt_name": "dept_prog",
    "tgt_type": "keyword",
    "normalizer": "std_normalizer"
  },
  {
    "src_name": "user_primary_campus",
    "tgt_name": "campus",
    "tgt_type": "keyword",
    "normalizer": "std_normalizer"
  },
  {
    "src_name": "user_profile_link",
    "tgt_name": "profile_link",
    "tgt_type": "keyword"
  },
  {
    "src_name": "user_pr_status",
    "tgt_name": "is_active",
    "tgt_type": "boolean"
  }
]"""
        return self.dbi.fetch(f"SELECT pr_identity_utln as utln, pr_identity_email as email FROM pr_fis_vw WHERE pr_identity_utln = '{utln}'")


    def email_lookup(self, email):
        return self.dbi.fetch(f"SELECT pr_identity_utln as utln, pr_identity_email as email FROM pr_fis_vw WHERE pr_identity_email = '{email}'")


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
        return self.dbi.fetch(f"SELECT pr_identity_utln as utln, pr_identity_email as email FROM pr_fis_vw WHERE pr_identity_email = '{email}'")

    
    def get_tag_from_techconnect(self, tag):
        pass

    def jira_tag_from_mapping(self, tag):
        pass

    def get_tag_mappings(self):
        pass

    def get_tag_from_techconnect(self, tag):
        tags = self.get_tag_mappings()
        match = [i for i in tags if i['snow_tag'] == tag]
        if len(match) == 0:
            return None
        elif len(match) == 1:
            return match[0]
        else:
            raise Exception('too many matches in tag set.. please review')




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