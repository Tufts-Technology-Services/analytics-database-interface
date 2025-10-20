from database_interface import DatabaseInterface
from .errors import InvalidEmailAddressError, InvalidCourseCatalogNumberError, APIError, NoMatchFoundError


class LookupClient:
    """
    class for looking up data in the database
    """
    def __init__(self, database_interface: DatabaseInterface):
        self.dbi = database_interface        

    def get_email(self, utln):
        utln = utln.strip().lower()
        r = self.dbi.fetch(f"SELECT pr_identity_email FROM pr_fis WHERE pr_identity_utln = '{utln}'")
        if len(r) == 0:
            raise NoMatchFoundError(f'No match found for utln {utln}')
        return r[0][0]

    def utln_lookup(self, utln):
        utln = utln.strip().lower()
        fields = [{"src": "pr_identity_utln", "tgt": "utln", "type": "keyword"}, 
                  {"src": "pr_identity_email", "tgt": "email", "type": "keyword"},
                  {"src": "pr_identity_firstname", "tgt": "first_name", "type": "keyword"},
                  {"src": "pr_identity_lastname", "tgt": "last_name", "type": "keyword"},
                  {"src": "user_primary_affiliation", "tgt": "primary_affiliation", "type": "keyword"},
                  {"src": "pr_hr_title", "tgt": "primary_title", "type": "keyword"},
                  {"src": "user_primary_division", "tgt": "division", "type": "keyword"},
                  {"src": "user_primary_dept_prog", "tgt": "dept_prog", "type": "keyword"},
                  {"src": "user_primary_campus", "tgt": "campus", "type": "keyword"},
                  {"src": "fis_public_url_fragment", "tgt": "profile_link", "type": "keyword"},
                  {"src": "user_pr_status", "tgt": "is_active", "type": "boolean"}]
        search_fields = ', '.join([f['src'] for f in fields])
        sql = f"""
          SELECT {search_fields} FROM pr_fis WHERE pr_identity_utln = '{utln}'
          """
        r = self.dbi.fetch(sql)
        if len(r) == 0:
            raise NoMatchFoundError(f'No match found for utln {utln}')
        info = {f['tgt']: r[0][i] for i, f in enumerate(fields)}
        if info['is_active'] is not None:
            info['is_active'] = True if info['is_active'].lower() == 'a' else False
        if info['profile_link'] is not None:
            info['profile_link'] = f'https://facultyprofiles.tufts.edu/{info["profile_link"]}'
        return info

    def email_lookup(self, email):
        em = LookupClient.normalize_email_address(email)
        r = self.dbi.fetch(f"SELECT pr_identity_utln as utln, pr_identity_email as email FROM pr_fis WHERE pr_identity_email = '{em}'")
        if len(r) == 0:
            raise NoMatchFoundError(f'No match found for email {email}')
        return r[0][0]

    def course_lookup(self, course_num):

        fields = [
            {
                "src": "subject_cat_nbr_of_record",
                "tgt": "catalog_no",
                "tgt_type": "keyword",
                "normalizer": "std_normalizer"
            },
            {
                "src": "title_of_record",
                "tgt": "title",
                "tgt_type": "keyword",
                "normalizer": "std_normalizer"
            },
            {
                "src": "course_school_of_record",
                "tgt": "school",
                "tgt_type": "keyword",
                "normalizer": "std_normalizer"
            },
            {
                "src": "course_dept_prog_of_record",
                "tgt": "dept_prog",
                "tgt_type": "keyword",
                "normalizer": "std_normalizer"
            },
            {
                "src": "campus_ld",
                "tgt": "campus_long",
                "tgt_type": "keyword",
                "normalizer": "std_normalizer"
            },
            {
                "src": "campus_std",
                "tgt": "campus",
                "tgt_type": "keyword",
                "normalizer": "std_normalizer"
            }
        ]

        course_num = LookupClient.normalize_course_cat_no(course_num)
        search_fields = ', '.join([f['src'] for f in fields])
        sql = f"""
          SELECT {search_fields} FROM courses_instructors WHERE subject_cat_nbr = '{course_num}'
          """
        r = self.dbi.fetch(sql)
        if len(r) == 0:
            raise NoMatchFoundError(f'No match found for course {course_num}')
        info = {f['tgt']: r[0][i] for i, f in enumerate(fields)}
        return info

    def get_rt_members(self):
        r = self.dbi.fetch("SELECT utln, pr_identity_email FROM rt_staff LEFT JOIN pr_fis ON rt_staff.utln = pr_fis.pr_identity_utln")
        return [{'utln': i[0], 'email': i[1]} for i in r]

    def get_tag_mappings(self):
        r = self.dbi.fetch("SELECT value, snow_tag FROM rt_service_areas union SELECT value, snow_tag FROM rt_components")
        return [{'jira_value': i[0], 'snow_tag': i[1], 'type': 'service_area' if i[1].startswith('sa.') else 'component'} for i in r]
    
    def jira_component_from_mapping(self, tag):
        tags = self.get_tag_mappings()
        match = [i for i in tags if i['jira_value'] == tag and i['type'] == 'component']
        if len(match) == 0:
            return None
        elif len(match) == 1:
            return match[0]
        else:
            raise APIError('too many matches in tag set.. please review')

    def jira_service_area_from_mapping(self, tag):
        tags = self.get_tag_mappings()
        match = [i for i in tags if i['jira_value'] == tag and i['type'] == 'service_area']
        if len(match) == 0:
            return None
        elif len(match) == 1:
            return match[0]
        else:
            raise APIError('too many matches in tag set.. please review')
    
    def get_tag_from_techconnect(self, tag):
        tags = self.get_tag_mappings()
        match = [i for i in tags if i['snow_tag'] == tag]
        if len(match) == 0:
            return None
        elif len(match) == 1:
            return match[0]
        else:
            raise APIError('too many matches in tag set.. please review')

    @staticmethod
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

    @staticmethod
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
            return ' '.join(pieces[:2]).upper()
        else:
            pieces = course_catalog_no.strip().split('_')
            if len(pieces) >=2:
                return ' '.join(pieces[:2]).upper()
        raise InvalidCourseCatalogNumberError('Malformed Course Catalog number')