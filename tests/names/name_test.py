import re
import numpy as np

from datagenius.names import Name


class TestName:
    def test_allocate(self):
        n = Name('William Cyrus Jehosephat')

        n._allocate()

        assert n.fname == 'William'
        assert n.mname == 'Cyrus'
        assert n.lname == 'Jehosephat'


    def test_intake(self):
        name = 'fiddleford mcgucket'

        n = Name(name)

        assert n.name_list == ['Fiddleford', 'McGucket']

        name_list = ['terry', 'tangela']

        n = Name(name_list)

        assert n.name_list == ['Terry', 'Tangela']

        name_list = ['Hammond', np.nan, 'Tulip']

        n = Name(name_list)

        assert n.name_list == ['Hammond', None, 'Tulip']


    def test_validate(self):
        n = Name('Bob')

        assert not n.valid

        n = Name('The Anderson Family')

        assert not n.valid

        n = Name('123 come with me')

        assert not n.valid

        n = Name([None, None, None])
        n._allocate()
        n._validate(True)

        assert not n.valid

        n = Name(['N', None, 'R'])
        n._allocate()
        n._validate(True)

        assert not n.valid


    def test_cleanse_invalid_chars(self):
        n = Name.cleanse_invalid_chars('@badinput')

        assert n == 'badinput'


    def test_cleanse_invalid_word(self):
        n = Name.cleanse_invalid_word('Family')

        assert n == ''

        n = Name.cleanse_invalid_word('CPA')

        assert n == ''

        n = Name.cleanse_invalid_word('Beauregard')

        assert n == 'Beauregard'

        n = Name.cleanse_invalid_word('The Annoying Family')

        assert n == 'Annoying'

        n = Name.cleanse_invalid_word('The Subscriber Family')

        assert n == ''


    def test_format_camelcase(self):
        string = 'mccloud'

        match_obj = re.search('mc[a-z]', string)

        assert Name.format_camelcase(string, match_obj.end() - 1) == 'mcCloud'

        string = "o'brien"

        match_obj = re.search("o'[a-z]", string)

        assert Name.format_camelcase(string, match_obj.end() - 1) == "o'Brien"

        string = 'aloysius-heitkamp'

        match_obj = re.search('-', string)

        assert Name.format_camelcase(string, match_obj.end()) == 'aloysius-Heitkamp'

        string = 'badinput-'

        match_obj = re.search('-', string)

        assert Name.format_camelcase(string, match_obj.end()) == 'badinput-'


    def test_manage_cases(self):
        n = Name('robert mccloud')

        assert n.name_list == ['Robert', 'McCloud']

        n = Name ("claire o'brien")

        assert n.name_list == ['Claire', "O'Brien"]

        n = Name('Sophia Aloysius-heitkamp')

        assert n.name_list == ['Sophia', 'Aloysius-Heitkamp']

        n = Name('BERNADETTE Q. HAMSTER')

        assert n.name_list == ['Bernadette', 'Q.', 'Hamster']

        n = Name(['eunice', 'haberdasher'])

        assert n.name_list == ['Eunice', 'Haberdasher']

        n = Name(['griffin', 'mcelroy'])

        assert n.name_list == ['Griffin', 'McElroy']


    def test_populate(self):
        n = Name('Bob Kevin Smith')

        record_dict = {
            'original_name': None,
            'valid': None,
            'fname': 'Bob Kevin Smith',
            'mname': None,
            'lname': None,
            'street1': '888 magnolia rd',
            'eaddress': 'bobksmith@gmail.com'
        }

        n.populate(record_dict)

        expected = {
            'original_name': 'Bob Kevin Smith',
            'valid': True,
            'fname': 'Bob',
            'mname': 'Kevin',
            'lname': 'Smith',
            'street1': '888 magnolia rd',
            'eaddress': 'bobksmith@gmail.com'
        }

        assert record_dict == expected

        n = Name(['Bob', 'Kevin', 'Smith'])

        record_dict = {
            'original_name': None,
            'valid': None,
            'fname': 'Bob',
            'mname': 'Kevin',
            'lname': 'Smith',
            'street1': '888 magnolia rd',
            'eaddress': 'bobksmith@gmail.com'
        }

        n.populate(record_dict)

        expected = {
            'original_name': ['Bob', 'Kevin', 'Smith'],
            'valid': True,
            'fname': 'Bob',
            'mname': 'Kevin',
            'lname': 'Smith',
            'street1': '888 magnolia rd',
            'eaddress': 'bobksmith@gmail.com'
        }

        assert record_dict == expected


    def test_search_and_split(self):
        n = Name.search_and_split('mary jo', ' ')

        assert n == 'Mary Jo'

        n = Name.search_and_split('smith-smythe-smith', '-')

        assert n == 'Smith-Smythe-Smith'


    def test_standardize_hyphen(self):
        n = Name.standardize_hyphen('Herbert Edmund - Frankfurter')

        assert n == 'Herbert Edmund-Frankfurter'

        n = Name.standardize_hyphen('Herbert Edmund- Frankfurter')

        assert n == 'Herbert Edmund-Frankfurter'

        n = Name.standardize_hyphen('Herbert Edmund -Frankfurter')

        assert n == 'Herbert Edmund-Frankfurter'
