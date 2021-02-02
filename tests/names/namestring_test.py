from datagenius.names.namestring import Namestring


class TestAllocate:
    def test_that_it_can_handle_three_names(self):
        n = Namestring("William Cyrus Jehosephat")
        n._allocate()
        assert n.fname == "William"
        assert n.mname == "Cyrus"
        assert n.lname == "Jehosephat"

    def test_that_it_can_handle_two_names(self):
        n = Namestring("Bob Smith")
        n._allocate()
        assert n.fname == "Bob"
        assert n.mname is None
        assert n.lname == "Smith"

    def test_that_it_can_handle_two_name_lists(self):
        n = Namestring("Odysseus Ithaka")
        n.name_list2 = ["Penelope", "Ithaka"]
        n._allocate()
        assert n.fname == "Odysseus"
        assert n.mname is None
        assert n.lname == "Ithaka"
        assert n.fname2 == "Penelope"
        assert n.mname2 is None
        assert n.lname2 == "Ithaka"

    def test_that_it_can_handle_an_ampersand(self):
        n = Namestring("Bob and Helen Parr")
        n._allocate()
        assert n.fname == "Bob"
        assert n.mname is None
        assert n.lname == "Parr"
        assert n.fname2 == "Helen"
        assert n.mname2 is None
        assert n.lname2 == "Parr"

    def test_that_it_can_handle_a_similar_prefix_and_middle_initial(self):
        n = Namestring("Dr. Bob D. Parr")
        n._allocate()
        assert n.prefix == "Dr."
        assert n.fname == "Bob"
        assert n.mname == "D."
        assert n.lname == "Parr"


class TestAssignAffixes:
    def test_that_it_can_handle_single_prefix(self):
        n = Namestring("Mr. Bob Parr")
        assert n.prefix == "Mr."
        assert n.name_list == ["Bob", "Parr"]

    def test_that_it_can_handle_single_prefix_and_suffix(self):
        n = Namestring("Mr. Bob Parr Jr.")
        assert n.prefix == "Mr."
        assert n.suffix == "Jr."
        assert n.name_list == ["Bob", "Parr"]

    def test_that_it_can_handle_multiple_prefixes_and_a_suffix(self):
        n = Namestring("Mr. and Mrs. Bob Parr Jr.")
        assert n.prefix == "Mr."
        assert n.prefix2 == "Mrs."
        assert n.suffix == "Jr."
        assert n.name_list == ["Bob", "Parr"]

    def test_assign_affixes_with_two_people_and_thre_affixes(self):
        n = Namestring("Mr. and Mrs. and Dr. Bob Parr")

        assert n.prefix == "Mr."
        assert n.prefix2 == "Mrs."
        assert n.name_list == ["Bob", "Parr"]


class TestAssignMiddleInitials:
    def test_that_it_can_handle_single_middle_initial(self):
        n = Namestring("Frederick R Powell")
        assert n.mname == "R."
        assert n.name_list == ["Frederick", "Powell"]

    def test_assign_middle_initials_multiple_initials(self):
        n = Namestring("Kay O. G. Williams")
        assert n.mname == "O.G."
        assert n.name_list == ["Kay", "Williams"]

    def test_that_it_can_handle_names_that_start_with_initials(self):
        n = Namestring("E. B. White")
        assert n.mname == "B."
        assert n.name_list == ["E.", "White"]


class TestAssignAmpersandSplit:
    def test_that_it_can_handle_ampersand_between_two_complete_names(self):
        n = Namestring("Bob Parr and Helen Parr")
        assert n.name_list1 == ["Bob", "Parr"]
        assert n.name_list2 == ["Helen", "Parr"]

    def test_that_it_can_handle_ampersand_between_two_fnames_and_then_a_last_name(self):
        n = Namestring("Bob and Helen Parr")
        assert n.name_list1 == ["Bob"]
        assert n.name_list2 == ["Helen", "Parr"]
        n = Namestring("Bob & Helen Parr")
        assert n.name_list1 == ["Bob"]
        assert n.name_list2 == ["Helen", "Parr"]

    def test_that_it_can_handle_ampersand_and_prefixes(self):
        n = Namestring("Mr. and Ms. Bob Parr")
        assert n.name_list == ["Bob", "Parr"]
        assert n.name_list2 is None

    def test_that_it_can_handle_ampersand_w_prefixes_and_two_people(self):
        n = Namestring("Mr. and Ms. Bob and Helen Parr")
        assert n.name_list1 == ["Bob"]
        assert n.name_list2 == ["Helen", "Parr"]

    def test_assign_middle_initials_w_two_people(self):
        n = Namestring("Barbara O Hammond and Nicholas L. Krupke")
        assert n.mname == "O."
        assert n.mname2 == "L."
        assert n.name_list == ["Barbara", "Hammond", "Nicholas", "Krupke"]


class TestManageMultiFname:
    def test_that_it_can_handle_single_multi_fname(self):
        n = Namestring("mary ann williamson")
        assert n.name_list == ["Mary Ann", "Williamson"]

    def test_that_it_can_handle_multiple_multi_fnames(self):
        n = Namestring("mary ann and jo ann williamson")
        assert n.name_list == ["Mary Ann", "Jo Ann", "Williamson"]

    def test_that_it_can_handle_spelling_variations(self):
        n = Namestring("mary kaye ashley")
        assert n.name_list == ["Mary Kaye", "Ashley"]
        n = Namestring("mary kay ashley")
        assert n.name_list == ["Mary Kay", "Ashley"]
        n = Namestring("mary lu ashley")
        assert n.name_list == ["Mary Lu", "Ashley"]
        n = Namestring("mary lou ashley")
        assert n.name_list == ["Mary Lou", "Ashley"]


class TestManageMultiLname:
    def test_that_it_can_handle_two_part_multi_lnames(self):
        n = Namestring.manage_multi_lname(["bethany", "van", "houten"])
        assert n == ["bethany", "van houten"]

    def test_that_it_works_as_part_of_namestring_iniit(self):
        n = Namestring("sarah van den akker")
        assert n.name_list == ["Sarah", "Van Den Akker"]

    def test_that_it_can_handle_three_part_multi_lnames(self):
        n = Namestring.manage_multi_lname(["maria", "de", "las", "casas"])
        assert n == ["maria", "de las casas"]
        n = Namestring.manage_multi_lname(["bethany", "van", "der", "maar"])
        assert n == ["bethany", "van der maar"]

    def test_that_it_can_handle_multi_part_lnames_and_ampersands(self):
        n = Namestring("maria de las casas and miguel de las casas")
        assert n.name_list == [
            "Maria",
            "De",
            "Las",
            "Casas",
            "Miguel",
            "De",
            "Las",
            "Casas",
        ]
        assert n.name_list1 == ["Maria", "De Las Casas"]
        assert n.name_list2 == ["Miguel", "De Las Casas"]

    def test_that_it_does_not_group_multi_part_lnames_with_ampersands(self):
        n = Namestring("bethany vandermeer and george vandermeer")
        assert n.name_list == ["Bethany", "Vandermeer", "George", "Vandermeer"]


class TestExtractAltName:
    def test_that_it_can_handle_single_set_of_parens(self):
        s = "reginald (reggie) watts"
        n, a1, a2 = Namestring.extract_alt_name(s)
        assert n == "reginald watts"
        assert a1 == "reggie"
        assert not a2
        n = Namestring(s)
        assert n.name_list == ["Reginald", "Watts"]
        assert n.alt_name == "Reggie"

    def test_that_it_can_handle_two_sets_of_parens(self):
        s = "robert (rob) ryan and cassandra (cassie) maddox"
        n, a1, a2 = Namestring.extract_alt_name(s)
        assert n == "robert ryan and cassandra maddox"
        assert a1 == "rob"
        assert a2 == "cassie"
        n = Namestring(s)
        assert n.name_list == ["Robert", "Ryan", "Cassandra", "Maddox"]
        assert n.alt_name == "Rob"
        assert n.alt_name2 == "Cassie"

    def test_that_it_can_handle_many_sets_of_parens(self):
        n, a1, a2 = Namestring.extract_alt_name(
            "why (are) there (so) many (parens) in (this) name"
        )
        assert n == "why there many in name"
        assert a1 == "are"
        assert a2 == "so"

    def test_that_it_can_handle_weird_spacing(self):
        n, a1, a2 = Namestring.extract_alt_name("why  (    would   )you do this")
        assert n == "why you do this"
        assert a1 == "would"
        assert not a2
