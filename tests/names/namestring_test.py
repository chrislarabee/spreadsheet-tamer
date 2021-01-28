from datagenius.names import Namestring


def test_allocate():
    n = Namestring("William Cyrus Jehosephat")

    n._allocate()

    assert n.fname == "William"
    assert n.mname == "Cyrus"
    assert n.lname == "Jehosephat"

    n = Namestring("Bob Smith")

    n._allocate()

    assert n.fname == "Bob"
    assert n.mname is None
    assert n.lname == "Smith"

    n = Namestring("Odysseus Ithaka")
    n.name_list2 = ["Penelope", "Ithaka"]

    n._allocate()

    assert n.fname == "Odysseus"
    assert n.mname is None
    assert n.lname == "Ithaka"
    assert n.fname2 == "Penelope"
    assert n.mname2 is None
    assert n.lname2 == "Ithaka"

    n = Namestring("Bob and Helen Parr")

    n._allocate()

    assert n.fname == "Bob"
    assert n.mname is None
    assert n.lname == "Parr"
    assert n.fname2 == "Helen"
    assert n.mname2 is None
    assert n.lname2 == "Parr"


def test_assign_affixes():
    n = Namestring("Mr. Bob Parr")

    assert n.prefix == "Mr."
    assert n.name_list == ["Bob", "Parr"]

    n = Namestring("Mr. and Mrs. Bob Parr Jr.")

    assert n.prefix == "Mr."
    assert n.prefix2 == "Mrs."
    assert n.suffix == "Jr."
    assert n.name_list == ["Bob", "Parr"]

    n = Namestring("Mr. and Mrs. and Dr. Bob Parr")

    assert n.prefix == "Mr."
    assert n.prefix2 == "Mrs."
    assert n.name_list == ["Bob", "Parr"]


def test_assign_ampersand_split():
    n = Namestring("Bob Parr and Helen Parr")

    assert n.name_list1 == ["Bob", "Parr"]
    assert n.name_list2 == ["Helen", "Parr"]

    n = Namestring("Bob and Helen Parr")

    assert n.name_list1 == ["Bob"]
    assert n.name_list2 == ["Helen", "Parr"]

    n = Namestring("Mr. and Ms. Bob Parr")

    assert n.name_list == ["Bob", "Parr"]
    assert n.name_list2 is None

    n = Namestring("Mr. and Ms. Bob and Helen Parr")

    assert n.name_list1 == ["Bob"]
    assert n.name_list2 == ["Helen", "Parr"]

    n = Namestring("Bob & Helen Parr")

    assert n.name_list1 == ["Bob"]
    assert n.name_list2 == ["Helen", "Parr"]


def test_assign_middle_initials():
    n = Namestring("Kay O. G. Williams")

    assert n.mname == "O.G."
    assert n.name_list == ["Kay", "Williams"]

    n = Namestring("Barbara O Hammond and Nicholas L. Krupke")

    assert n.mname == "O."
    assert n.mname2 == "L."

    assert n.name_list == ["Barbara", "Hammond", "Nicholas", "Krupke"]

    n = Namestring("E. B. White")

    assert n.mname == "B."
    assert n.name_list == ["E.", "White"]

    # TODO: Add a test for names that start with initials.


def test_manage_multi_fname():
    n = Namestring("mary ann williamson")

    assert n.name_list == ["Mary Ann", "Williamson"]

    n = Namestring("mary ann and jo ann williamson")

    assert n.name_list == ["Mary Ann", "Jo Ann", "Williamson"]


def test_manage_multi_lname():
    n = Namestring("bethany van houten")

    assert n.name_list == ["Bethany", "Van Houten"]

    n = Namestring("maria de las casas")

    assert n.name_list == ["Maria", "De Las Casas"]

    n = Namestring("Lars Boaz")

    assert n.name_list == ["Lars", "Boaz"]
