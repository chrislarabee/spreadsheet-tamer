from tamer.names.nametoken import Nametoken


def test_allocate():
    n = Nametoken(["George", "G.", "Carlin"])

    n._allocate()

    assert n.fname == "George"
    assert n.mname == "G."
    assert n.lname == "Carlin"

    n = Nametoken(["Jonathan", "Strange"])

    n._allocate()

    assert n.fname == "Jonathan"
    assert n.mname is None
    assert n.lname == "Strange"

    n = Nametoken(["Heather and Rob", None, "Vandemar"])

    n._allocate()

    assert n.fname == "Heather"
    assert n.lname == "Vandemar"
    assert n.fname2 == "Rob"
    assert n.lname2 == "Vandemar"


def assign_ampersand_split():
    n = Nametoken(["Joe & Roger", None, "Jones"])

    assert n.name_list == ["Joe", None, "Jones"]
    assert n.fname2 == "Roger"

    n = Nametoken(["Penny & Alice", None, "Orville & Stabs"])

    assert n.name_list == ["Penny", "Orville"]
    assert n.fname2 == "Alice"
    assert n.lname2 == "Stabs"

    n._allocate()

    assert n.fname == "Penny"
    assert n.lname == "Orville"
    assert n.fname2 == "Alice"
    assert n.lname2 == "Stabs"


def test_assign_trailing_middle_initial():
    n = Nametoken(["George G.", "Carlin"])

    assert n.name_list == ["George", "Carlin"]
    assert n.mname == "G."

    n = Nametoken(["Mary Jo R.", "Williamson"])

    assert n.name_list == ["Mary Jo", "Williamson"]
    assert n.mname == "R."

    # Make sure mname stays in place after allocation:
    n._allocate()

    assert n.fname == "Mary Jo"
    assert n.mname == "R."
    assert n.lname == "Williamson"

    # Test single character fnames:
    n = Nametoken(["S", "Ramachandran"])

    n._allocate()

    assert n.fname == "S"
    assert n.mname is None
