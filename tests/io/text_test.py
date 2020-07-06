from datagenius.io import text


def test_build_template(customers):
    t = text.get_output_template('tests/samples/csv/simple.csv')
    assert t == customers()['columns']

