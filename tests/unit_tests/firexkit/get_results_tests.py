import unittest
from firexkit.result import DYNAMIC_RETURN, get_results, RETURN_KEYS_KEY
from firexkit.testing import MockFxAsyncResult


def _get_task_meta_asserts():
    raise AssertionError('Raising a fake assertion for the unit-test')

def _get_task_meta_attr_missing():
    raise AttributeError('Raising a fake attribute error for the unit-test')

class GetResultsTests(unittest.TestCase):
    def test_plain_case(self):
        result = {'a': 1, 'b': 2}
        r = MockFxAsyncResult(result=result)
        self.assertDictEqual(get_results(r, return_keys_only=False), result)
        self.assertDictEqual(get_results(r), {})

    def test_empty_case(self):
        result = {}
        r = MockFxAsyncResult(result=result)
        self.assertEqual(get_results(r), result)

    def test_none_case(self):
        result = None
        r = MockFxAsyncResult(result)
        self.assertEqual(get_results(r, return_keys_only=False), {})
        self.assertDictEqual(get_results(r), {})

    def test_unsucessful_result(self):
        result = {'a': 1, 'b': 2}
        r = MockFxAsyncResult(result=result, successful=False)
        self.assertDictEqual(get_results(r, return_keys_only=False), {})
        self.assertDictEqual(get_results(r), {})

    def test_assertion(self):
        result = {'a': 1, 'b': 2}
        r = MockFxAsyncResult(result=result)
        # monkey patch
        r._get_task_meta = _get_task_meta_asserts
        self.assertDictEqual(get_results(r, return_keys_only=False), result)
        self.assertDictEqual(get_results(r), {})

    def test_result_with_children(self):
        result = {'a': 1, 'b': 2}
        c1_result = {'c': 3, 'd': 4, 'a': 5}
        c2_result = {'e': 6, 'c': 7}
        c1 = MockFxAsyncResult(result=c1_result)
        c2 = MockFxAsyncResult(result=c2_result)

        r = MockFxAsyncResult(result=result, children=[c1, c2])

        with self.subTest('Merging results from children'):
            expected = result.copy()
            expected.update(c1_result)
            expected.update(c2_result)
            self.assertDictEqual(get_results(r, return_keys_only=False, merge_children_results=True),expected)
            self.assertDictEqual(get_results(r, merge_children_results=True), {})

        with self.subTest('No extraction from children'):
            self.assertDictEqual(get_results(r, return_keys_only=False,), result)
            self.assertDictEqual(get_results(r, merge_children_results=True), {})


        with self.subTest('Order of children does matter'):
            r = MockFxAsyncResult(result=result, children=[c2, c1])
            expected = result.copy()
            expected.update(c2_result)
            expected.update(c1_result)
            self.assertDictEqual(get_results(r, return_keys_only=False, merge_children_results=True), expected)
            self.assertDictEqual(get_results(r, merge_children_results=True), {})

    def test_result_with_children_with_unsuccessful(self):
        result = {'a': 1, 'b': 2}
        c1_result = {'c': 3, 'd': 4, 'a': 5}
        c2_result = {'e': 6, 'c': 7}
        c1 = MockFxAsyncResult(result=c1_result)
        c2 = MockFxAsyncResult(result=c2_result)

        with self.subTest('Parent not successful'):
            r = MockFxAsyncResult(result=result, children=[c1, c2], successful=False)
            expected = c1_result.copy()
            expected.update(c2_result)
            self.assertDictEqual(get_results(r, return_keys_only=False, merge_children_results=True), expected)
        self.assertDictEqual(get_results(r, merge_children_results=True), {})

        with self.subTest('Child not successful'):
            c3 = MockFxAsyncResult(result=c2_result, successful=False)
            r = MockFxAsyncResult(result=result, children=[c1, c3])
            expected = result.copy()
            expected.update(c1_result)
            self.assertDictEqual(get_results(r, return_keys_only=False, merge_children_results=True), expected)
            self.assertDictEqual(get_results(r, merge_children_results=True), {})

        with self.subTest('Child attribute missing'):
            c4 = MockFxAsyncResult(result=c2_result)
            c4._get_task_meta = _get_task_meta_attr_missing
            r = MockFxAsyncResult(result=result, children=[c1, c4])
            expected = result.copy()
            expected.update(c1_result)
            expected.update(c2_result)
            self.assertDictEqual(get_results(r, return_keys_only=False, merge_children_results=True), expected)
            self.assertDictEqual(get_results(r, merge_children_results=True), {})

    def test_return_keys(self):
        with self.subTest('return_keys'):
            return_keys = {RETURN_KEYS_KEY: ('a', 'b', 'c', 'd')}
            values = {'a': 1, 'b': 2, 'c': 3, 'd': 4}
            result = values.copy()
            result.update(**return_keys)
            r = MockFxAsyncResult(result=result)

            with self.subTest('string'):
                self.assertTupleEqual(get_results(r, return_keys='a'), (1, ))
            with self.subTest('tuple of one'):
                self.assertTupleEqual(get_results(r, return_keys=('a',)), (1, ))
            with self.subTest('tuple of two'):
                self.assertTupleEqual(get_results(r, return_keys=('b', 'd')), (2, 4))
            with self.subTest('None'):
                self.assertDictEqual(get_results(r, return_keys=None), values)
            with self.subTest('Non-existent key'):
                self.assertTupleEqual(get_results(r, return_keys=('b', 'z')), (2, None))

    def test_extract_task_returns_only(self):
        with self.subTest('plain case'):
            return_keys = ('a', 'b')
            result = {'a': 1, 'b': 2, 'c': 3, RETURN_KEYS_KEY: return_keys}
            r = MockFxAsyncResult(result=result)
            self.assertDictEqual(get_results(r, merge_children_results=True),
                                 {k:result[k] for k in return_keys})

        with self.subTest('return keys is None'):
            result = {'a': 1, 'b': 2, 'c': 3, RETURN_KEYS_KEY: None}
            r = MockFxAsyncResult(result=result)
            self.assertDictEqual(get_results(r, merge_children_results=True), {})

        with self.subTest('some of the return keys are non-existent'):
            result = {'a': 1, 'b': 2, 'c': 3, RETURN_KEYS_KEY: ('a', 'd')}
            r = MockFxAsyncResult(result=result)
            self.assertDictEqual(get_results(r, merge_children_results=True), {'a': 1})

        with self.subTest('child has a return key'):
            result = {'a': 1, 'b': 2}
            c1_result = {'c': 3, 'd': 4, 'a': 5, RETURN_KEYS_KEY: ('d',)}
            c2_result = {'e': 6, 'c': 7}
            c1 = MockFxAsyncResult(result=c1_result)
            c2 = MockFxAsyncResult(result=c2_result)
            r = MockFxAsyncResult(result=result, children=[c1, c2])
            self.assertDictEqual(get_results(r, merge_children_results=True), {'d': 4})

        with self.subTest('parent and child have return keys'):
            return_keys = ('a', )
            result = {'a': 1, 'b': 2, RETURN_KEYS_KEY: return_keys}
            c1_result = {'c': 3, 'd': 4, 'a': 5, RETURN_KEYS_KEY: ('d',)}
            c2_result = {'e': 6, 'c': 7}
            c1 = MockFxAsyncResult(result=c1_result)
            c2 = MockFxAsyncResult(result=c2_result)
            r = MockFxAsyncResult(result=result, children=[c1, c2])
            self.assertDictEqual(get_results(r, merge_children_results=True), {'a': 1, 'd': 4})

        with self.subTest('parent and child have empty return keys'):
            result = {'a': 1, 'b': 2, RETURN_KEYS_KEY: tuple()}
            c1_result = {'c': 3, 'd': 4, 'a': 5, RETURN_KEYS_KEY: ('d',)}
            c2_result = {'e': 6, 'c': 7, RETURN_KEYS_KEY: None}
            c1 = MockFxAsyncResult(result=c1_result)
            c2 = MockFxAsyncResult(result=c2_result)
            r = MockFxAsyncResult(result=result, children=[c1, c2])
            self.assertDictEqual(get_results(r, merge_children_results=True), {'d': 4})

    def test_extract_with_dynamic_return(self):
        with self.subTest('return keys is the string DYNAMIC_RETURN'):
            result = {'a': 1, 'b': 2, 'c': 3, RETURN_KEYS_KEY: ('a', 'b')}
            r = MockFxAsyncResult(result=result)
            self.assertDictEqual(get_results(r, return_keys=DYNAMIC_RETURN), {'a': 1, 'b': 2})

        with self.subTest('return keys is the tuple (DYNAMIC_RETURN, )'):
            result = {'a': 1, 'b': 2, 'c': 3, RETURN_KEYS_KEY: ('a', 'b')}
            r = MockFxAsyncResult(result=result)
            self.assertDictEqual(get_results(r, return_keys=(DYNAMIC_RETURN, )), {'a': 1, 'b': 2})

        with self.subTest('return keys is the tuple (DYNAMIC_RETURN, "a")'):
            result = {'a': 1, 'b': 2, 'c': 3, RETURN_KEYS_KEY: ('a', 'b')}
            r = MockFxAsyncResult(result=result)
            v1, v2 = get_results(r, return_keys=(DYNAMIC_RETURN, "a"))
            self.assertDictEqual(v1, {'a': 1, 'b': 2})
            self.assertEqual(v2, 1)

        with self.subTest('return keys is the tuple ("a", DYNAMIC_RETURN)'):
            result = {'a': 1, 'b': 2, 'c': 3, RETURN_KEYS_KEY: ('a', 'b')}
            r = MockFxAsyncResult(result=result)
            v1, v2 = get_results(r, return_keys=("a", DYNAMIC_RETURN))
            self.assertEqual(v1, 1)
            self.assertDictEqual(v2, {'a': 1, 'b': 2})

    def test_extract_from_parents(self):
        bb_child = MockFxAsyncResult(result={'bb': 9, 'b': 3, 'bbb': 0, RETURN_KEYS_KEY: ('bb', 'b')})

        a = MockFxAsyncResult(result={'a': 1, 'aa': 11, 'aaa': 111, RETURN_KEYS_KEY: ('a', 'aa')})
        b = MockFxAsyncResult(result={'b': 2, 'aa': 22, RETURN_KEYS_KEY: ('b', 'aa')}, parent=a, children=[bb_child])
        c = MockFxAsyncResult(result={'c': 3, RETURN_KEYS_KEY: ('c', )}, parent=b)

        with self.subTest('extract_from_parents=True (default)'):
            self.assertDictEqual(get_results(c), {'a': 1, 'b': 2, 'c': 3, 'aa': 22})

        with self.subTest('merge_children_results=True'):
            self.assertDictEqual(get_results(c, merge_children_results=True),
                                 {'a': 1, 'b': 2, 'c': 3, 'aa': 22, 'bb': 9})

        with self.subTest('merge_children_results=True, return_keys_only=False'):
            self.assertDictEqual(get_results(c, merge_children_results=True, return_keys_only=False),
                                 {'aaa': 111, 'a': 1, 'b': 2, 'c': 3, 'aa': 22, 'bb': 9, 'bbb': 0})

        with self.subTest('return_keys_only=False'):
            self.assertDictEqual(get_results(c, return_keys_only=False), {'a': 1, 'b': 2, 'c': 3, 'aa': 22, 'aaa': 111})

        with self.subTest('parent_id'):
            self.assertDictEqual(get_results(c, parent_id=b.id), {'c': 3})

        with self.subTest('extract_from_parents=False'):
            self.assertDictEqual(get_results(c, extract_from_parents=False), {'c': 3})

