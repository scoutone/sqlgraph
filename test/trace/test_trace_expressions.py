from test.trace.helpers import (
    TraceTestCase, trace, col, const, transform, composite, UNKNOWN, resolve, leaves
)

FN = col('people', 'first_name')
LN = col('people', 'last_name')
AGE = col('people', 'age')
ID = col('people', 'id')
DATA = col('people', 'data')


class ExpressionTraceTests(TraceTestCase):
    """Lineage through expressions, operators and functions"""

    def select(self, expression, extra_from='db.people'):
        return trace(f"SELECT {expression} AS x FROM {extra_from}")

    # ---- constants -------------------------------------------------------------------------------

    def test_literals(self):
        ts = trace("SELECT 1 AS a, 'x' AS b, TRUE AS c, NULL::TEXT AS d FROM db.people")
        self.assert_sources(ts, {
            'a': const('1'),
            'b': const("'x'"),
            'c': const('TRUE'),
            'd': const('NULL'),
        })

    def test_select_without_from(self):
        ts = trace("SELECT 1 AS a, 'x' AS b")
        self.assert_sources(ts, {'a': const('1'), 'b': const("'x'")})

    def test_current_date_and_timestamp_are_constants(self):
        ts = trace("SELECT CURRENT_TIMESTAMP AS t, CURRENT_DATE AS d FROM db.people")
        self.assertEqual({'CONSTANT'}, leaves(ts.sources['t']))
        self.assertEqual({'CONSTANT'}, leaves(ts.sources['d']))

    def test_values_table(self):
        ts = trace("SELECT v.a FROM (VALUES (1, 2)) AS v(a, b)")
        self.assert_source(ts, 'a', const('VALUES'))

    def test_generate_series(self):
        ts = trace("SELECT g FROM GENERATE_SERIES(1, 3) AS g")
        self.assert_source(ts, 'g', const('SERIES[1..3]'))

    # ---- casts and pass-through wrappers ---------------------------------------------------------

    def test_cast_is_transparent(self):
        self.assert_source(self.select("age::TEXT"), 'x', AGE)

    def test_cast_function_is_transparent(self):
        self.assert_source(self.select("CAST(age AS TEXT)"), 'x', AGE)

    def test_parentheses_are_transparent(self):
        self.assert_source(self.select("(age)"), 'x', AGE)

    def test_chained_casts(self):
        self.assert_source(self.select("(age::TEXT)::VARCHAR"), 'x', AGE)

    def test_aggregates_pass_through_their_argument(self):
        ts = trace("SELECT MAX(age) AS mx, MIN(age) AS mn, SUM(age) AS sm FROM db.people GROUP BY dept_id")
        self.assert_sources(ts, {'mx': AGE, 'mn': AGE, 'sm': AGE})

    def test_count_is_constant(self):
        ts = trace("SELECT COUNT(*) AS c FROM db.people GROUP BY dept_id")
        self.assert_source(ts, 'c', const('COUNT'))

    def test_extract_traces_its_source(self):
        self.assert_source(self.select("EXTRACT(YEAR FROM age)"), 'x', AGE)

    # ---- string functions ------------------------------------------------------------------------

    def test_upper_lower_trim(self):
        ts = trace("SELECT UPPER(first_name) AS u, LOWER(last_name) AS l, TRIM(first_name) AS t FROM db.people")
        self.assert_sources(ts, {
            'u': transform('UPPER', [FN]),
            'l': transform('LOWER', [LN]),
            't': transform('TRIM', [FN]),
        })

    def test_substring(self):
        self.assert_source(
            self.select("SUBSTRING(first_name, 1, 2)"), 'x',
            transform('SUBSTRING(1,2)', [FN])
        )

    def test_split_part(self):
        self.assert_source(
            self.select("SPLIT_PART(first_name, ' ', 1)"), 'x',
            transform("SPLIT_PART(' ',1)", [FN])
        )

    def test_regexp_replace(self):
        self.assert_source(
            self.select("REGEXP_REPLACE(first_name, 'a', 'b')"), 'x',
            transform("REGEXP_REPLACE('a', 'b')", [FN])
        )

    def test_concat_function(self):
        self.assert_source(
            self.select("CONCAT(first_name, last_name)"), 'x',
            transform('CONCAT', [FN, LN])
        )

    def test_concat_operator(self):
        self.assert_source(
            self.select("first_name || ' ' || last_name"), 'x',
            transform('CONCAT', [FN, const("' '"), LN])
        )

    def test_concat_operator_is_flattened(self):
        # a || b || c || d must be a single CONCAT of four inputs, not nested CONCATs
        self.assert_source(
            self.select("first_name || '-' || last_name || '!'"), 'x',
            transform('CONCAT', [FN, const("'-'"), LN, const("'!'")])
        )

    # ---- operators -------------------------------------------------------------------------------

    def test_arithmetic(self):
        ts = trace("SELECT age + 1 AS a, age * id AS b, age - 1 AS c, age / 2 AS d FROM db.people")
        self.assert_sources(ts, {
            'a': transform('ADD', {'left': AGE, 'right': const('1')}),
            'b': transform('MUL', {'left': AGE, 'right': ID}),
            'c': transform('SUB', {'left': AGE, 'right': const('1')}),
            'd': transform('DIV', {'left': AGE, 'right': const('2')}),
        })

    def test_comparisons(self):
        ts = trace("SELECT age > 1 AS gt, age < 1 AS lt, age >= 1 AS gte, age <= 1 AS lte, first_name = 'a' AS eq, first_name <> 'a' AS neq FROM db.people")
        self.assert_sources(ts, {
            'gt': transform('GT', {'left': AGE, 'right': const('1')}),
            'lt': transform('LT', {'left': AGE, 'right': const('1')}),
            'gte': transform('GTE', {'left': AGE, 'right': const('1')}),
            'lte': transform('LTE', {'left': AGE, 'right': const('1')}),
            'eq': transform('EQ', {'left': FN, 'right': const("'a'")}),
            'neq': transform('NEQ', {'left': FN, 'right': const("'a'")}),
        })

    def test_boolean_operators(self):
        ts = trace("SELECT (age > 1 AND age < 5) AS a, (age = 1 OR age = 2) AS o FROM db.people")
        self.assertEqual({'AND'}, {resolve(ts.sources['a'])[1]})
        self.assertEqual({'OR'}, {resolve(ts.sources['o'])[1]})
        self.assertEqual({'cat.db.people.age', 'CONSTANT'}, leaves(ts.sources['a']))

    def test_not(self):
        self.assert_source(
            self.select("NOT (age > 1)"), 'x',
            transform('NOT', [transform('GT', {'left': AGE, 'right': const('1')})])
        )

    def test_is_null(self):
        self.assert_source(self.select("first_name IS NULL"), 'x', transform('IS NULL', [FN]))

    def test_is_not_null(self):
        self.assert_source(
            self.select("first_name IS NOT NULL"), 'x',
            transform('NOT', [transform('IS NULL', [FN])])
        )

    def test_in_list(self):
        self.assert_source(
            self.select("age IN (1, 2)"), 'x',
            transform('IN', {'left': AGE, 'right': transform('SET', [const('1'), const('2')])})
        )

    def test_between(self):
        self.assert_source(
            self.select("age BETWEEN 1 AND 3"), 'x',
            transform('BETWEEN', {'value': AGE, 'low': const('1'), 'high': const('3')})
        )

    def test_like(self):
        self.assert_source(
            self.select("first_name LIKE 'a%'"), 'x',
            transform('LIKE', {'left': FN, 'right': const("'a%'")})
        )

    def test_not_like(self):
        # newer sqlglot represents this as Like(negate=True) rather than Not(Like(..)); the lineage is the same
        self.assert_source(
            self.select("first_name NOT LIKE 'a%'"), 'x',
            transform('NOT', [transform('LIKE', {'left': FN, 'right': const("'a%'")})])
        )

    def test_not_ilike(self):
        self.assert_source(
            self.select("first_name NOT ILIKE 'a%'"), 'x',
            transform('NOT', [transform('ILIKE', {'left': FN, 'right': const("'a%'")})])
        )

    # ---- functions newer sqlglot types but older sqlglot treats as generic calls -------------------

    def test_replace_traces_all_arguments(self):
        self.assert_source(
            self.select("REPLACE(first_name, 'a', 'b')"), 'x',
            transform('REPLACE', [FN, const("'a'"), const("'b'")])
        )

    def test_ascii_and_trunc_trace_their_arguments(self):
        self.assert_source(self.select("ASCII(first_name)"), 'x', transform('ASCII', [FN]))
        self.assert_source(self.select("TRUNC(age)"), 'x', transform('TRUNC', [AGE]))

    def test_json_object_agg_with_a_function_key(self):
        # the key is itself a function call: it must not be mistaken for a key/value pair node
        self.assert_source(
            self.select("JSON_OBJECT_AGG(TRIM(first_name), last_name)"), 'x',
            transform('JSON_OBJECT_AGG', [transform('TRIM', [FN]), LN])
        )

    def test_variadic_argument(self):
        self.assert_source(
            self.select("my_func(first_name, VARIADIC (ARRAY[age]))"), 'x',
            transform('MY_FUNC', [FN, transform('VARIADIC', [transform('ARRAY', [AGE])])])
        )

    def test_regexp_replace_with_an_escape_string_pattern(self):
        # E'..' patterns print as nothing through sqlglot's default generator in newer releases
        self.assert_source(
            self.select("REGEXP_REPLACE(first_name, E'[xy]', '_')"), 'x',
            transform("REGEXP_REPLACE([xy], '_')", [FN])
        )

    # ---- composites ------------------------------------------------------------------------------

    def test_coalesce(self):
        self.assert_source(self.select("COALESCE(first_name, last_name)"), 'x', composite('COALESCE', [FN, LN]))

    def test_coalesce_with_literal_default(self):
        self.assert_source(
            self.select("COALESCE(first_name, 'n/a')"), 'x',
            composite('COALESCE', [FN, const("'n/a'")])
        )

    def test_greatest_least(self):
        ts = trace("SELECT GREATEST(age, id) AS g, LEAST(age, id) AS l FROM db.people")
        self.assert_sources(ts, {
            'g': composite('GREATEST', [AGE, ID]),
            'l': composite('LEAST', [AGE, ID]),
        })

    def test_array_constructor(self):
        self.assert_source(self.select("ARRAY[first_name, last_name]"), 'x', transform('ARRAY', [FN, LN]))

    def test_lag(self):
        self.assert_source(self.select("LAG(age) OVER (ORDER BY id)"), 'x', composite('LAG', [AGE]))

    # ---- CASE ------------------------------------------------------------------------------------

    def test_case_with_else(self):
        self.assert_source(
            self.select("CASE WHEN age > 10 THEN first_name ELSE last_name END"), 'x',
            ('cond', transform('GT', {'left': AGE, 'right': const('10')}), FN, LN)
        )

    def test_case_without_else_defaults_to_null(self):
        self.assert_source(
            self.select("CASE WHEN age > 10 THEN first_name END"), 'x',
            ('cond', transform('GT', {'left': AGE, 'right': const('10')}), FN, const('NULL'))
        )

    def test_case_with_multiple_branches(self):
        ts = self.select("CASE WHEN age < 10 THEN 'child' WHEN age < 20 THEN 'teen' ELSE 'adult' END")
        # branches nest: first condition outermost, ELSE innermost
        kind, cond1, true1, false1 = resolve(ts.sources['x'])
        self.assertEqual('cond', kind)
        self.assertEqual(const("'child'"), true1)
        kind2, cond2, true2, false2 = false1
        self.assertEqual('cond', kind2)
        self.assertEqual(const("'teen'"), true2)
        self.assertEqual(const("'adult'"), false2)

    def test_case_inputs(self):
        ts = self.select("CASE WHEN age > 10 THEN first_name ELSE last_name END")
        self.assertEqual({'cat.db.people.age', 'cat.db.people.first_name', 'cat.db.people.last_name', 'CONSTANT'}, leaves(ts.sources['x']))

    # ---- JSON ------------------------------------------------------------------------------------

    def test_json_text_extract(self):
        self.assert_source(self.select("data->>'name'"), 'x', ('path', 'name', DATA))

    def test_json_chained_extract(self):
        self.assert_source(self.select("data->'a'->>'b'"), 'x', ('path', 'a.b', DATA))

    def test_json_deeply_chained_extract(self):
        self.assert_source(self.select("data->'a'->'b'->'c'->>'d'"), 'x', ('path', 'a.b.c.d', DATA))

    def test_json_extract_through_cast(self):
        self.assert_source(self.select("(data->>'age')::INT"), 'x', ('path', 'age', DATA))

    def test_json_extract_with_a_cast_on_the_key(self):
        # `::` binds tighter than `->>`: newer sqlglot casts the key ('b'::text), older casts the whole result
        self.assert_source(self.select("data->'a'->>'b'::TEXT"), 'x', ('path', 'a.b', DATA))
        self.assert_source(self.select("data->'a b'->>'c'::TEXT"), 'x', ('path', '"a b".c', DATA))

    def test_jsonb_extract(self):
        self.assert_source(self.select("data::jsonb->>'x'"), 'x', ('path', 'x', DATA))

    def test_json_extract_path_text(self):
        self.assert_source(self.select("JSON_EXTRACT_PATH_TEXT(data, 'a', 'b')"), 'x', ('path', 'a.b', DATA))

    def test_json_extract_of_cte_column(self):
        ts = trace("WITH c AS (SELECT data AS d FROM db.people) SELECT d->>'k' AS x FROM c")
        self.assert_source(ts, 'x', ('path', 'k', DATA))

    def test_json_build_object(self):
        self.assert_source(
            self.select("JSON_BUILD_OBJECT('a', first_name, 'b', age)"), 'x',
            transform('JSON_BUILD_OBJECT', [const("'a'"), FN, const("'b'"), AGE])
        )

    def test_json_build_object_then_extract_in_cte(self):
        ts = trace("""
            WITH names AS (
              SELECT JSON_BUILD_OBJECT('first_name', first_name, 'last_name', last_name) AS name FROM db.people
            )
            SELECT name->'first_name' AS fn, name->'last_name' AS ln FROM names
        """)
        self.assertEqual({'cat.db.people.first_name', 'CONSTANT', 'cat.db.people.last_name'}, leaves(ts.sources['fn']) | leaves(ts.sources['ln']))
        self.assertNotIn('UNKNOWN', leaves(ts.sources['fn']))

    # ---- window functions ------------------------------------------------------------------------

    def test_row_number_is_constant(self):
        self.assert_source(
            self.select("ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY age)"), 'x', const('ROW_NUMBER')
        )

    def test_dense_rank_is_constant(self):
        self.assert_source(self.select("DENSE_RANK() OVER (ORDER BY age)"), 'x', const('DENSE_RANK'))

    def test_aggregate_window_traces_its_argument(self):
        self.assert_source(self.select("SUM(age) OVER (PARTITION BY dept_id)"), 'x', AGE)

    # ---- generic function calls ------------------------------------------------------------------

    def test_generic_function_traces_all_arguments(self):
        self.assert_source(self.select("my_func(first_name, age)"), 'x', transform('MY_FUNC', [FN, AGE]))

    def test_generic_function_single_argument(self):
        self.assert_source(self.select("my_func(first_name)"), 'x', transform('MY_FUNC', [FN]))

    def test_schema_qualified_generic_function(self):
        self.assert_source(self.select("public.my_func(first_name)"), 'x', transform('MY_FUNC', [FN]))

    def test_nested_generic_functions(self):
        self.assert_source(
            self.select("outer_func(inner_func(age), 'lit')"), 'x',
            transform('OUTER_FUNC', [transform('INNER_FUNC', [AGE]), const("'lit'")])
        )

    def test_generic_function_without_arguments_is_constant(self):
        self.assert_source(self.select("some_func()"), 'x', const('SOME_FUNC'))

    def test_generic_function_with_only_constant_arguments(self):
        self.assert_source(self.select("some_func(1, 'a')"), 'x', transform('SOME_FUNC', [const('1'), const("'a'")]))

    def test_generic_function_name_is_uppercased(self):
        self.assertEqual('MixedCase'.upper(), resolve(self.select("MixedCase(age)").sources['x'])[1])

    def test_generic_function_inside_other_expressions(self):
        self.assert_source(
            self.select("my_func(age) + 1"), 'x',
            transform('ADD', {'left': transform('MY_FUNC', [AGE]), 'right': const('1')})
        )

    # ---- unnest ----------------------------------------------------------------------------------

    def test_unnest_in_from(self):
        ts = trace("SELECT u FROM db.people, UNNEST(ARRAY[first_name]) AS u")
        self.assert_source(ts, 'u', transform('ARRAY', [FN]))

    # ---- unsupported -----------------------------------------------------------------------------

    def test_unresolvable_column_inside_expression_is_unknown_not_an_error(self):
        ts = self.select("UPPER(zz)")
        self.assert_source(ts, 'x', transform('UPPER', [UNKNOWN]))

    # ---- custom tracers --------------------------------------------------------------------------

    def test_custom_tracer_overrides_default_handling(self):
        from sqlglot import exp
        from sqlgraph import model as mdl

        def custom_upper(tracer, e):
            return mdl.ConstantSource('CUSTOM')

        ts = trace("SELECT UPPER(first_name) AS x FROM db.people", tracers={exp.Upper: custom_upper})
        self.assert_source(ts, 'x', const('CUSTOM'))

    def test_custom_tracer_can_delegate_back_to_the_tracer(self):
        from sqlglot import exp
        from sqlgraph import model as mdl

        def wrap_upper(tracer, e):
            return mdl.TransformSource('WRAPPED', tracer.trace(e.args['this']))

        ts = trace("SELECT UPPER(first_name) AS x FROM db.people", tracers={exp.Upper: wrap_upper})
        self.assert_source(ts, 'x', transform('WRAPPED', [FN]))
