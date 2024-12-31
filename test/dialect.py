from sqlglot.dialects.dialect import var_map_sql, rename_func
from sqlglot.dialects.postgres import Postgres
from sqlglot.helper import flatten

import typing as t

from sqlglot import exp, parser
from sqlglot._typing import E
from sqlglot.tokens import TokenType


            
def _build_build_object(args: t.List) -> t.Union[exp.StarMap, exp.Struct]:
    expression = parser.build_var_map(args)
    
    if isinstance(expression, exp.StarMap):
        return expression
    
    return exp.Struct(
        expressions=[
            exp.PropertyEQ(this=k, expression=v) for k, v in zip(expression.keys, expression.values)
        ]
    )
    
def _parse_json_to_record(parser, **kwargs):
    if not parser._curr or parser._curr.text != 'JSON_TO_RECORD':
        return None
    parser._retreat(parser._index-1)
    implicit_lateral = not parser._match(TokenType.LATERAL)
    parser._advance(3)
    
    column_name = parser._curr.text
    parser._advance(2)
    
    alias_idx = parser._index
    while parser._curr.token_type != TokenType.R_PAREN:
        parser._advance()
    r_paren_idx = parser._index
    parser._retreat(alias_idx)
    
    table_alias = parser._parse_table_alias()
    
    column_expressions = []
    for column in table_alias.columns:
        alias = parser.expression(
            exp.Alias,
            alias=parser.expression(
                exp.Identifier,
                this=column.name,
                quoted=False
            ),
            this=parser.expression(
                exp.JSONExtract,
                this = parser.expression(
                    exp.Column,
                    this=parser.expression(
                        exp.Identifier,
                        this=column_name,
                        quoted=False
                    )
                ),
                expression = parser.expression(
                    exp.JSONPath,
                    expressions=[
                        parser.expression(exp.JSONPathRoot),
                        parser.expression(exp.JSONPathKey, this=column.name)
                    ]
                ),
                only_json_types=True
            )
        )
        column_expressions.append(alias)
    
    e = parser.expression(
        exp.Subquery,
        this = parser.expression(
            exp.Select,
            expressions=column_expressions
        ),
        alias=table_alias
    )
    
    if implicit_lateral:
        e = parser.expression(
            exp.Lateral,
            this = e
        )
    
    parser._index = r_paren_idx + 1
    parser._advance(0)
    return e
            
class PostgresExtended(Postgres):
    class Parser(Postgres.Parser):
        def _parse_json_array(self, expr_type: t.Type[E], **kwargs) -> E:
            return self.expression(
                expr_type,
                null_handling=self._parse_on_handling("NULL", "NULL", "ABSENT"),
                return_type=self._match_text_seq("RETURNING") and self._parse_type(),
                strict=self._match_text_seq("STRICT"),
                **kwargs,
            )
            
        FUNCTIONS = {
            **Postgres.Parser.FUNCTIONS,
            "JSON_BUILD_OBJECT": _build_build_object,
        }
            
        FUNCTION_PARSERS = {
            **Postgres.Parser.FUNCTION_PARSERS,
            "JSON_ARRAYAGG": lambda self: self._parse_json_array(
                exp.JSONArrayAgg,
                this=self._parse_format_json(self._parse_bitwise()),
                order=self._parse_order(),
            ),
            #"JSON_TO_RECORD": lambda self: test(self),
        }
        
        def _parse_lateral(self, *args, **kwargs) -> t.Optional[exp.Expression]:
            json_to_record = _parse_json_to_record(self)
            if json_to_record:
                return json_to_record
            return super()._parse_lateral(*args, **kwargs)
        
    
        
    class Generator(Postgres.Generator):
        TRANSFORMS = {
            **Postgres.Generator.TRANSFORMS,
            exp.Map: lambda self, e: var_map_sql(self, e, "JSON_BUILD_OBJECT"),
            exp.StarMap: rename_func("JSON_BUILD_OBJECT"),
            exp.VarMap: lambda self, e: var_map_sql(self, e, "JSON_BUILD_OBJECT"),
        }

        def struct_sql(self, expression: exp.Struct) -> str:
            keys = []
            values = []

            for i, e in enumerate(expression.expressions):
                if isinstance(e, exp.PropertyEQ):
                    keys.append(
                        exp.Literal.string(e.name) if isinstance(e.this, exp.Identifier) else e.this
                    )
                    values.append(e.expression)
                else:
                    keys.append(exp.Literal.string(f"_{i}"))
                    values.append(e)

            return self.func("JSON_BUILD_OBJECT", *flatten(zip(keys, values)))