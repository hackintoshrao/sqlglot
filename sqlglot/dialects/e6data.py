from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.tokens import TokenType
from sqlglot.parser import Parser
from sqlglot.generator import Generator
from sqlglot.dialects.mysql import MySQL
from dateutil.parser import parse
import datetime
from decimal import Decimal

class E6dataTimestampExpression(exp.Timestamp):
    def __init__(self, this):
        super().__init__(this)

    @classmethod
    def from_string(cls, value):
        if value is not None:
            try:
                return datetime.datetime.fromisoformat(value)
            except ValueError:
                return parse(value)
        return None

class E6dataDecimalExpression(exp.Decimal):
    def __init__(self, this):
        super().__init__(this)

    @classmethod
    def from_string(cls, value):
        if value is not None:
            return Decimal(value)
        return None


class E6data(MySQL):
    class Tokenizer(MySQL.Tokenizer):
        IDENTIFIERS = ['"']
        KEYWORDS = {
            **MySQL.Tokenizer.KEYWORDS,
        }

    class Parser(MySQL.Parser):
        # Custom parsing rules can be added here if needed
        pass

    class Generator(MySQL.Generator):
        TYPE_MAPPING = {
            **MySQL.Generator.TYPE_MAPPING,
            exp.DataType.Type.BOOLEAN: "BOOLEAN",
            exp.DataType.Type.TINYINT: "TINYINT",
            exp.DataType.Type.SMALLINT: "SMALLINT",
            exp.DataType.Type.INT: "INTEGER",
            exp.DataType.Type.BIGINT: "BIGINT",
            exp.DataType.Type.FLOAT: "FLOAT",
            exp.DataType.Type.DOUBLE: "DOUBLE",
            exp.DataType.Type.VARCHAR: "STRING",
            exp.DataType.Type.CHAR: "STRING",
            exp.DataType.Type.DATE: "DATE",
            exp.DataType.Type.TIMESTAMP: "TIMESTAMP",
            exp.DataType.Type.BINARY: "BINARY",
            exp.DataType.Type.ARRAY: "STRING",
            exp.DataType.Type.MAP: "STRING",
            exp.DataType.Type.STRUCT: "STRING",
            exp.DataType.Type.DECIMAL: "DECIMAL",
        }
        TRANSFORMS = {
            **MySQL.Generator.TRANSFORMS,
            exp.Concat: lambda self, e: self.func("concat", e.left, e.right),
            exp.Length: lambda self, e: self.func("length", e.this),
            E6dataTimestampExpression: lambda self, e: f"CAST({self.sql(e.this)} AS TIMESTAMP)",
            E6dataDecimalExpression: lambda self, e: f"CAST({self.sql(e.this)} AS DECIMAL)",
        }
        def generate(self, expression, copy: bool = True, **opts):
            if isinstance(expression, exp.Insert):
                raise NotImplementedError("INSERT operations are not supported in E6data")
            
            return super(**opts).generate(expression, copy=copy)
