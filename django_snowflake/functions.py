import django

if django.VERSION >= (3, 2, 0):
    from django.db.models.functions import (
        SHA224, SHA256, SHA384, SHA512, Ceil, Collate, ConcatPair, Random,
        StrIndex,
    )
else:
    from django.db.models.functions import (
        Ceil, ConcatPair, StrIndex,
    )

    


def ceil(self, compiler, connection, **extra_context):
    return self.as_sql(compiler, connection, function='CEIL', **extra_context)


def collate(self, compiler, connection, **extra_context):
    # https://docs.snowflake.com/en/sql-reference/functions/collate.html
    return self.as_sql(
        compiler,
        connection,
        # Snowflake requires single quotes instead of double quotes.
        collation=f"'{self.collation}'",
        # COLLATE(<string_expression>, '<collation_specification>')
        template='%(function)s(%(expressions)s, %(collation)s)',
        **extra_context,
    )


def concatpair(self, compiler, connection, **extra_context):
    # coalesce() prevents Concat from returning null instead of empty string.
    return self.coalesce().as_sql(compiler, connection, **extra_context)


def random(self, compiler, connection, **extra_context):
    template = 'UNIFORM(0, 0.99999999999999999, RANDOM())'
    return self.as_sql(compiler, connection, template=template, **extra_context)


def strindex(self, compiler, connection, **extra_context):
    # POSITION takes arguments in the opposite order of other databases.
    # https://docs.snowflake.com/en/sql-reference/functions/position.html
    return StrIndex(
        self.source_expressions[1],
        self.source_expressions[0],
    ).as_sql(compiler, connection, function='POSITION', **extra_context)


def register_functions():
    if django.VERSION >= (3, 2, 0):
        SHA224.as_snowflake = SHA224.as_mysql
        SHA256.as_snowflake = SHA256.as_mysql
        SHA384.as_snowflake = SHA384.as_mysql
        SHA512.as_snowflake = SHA512.as_mysql
        Collate.as_snowflake = collate
        Random.as_snowflake = random
    Ceil.as_snowflake = ceil
    ConcatPair.as_snowflake = concatpair
    StrIndex.as_snowflake = strindex
