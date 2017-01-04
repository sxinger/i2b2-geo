'''

KLUDGE: This is actually an integration test, as it relies on access
to the index_of_data_fields file.

    >>> def mn_census_data():
    ...     from pathlib2 import Path
    ...     # return Path('/d1/geo-census/mn-census-data')
    ...     return Path('.')

    >>> bk = pd.read_csv(
    ...     (mn_census_data() /
    ...      'acs_20135a/index_of_data_fields__acs_20135a.csv').open('rb'))
    >>> print table_def('acs_zcta_200', 'ge.00_file.dat.gz', bk[:5])
    create table acs_zcta_200 (
      FILEID VARCHAR2(6),
      STUSAB VARCHAR2(2),
      SUMLEVEL VARCHAR2(3),
      COMPONENT VARCHAR2(2),
      LOGRECNO INTEGER
    ) organization external (
      type oracle_loader
      default directory geo_census_stage
      access parameters (
        records delimited by newline
        preprocessor staging_tools:'zcat.sh'
        fields lrtrim
        (
          FILEID position (1-6) char(6),
          STUSAB position (7-2) char(2),
          SUMLEVEL position (9-3) char(3),
          COMPONENT position (12-2) char(2),
          LOGRECNO position (14-7) char(7) NULLIF LOGRECNO = '.'
        )
      )
      location ('ge.00_file.dat.gz')
    )

'''

from textwrap import dedent

import pandas as pd


def column_def(field):
    dty = (
        # TODO: implied_decimal_places, multiplier
        'INTEGER' if field.data_type == 'n'
        else 'VARCHAR2(%d)' % field.width)
    return '  {name} {dty}'.format(name=field.variable_code, dty=dty)


def field_spec(field):
    return '      %s position (%d-%d) char(%d)%s' % (
        field.variable_code,
        field.start_column, field.width,
        field.width,
        (" NULLIF %s = '.'" % field.variable_code
         if field.data_type == 'n' else ''))


def table_def(name, location, fields,
              data_dir='geo_census_stage',
              tools_dir='staging_tools'):
    if len(fields) > 1000:
        raise ValueError(
            'ORA-01792: maximum number of columns in a table or view is 1000')
    coldefs = ',\n'.join(column_def(f)
                         for (_, f) in fields.iterrows())
    field_list = ',\n'.join(field_spec(f)
                            for (_, f) in fields.iterrows())
    return dedent('''\
    create table {name} (
    {coldefs}
    ) organization external (
      type oracle_loader
      default directory {data_dir}
      access parameters (
        records delimited by newline
        preprocessor {tools_dir}:'zcat.sh'
        fields lrtrim
        (
    {field_list}
        )
      )
      location ('{location}')
    )
    ''').strip().format(
        name=name, coldefs=coldefs, field_list=field_list, location=location,
        data_dir=data_dir, tools_dir=tools_dir)
