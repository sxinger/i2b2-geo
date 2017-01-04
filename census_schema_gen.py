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

MAX_COLUMNS = 768  # nicely splits columns into 6 groups

GRANULARITIES = ['urb_area_400', 'county_050', 'cty_sub_060',
                 'zcta_860', 'place_070', 'blck_grp_150', 'tract_140']
# Note we're skipping acs_20135b, cph_2010_sf1a, and cph_2010_sf1b
DIR = 'acs_20135a'


def main(argv, cwd):
    [field_index_fn, out_fn] = argv[1:3]

    fields = pd.read_csv((cwd / field_index_fn).open('rb'))

    with (cwd / out_fn).open('wb') as out:
        for granularity in GRANULARITIES:
            for g_ix, grp in field_groups(fields, max_columns=MAX_COLUMNS):
                name = '%s_%s' % (DIR, granularity)
                dat_fn = name + '.dat.gz'
                out.write(table_def('MPC.%s_%d' % (name, g_ix), dat_fn, grp))
                out.write(';\n\n')


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


def field_groups(all_fields,
                 last_key='NAME', max_columns=1000):
    n_keys = all_fields[all_fields.variable_code == last_key].index.values[0]
    group = all_fields[:n_keys]
    g_ix = 0
    for table_code in all_fields.table_source_code[n_keys + 1:].unique():
        # print '========', table_code
        table_fields = all_fields[all_fields.table_source_code == table_code]
        # print table_fields.index
        if len(group) + len(table_fields) > max_columns:
            yield g_ix, group
            g_ix += 1
            group = all_fields[:n_keys]
        group = group.append(table_fields)
    if len(group) > n_keys:
        yield g_ix, group


if __name__ == '__main__':
    def _script():
        from sys import argv
        from pathlib2 import Path

        main(argv, cwd=Path('.'))

    _script()
