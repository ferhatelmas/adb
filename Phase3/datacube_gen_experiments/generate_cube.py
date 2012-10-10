# Usage:
# first run join_everything.sql
#

# We implement the multiple zoom-levels for each dimension through sub-columns
# However only one of them (e.g. nation or region) from certain group (e.g. LOCATION) is used

# we may have a clustered index on product ID, containing only names, therefore, joining shall not be too expensive, but seeks!


dimensions = {
    'location': ['NULL', 'REGIONKEY', 'NATIONKEY'],
    'time': ['NULL', 'YEAR', 'YEARMONTH', 'DAY_OF_YEAR'],
    # it seem to be very very very slow because of product!! it may be better to use the initial database for queries with product, e.g. for simplicity we may set up a view!!! 
    # TODO: use a VIEW for product ID!!
    'product': ['NULL', ]
}

# Define which column combinations to be included, and while will not be materialized
product_overrides = {'time': ['NULL', 'YEAR',]}



import itertools

def get_cube_sql(dimensions, exclude = {}):
    combinations = itertools.product(dimensions['location'], dimensions['time'], dimensions['product'])

    queries = []
    for comb in combinations:
        location, time, product = comb
        
        group_by = ", ".join([ group_name
          for (group_name, group_col) in {'LOCATION': location, 'TIME': time, 'PRODUCT': product}.items() 
          if group_col != 'NULL'])
        
        # TODO: how do we store everything? separate tables? not scalable?
        # TODO: in one table either many empty values, or column types would not mathc now
        selection = 'SELECT %s AS LOCATION, %s AS TIME, %s AS PRODUCT, P_NAME AS PRODUCT_NAME,  SUM(VOLUME) AS SUM_VOLUME, AVG(VOLUME) AS AVG_VOLUME FROM fulljoin ' % comb
        
        query = selection + (group_by and ' GROUP BY ' + group_by or '')
        queries.append(query)
    return "\nUNION\n".join(queries)    

# Datacube with completely materialized columns (no detailed product)
normal_tables = get_cube_sql(dimensions)

# Materialize Product only for some time dimensions (all, year)
dimensions_with_time_excl = dimensions.copy()
dimensions_with_time_excl['product']= ['PRODUCT_ID', ]
dimensions_with_time_excl['time'] = product_overrides['time']

product_with_time_excluded = get_cube_sql(dimensions_with_time_excl);

# 4 MB (no product)
#TODO: print "create table cube_no_prod\n"+ normal_tables + ";\n";


print "create table cube_prod_lzoom_time\n"+ product_with_time_excluded + ";\n"
# 310 MB full product cube, with s=0.1. if excluded product with high time precision, we get: 96.2MB just exactly as original size
# storage usage may be lowered by using IDs instead of names, which may be downloaded before hand

# if externalizing region and nation and product name, we save down to ~88MB
# however if  product_name was materialized, we get: 139.3 MiB

# if storing all dimensions, with product materialized and including product_name we get: 


# For the rest of time dimensions (year_month, day) we use non materialized view, instead for the Product-subset of datacube
# TODO: select only non-materialized time zoom-levels
dimensions['product']= ['PRODUCT_ID', ]
print "create view prod_cube AS \n"+ get_cube_sql(dimensions)  + ";\n"
