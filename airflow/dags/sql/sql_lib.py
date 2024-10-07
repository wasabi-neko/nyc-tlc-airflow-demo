# set query_tag = {arg.tag}
# use warehouse = {arg.warehouse}

# create external stage
    # stage_name, tag, warehouse
# create yellow table
    # table name
# copy into yellow table from external stage // same as internal stage
    # table name, stage name
# create taxi stage
    #  stage name
# create taxi table
    # table name
# join yellow & taxi table
    # yellow table, taxi table

# create external stage
    # stage name
# create internal stage
    # stage name
# copy file into internal stage from external stage
    # in table, ex table
# create yellow table
# copy into yellow table fomr internal stage
# create taxi table
# create taxi table
# join yellow & taxi table

# drop everything (stages, tables)


# TODO: make each of them a custom airflow operator
