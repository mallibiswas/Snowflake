## The purpose of this script is to validate if for every source table available in sources a staging table is available.a
import os, os.path
import errno
import yaml

source_path = '../models/sources'
staging_path = '../models/staging'
create_missing_staging = False

# Taken from https://stackoverflow.com/a/600612/119527
def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

def safe_open_w(path):
    ''' Open "path" for writing, creating any parent directories as needed.
    '''
    mkdir_p(os.path.dirname(path))
    return open(path, 'w')

for subdir, dirs, files in os.walk(source_path):
    if len(files) > 0:
        subdir = subdir[subdir.index("sources/")+8:]
    for file in files:
        filename, extension = os.path.splitext(file)
        if extension.lower() == '.sql':
            staging_file = os.path.join(staging_path, subdir, file).replace("/src_", "/stg_")
            if create_missing_staging and not os.path.exists(staging_file):
                print("creating missing staging model:",  staging_file)
                f = safe_open_w(staging_file)
                f.write("""SELECT * FROM {{ ref('%s') }}""" % file.replace(".sql", ""))
                f.close()
            elif not os.path.exists(staging_file):
                print("Please create the following missing staging model:",  staging_file)
        if extension.lower() == '.yml':
            with open(os.path.join(source_path, subdir, file), 'r') as stream:
                try:
                    dbt_config = yaml.load(stream)
                    if "sources" in dbt_config:
                        for source in dbt_config.get("sources"):
                            for table in source["tables"]:
                                dir = os.path.split(subdir)
                                staging_file = os.path.join(staging_path, subdir, "stg_" + dir[len(dir)-1] + "__" + table["name"].lower() + ".sql")
                                if create_missing_staging and not os.path.exists(staging_file):
                                    print("creating missing staging model:",  staging_file)
                                    f = safe_open_w(staging_file)
                                    f.write("""SELECT * FROM {{ source('%s', '%s') }}""" % (source["name"], table["name"]))
                                    f.close()
                                elif not os.path.exists(staging_file):
                                    print("Please create the following missing staging model:",  staging_file)
                except yaml.YAMLError as exc:
                    print(exc)