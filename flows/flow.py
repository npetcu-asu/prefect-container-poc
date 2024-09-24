from __future__ import annotations
import boto3
import gzip
import io
import numpy as np
import os
import pandas as pd
from prefect import prefect, Flow, task, unmapped, Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import ECSRun
from prefect.storage import Docker
from prefect.tasks.secrets import PrefectSecret
import psycopg2
import re
import sqlite3


SQL_ROOT_DIR = '/opt/prefect/flows/sql'
MAROON_PS_CONDITION_CODES_TO_DARS_CODES = {
  'C': 'c',
  'CS': 'Q',
  'G': 'g',
  'H': 'h',
  'HU': 'H',
  'L': 't',
  'MA': 'v',
  'SB': 'S',
  'SG': 'z',
  'SQ': 'y',
}
GOLD_PS_CONDITION_CODES_TO_DARS_CODES = {
  'HUAD': '¿',
  'SOBE': 'Ñ',
  'SCIT': 'ß',
  'QTRS': '£',
  'MATH': 'Æ',
  'AMIT': 'ÿ',
  'CIVI': '«',
  'GCSI': 'ñ',
  'SUST': 'ù'
}


@task
def load_requirement_years_query():
    with open(f'{SQL_ROOT_DIR}/requirement-years.sql', encoding='utf-8') as sql_file:
        return sql_file.read()


@task
def load_sub_requirements_query():
    with open(f'{SQL_ROOT_DIR}/sub-requirements.sql', encoding='utf-8') as sql_file:
        return sql_file.read()


@task
def load_offered_courses_query():
    with open(f'{SQL_ROOT_DIR}/offered-courses.sql', encoding='utf-8') as sql_file:
        return sql_file.read()


@task
def load_accept_reject_criteria_query():
    with open(f'{SQL_ROOT_DIR}/accept-reject-criteria.sql', encoding='utf-8') as sql_file:
        return sql_file.read()


@task
def load_courses_join_crosswalk_query():
    with open(f'{SQL_ROOT_DIR}/courses-join-crosswalk.sql', encoding='utf-8') as sql_file:
        return sql_file.read()


@task
def load_courses_join_sub_reqs_query():
    with open(f'{SQL_ROOT_DIR}/courses-join-sub-reqs.sql', encoding='utf-8') as sql_file:
        return sql_file.read()


@task(log_stdout=True)
def query_offered_courses(
    offered_courses_query: str,
    db_name: str,
    db_host: str,
    db_port: str,
    db_user: str,
    db_password: str
) -> pd.DataFrame:
    '''
    Retrieves the offered courses from ODS.

    Args:
        offered_courses_query: The query used to retrieve the offered courses.
        db_name: The database name.
        db_host: The database host.
        db_port: The database port.
        db_user: The database user's application ID.
        db_password: The database user's password.

    Returns:
        A DataFrame containing the offered course data.
    '''
    print('Obtaining ODS DB connection.')
    db_conn = psycopg2.connect(
        database=db_name,
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password
    )

    try:
        print('Getting offered courses from ODS.')
        offered_courses = pd.read_sql(offered_courses_query, db_conn)
        return offered_courses
    finally:
        db_conn.close()


def replace_ps_code_with_dars_code(value: str) -> str:
    '''
    Replaces general studies PS style codes with DARS style single-character codes.

    Args:
        value: The general studies condition code value.

    Returns:
        The value with all PS style codes replaced by DARS style single-character codes.
    '''
    ps_condition_codes = {**MAROON_PS_CONDITION_CODES_TO_DARS_CODES, **GOLD_PS_CONDITION_CODES_TO_DARS_CODES}
    for ps_code, dars_code in ps_condition_codes.items():
        # Use negative look around to avoid replacing just the first or last part of a code
        # (i.e. CS should be replaced with Q and not end up as cS, MATH should be replaced with Æ and not end up as MATh, etc.)
        value = re.sub(rf'(?<![a-zA-Z]){ps_code}(?![a-zA-Z])', dars_code, value)
    return value



def split_gold_and_maroon_combos(value: str) -> list[str]:
    '''
    Splits gold condition codes from maroon condition code combos to prevent the gold code from being
    treated as part of the combination.

    For example, designation code "GE11" maps to "HUAD OR (HU or SB) & C & H"
    This should be treated as:
    * HUAD
    * HU & C & H
    * SB & C & H

    Args:
        value: The designation code value to split, if necessary.

    Returns:
        A list containing value split into separate values if a gold condition code is present or the
        original value in a single-element list.
    '''
    value = value.replace(' OR ', ' or ')
    for dars_code in GOLD_PS_CONDITION_CODES_TO_DARS_CODES.values():
        if value.startswith(f'{dars_code} or '):
            return [dars_code, value.replace(f'{dars_code} or ', '')]
    return [value]



@task(log_stdout=True)
def import_course_crosswalk() -> pd.DataFrame:
    '''
    Retrieves the course crosswalk data.

    Returns:
        A DataFrame containing course crosswalk data.
    '''
    print('Retrieving crosswalk CSV from DARS/uAchieve public S3 bucket and loading into a DataFrame.')
    # Read the object byte-stream using pandas for transformation
    ps_req_des = pd.read_csv('https://dars-uachieve-cmdata-sharing-qa.s3.us-west-2.amazonaws.com/cmdata/condition_codes_qa.csv')

    print('Transforming crosswalk data.')
    # Replace PS style codes from crosswalk file with DARS single-character code equivalent for consistency with matching later on
    ps_req_des['DARS Subreq AC1-AC5'] = ps_req_des['DARS Subreq AC1-AC5'].apply(replace_ps_code_with_dars_code)

    # Split condition combinations that start with a gold code into a row for the gold and a row for the rest of the conditions
    ps_req_des['DARS Subreq AC1-AC5'] = ps_req_des['DARS Subreq AC1-AC5'].apply(split_gold_and_maroon_combos)
    ps_req_des = ps_req_des.explode('DARS Subreq AC1-AC5')

    # Split up columns by "&"
    ps_req_des[['ac1', 'ac2', 'ac3', 'ac4']] = ps_req_des['DARS Subreq AC1-AC5'].str.split(pat='&', expand=True)
    ps_req_des.drop('DARS Subreq AC1-AC5', axis=1, inplace=True)

    # Copy "or" values to multiple rows with one of the or values in each row.  Force all to lowercase
    # before splitting as crosswalk has mixed case.
    ps_req_des.ac1 = ps_req_des.ac1.apply(lambda x: x.split('or'))
    ps_cw = ps_req_des.explode('ac1')

    # Clean up spaces and other characters
    for i in ['ac1', 'ac2', 'ac3', 'ac4']:
        ps_cw[i] = ps_cw[i].replace(regex=r'[() ]', value='')
        ps_cw[i] = ps_cw[i].apply(lambda x: x or '')

    # Merge columns back together so that each row has a complete combination of eligible matching criteria
    ps_cw['ac_cw'] = ps_cw['ac1'] + ps_cw['ac2'] + ps_cw['ac3'] + ps_cw['ac4']
    ps_cw.drop(['ac1', 'ac2', 'ac3', 'ac4'], axis=1, inplace=True)
    ps_cw = pd.concat([ps_cw, pd.DataFrame({'PS Requirement Designation': ['-'], 'ac_cw': ['']})], ignore_index=True)

    print('Crosswalk data transformation complete. Returning result.')
    return ps_cw


@task(log_stdout=True)
def query_requirement_years(
    requirement_years_query: str,
    db_name: str,
    db_host: str,
    db_port: str,
    db_user: str,
    db_password: str
) -> list[dict]:
    '''
    Retrieves all requirement years from ODS.

    Args:
        requirement_years_query: The query used to retrieve all requirement years.
        db_name: The database name.
        db_host: The database host.
        db_port: The database port.
        db_user: The database user's application ID.
        db_password: The database user's password.

    Returns:
        A list of dicts of requirement year values.
    '''
    print('Obtaining ODS connection.')
    db_conn = psycopg2.connect(
        database=db_name,
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password
    )
    cur = db_conn.cursor()

    try:
        print('Querying requirement years.')
        cur.execute(requirement_years_query)
        # Parse out the requirement year values from the list of tuples returned by the query
        requirement_years = [{'rqfyt': rqfyt, 'lyt': lyt} for rqfyt, lyt in cur]
        return requirement_years
    finally:
        cur.close()
        db_conn.close()


@task(log_stdout=True)
def query_sub_requirements(
    req_years: dict,
    sub_requirements_query: str,
    db_name: str,
    db_host: str,
    db_port: str,
    db_user: str,
    db_password: str
) -> pd.DataFrame:
    '''
    Retrieves sub-requirement data from ODS.

    Args:
        req_year: The requirement year to retrieve sub-requirements for.
        sub_requirements_query: The query used to retrieve sub-requirement data.
        db_name: The database name.
        db_host: The database host.
        db_port: The database port.
        db_user: The database user's application ID.
        db_password: The database user's password.

    Returns:
        A DataFrame containing sub-requirement data.
    '''
    print('Obtaining ODS connection.')
    db_conn = psycopg2.connect(
        database=db_name,
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password
    )
    try:
        print(f'Querying sub-requirements from ODS for requirement years {req_years}.')
        return pd.read_sql(sub_requirements_query, db_conn, params={'rqfyt': req_years['rqfyt'], 'lyt': req_years['lyt']})
    finally:
        db_conn.close()


@task(log_stdout=True)
def transform_sub_requirements(sub_requirements: pd.DataFrame) -> pd.DataFrame:
    '''
    Performs cleanup and transformations on the sub-requirements raw data.

    Args:
        sub_requirements: A DataFrame containing the raw data returned from the sub-requirements query.

    Returns:
        A DataFrame containing the cleaned up and transformed sub-requirements to be used in the final transformation.
    '''
    print('Transforming sub-requirement results.')
    # Convert requirement years to strm codes
    convert_requirement_year = lambda req_year: req_year.replace(' ', '')[0:1] + req_year.replace(' ', '')[2:]
    sub_requirements['rqfyt'] = sub_requirements['rqfyt'].apply(lambda year: convert_requirement_year(year))
    sub_requirements['lyt'] = sub_requirements['lyt'].apply(lambda year: convert_requirement_year(year))
    sub_requirements['rqfyt2'] = sub_requirements['rqfyt2'].apply(lambda year: convert_requirement_year(year) if year else year)
    sub_requirements['lyt2'] = sub_requirements['lyt2'].apply(lambda year: convert_requirement_year(year) if year else year)

    # Create upper-division and lower-division column
    sub_requirements['upper_lower_acc'] = ''
    sub_requirements['upper_lower_rej'] = ''

    # General studies condition code groupings
    maroon_condition_codes = ('G','H','Q','S','c','g','h','t','v','w','x','y','z')

    # Format the accept criteria and reject criteria
    for i in ['r_ac1', 'r_ac2', 'r_rc1', 'r_rc2', 'ac', 'rc', 'ac1', 'ac2', 'ac3', 'ac4', 'ac5', 'rc1', 'rc2', 'rc3', 'rc4', 'rc5']:
        # Get rid of extra spaces
        sub_requirements[i] = sub_requirements[i].str.replace(' ', '', regex=False)
        # Convert null values to empty strings
        sub_requirements[i] = sub_requirements[i].apply(lambda x: x or '')

        # Save upper division and lower division criteria real quick
        if i in ['r_ac1', 'r_ac2', 'ac', 'ac1', 'ac2', 'ac3', 'ac4', 'ac5']:
            # Add "U" or "L" to the upper/lower accept column
            sub_requirements['upper_lower_acc'] = sub_requirements['upper_lower_acc'] + sub_requirements[i].apply(lambda x: x if x in ('U', 'L') else '')
        else:
            # Add "U" or "L" to the upper/lower reject column
            sub_requirements['upper_lower_rej'] = sub_requirements['upper_lower_rej'] + sub_requirements[i].apply(lambda x: x if x in ('U', 'L') else '')

        # Filter to just the values that relate to general studies requirements or upper and lower division and not other stuff like gpa or min grade
        sub_requirements[i] = sub_requirements[i].apply(lambda x: x if x in maroon_condition_codes or x in GOLD_PS_CONDITION_CODES_TO_DARS_CODES.values() else '')

    # Merge the ac columns for ease of matching in the next steps
    sub_requirements['ac_5'] = sub_requirements['ac1'] + sub_requirements['ac2'] + sub_requirements['ac3'] + sub_requirements['ac4'] + sub_requirements['ac5']
    sub_requirements[['acor', 'ac_5']] = sub_requirements[['acor', 'ac_5']].replace(np.nan, '')

    # If acor is not empty or null, make a list and then explode the values to different rows for joining and matching later one
    sub_requirements['ac_5'] = np.where(sub_requirements.acor.str.contains('-'), sub_requirements['ac_5'].apply(lambda x: list(x) or ''), sub_requirements['ac_5'])
    sub_requirements = sub_requirements.explode('ac_5')

    # Incorporate the other sub-requirement level ac and the requirement level ac
    sub_requirements['ac_all'] = sub_requirements['r_ac1'] + sub_requirements['r_ac2'] + sub_requirements['ac'] + sub_requirements['ac_5']
    # Drop the old ac columns
    sub_requirements.drop(['r_ac1', 'r_ac2', 'ac', 'ac1', 'ac2', 'ac3', 'ac4', 'ac5', 'ac_5', 'acor'], axis=1, inplace=True)

    # Sort rc values into two groups, rc_ord, where any one value is cause for rejection, and rc_and, where
    # having all values will lead to rejection. Check for ands and merge appropriate columns together if appropriate.
    sub_requirements['rcand'] = sub_requirements['rcand'].replace(np.nan, '')
    sub_requirements['rc_and'] = np.where(sub_requirements['rcand'] == '-', sub_requirements['rc1'] + sub_requirements['rc2'] + sub_requirements['rc3'] + sub_requirements['rc4'] + sub_requirements['rc5'], '')

    # Combine the rest as "or'd" and add the rc1-5 if rcand is not checked
    sub_requirements['rc_ord'] = sub_requirements['r_rc1'] + sub_requirements['r_rc2'] + sub_requirements['rc'] + np.where(sub_requirements['rcand'] == '-', '', sub_requirements['rc1'] + sub_requirements['rc2'] + sub_requirements['rc3'] + sub_requirements['rc4'] + sub_requirements['rc5'])
    # Drop the old rc columns
    sub_requirements.drop(['r_rc1', 'r_rc2', 'rc', 'rc1', 'rc2', 'rc3', 'rc4', 'rc5', 'rcand'], axis=1, inplace=True)

    print('Sub-requirement transformation completed successfully.')
    return sub_requirements


@task(log_stdout=True)
def transform_to_final_result(
    sub_requirements: pd.DataFrame,
    offered_courses: pd.DataFrame,
    course_crosswalk: pd.DataFrame,
    accept_reject_criteria_query: str,
    courses_join_crosswalk_query: str,
    courses_join_sub_reqs_query: str
) -> pd.DataFrame:
    '''
    Performs the necessary transformations to produce all courses that satisfy degree sub-requirements.

    Args:
        sub_requirements: A DataFrame containing the sub-requirements.
        offered_courses: A DataFrame containing all offered courses.
        course_crosswalk: A DataFrame containing the course crosswalk data.
        accept_reject_criteria_query: The query used to find requirement acceptance and rejection criteria.
        courses_join_crosswalk_query: The query used to join the crosswalk details to the course offerings.
        courses_join_sub_reqs_query: The query used to produce all courses that satisfy sub-requirements.

    Returns:
        A DataFrame containing the final set of courses that satisfy sub-requirements.
    '''
    # Using the in-memory option for sqlite3 resulted in the tasks failing due to running out of
    # memory during the final transformation step as huge amount of data is produced by the
    # query.  Using the db file instead is slower but the tasks succeed.
    print('Creating local database.')
    db_file_name = f"/opt/prefect/cremo-idp-sub-req-${prefect.context.get('map_index')}.db"
    db_conn = sqlite3.connect(db_file_name)
    cursor = db_conn.cursor()
    cursor.execute('PRAGMA case_sensitive_like = 1;')
    # cursor.execute('PRAGMA journal_mode = wal;') # Enables concurrent readers with one writer when applying DB changes
    # cursor.execute('PRAGMA synchronous = normal;') # Syncs DB with file system less often

    try:
        print('Transforming final results.')
        # Get distinct acceptance and rejection criteria and figure out which requirement designations match
        acc_rej_criteria = sub_requirements[['ac_all', 'rc_and', 'rc_ord']].copy().drop_duplicates().reset_index().drop('index', axis=1)
        acc_rej_criteria.to_sql('acc_rej_crit', db_conn, index=False)

        # Load course crosswalk into local database
        course_crosswalk.to_sql('ps_cw', db_conn, index=False)

        # Query with accept_reject_criteria_query to split up rows
        ac_criteria_met = pd.read_sql(accept_reject_criteria_query, db_conn)

        # Load offered courses into local database
        offered_courses.to_sql('ps_course', db_conn, index=False)

        # Query to join offered courses to crosswalk
        courses = pd.read_sql(courses_join_crosswalk_query, db_conn)

        # Load sub-requirements into local database
        sub_requirements.to_sql('reqs', db_conn, index=False)

        # Load joined crosswalk to offered courses result into local database
        courses.to_sql('courses', db_conn, index=False)

        # Load ac_criteria_met into local database
        ac_criteria_met.to_sql('ac_criteria_met', db_conn, index=False)

        print('Querying list of offered courses that meet requirements.  This will take a while.')
        reqs_aug = pd.read_sql(courses_join_sub_reqs_query, db_conn)

        print('Transformation of final results complete. Return result.')
        return reqs_aug
    finally:
        # Close the database connection and delete the local .db file to free up
        # disk space for the other mapped tasks.
        cursor.close()
        db_conn.close()
        os.remove(db_file_name)


@task(log_stdout=True)
def put_final_result_in_data_lake(result: pd.DataFrame):
    '''
    Converts the final transformed DataFrame into a CSV, compresses the file,
    and puts it into the EDS Data Lake S3 bucket.

    Args:
        result: The final transformed DataFrame result.
    '''
    print('Converting the final result DataFrame into a CSV and compressing.')
    # Write the result Pandas DataFrame to a buffer
    csv_buffer = io.StringIO()
    result.to_csv(csv_buffer, index=False)

    # Compress the CSV
    gzipped_csv = gzip.compress(csv_buffer.getvalue().encode('utf-8'))

    print('Putting the compressed CSV into the EDS Data Lake S3 bucket.')
    s3_client = boto3.client('s3', region_name='us-west-2')
    s3_client.put_object(
        Bucket='asu-s3dl-eds',
        Key=f"raw/cremo-idp/sub-reqs-courses-part-{prefect.context.get('map_index')}.csv.gz",
        Body=gzipped_csv
    )


# Docker storage configuration for flow.
docker_storage = Docker(
    registry_url=os.getenv('REGISTRY_URL'),
    image_name=os.getenv('IMAGE_NAME'),
    dockerfile='Dockerfile'
)


# ECS run configuration for flow.
# Possible CPU/memory values documented in link:
# https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_definition_parameters.html
ecs_run_config = ECSRun(
    task_role_arn=os.getenv('TASK_ROLE_ARN'),
    execution_role_arn=os.getenv('EXECUTION_ROLE_ARN'),
    cpu=16384,     # 16vCPU
    memory=122880  # 120 GB
)


with Flow('cremo-idp-sub-req-courses', executor=LocalDaskExecutor(scheduler='threads', num_workers=8), storage=docker_storage, run_config=ecs_run_config) as flow:
    '''
    Orchestrates the execution of tasks.

    LocalDaskExecutor allows for parallel execution of tasks.  By default, the number of threads or processes
    used is equal to the number of cores available.  Prefect suggests using the threads scheduler instead of
    the processes scheduler for tasks that are IO bound (DB calls, uploading/downloading data, etc.) and for
    tasks that make use of pandas or numpy libraries.
    See https://discourse.prefect.io/t/what-is-the-difference-between-a-daskexecutor-and-a-localdaskexecutor/374#selecting-a-scheduler-and-num_workers-4
    '''

    # Retrieve flow parameters
    db_name = Parameter('ods_db_name', default='ods')
    db_host = Parameter('ods_db_host', default='ods-np-ro.asu.edu')
    db_port = Parameter('ods_db_port', default='5432')

    # Retrieve ODS credentials from Prefect secrets storage
    # db_user = PrefectSecret('cremo-idp-sub-req-courses-username')
    # db_password = PrefectSecret('cremo-idp-sub-req-courses-non-prod-password')
    db_user = 'test'
    db_password = 'test'

    # Load all queries from SQL files
    requirement_years_query = load_requirement_years_query()
    sub_requirements_query = load_sub_requirements_query()
    offered_courses_query = load_offered_courses_query()
    accept_reject_criteria_query = load_accept_reject_criteria_query()
    courses_join_crosswalk_query = load_courses_join_crosswalk_query()
    courses_join_sub_reqs_query = load_courses_join_sub_reqs_query()

    # Run tasks.  Tasks will be run in parallel where possible or will wait until all arguments become available
    # from previous tasks.
    offered_courses = query_offered_courses(offered_courses_query, db_name, db_host, db_port, db_user, db_password)
    course_crosswalk = import_course_crosswalk()
    requirement_years = query_requirement_years(requirement_years_query, db_name, db_host, db_port, db_user, db_password)
    sub_requirements = query_sub_requirements.map(requirement_years, unmapped(sub_requirements_query), unmapped(db_name), unmapped(db_host), unmapped(db_port), unmapped(db_user), unmapped(db_password))
    transformed_sub_requirements = transform_sub_requirements.map(sub_requirements)
    final_result = transform_to_final_result.map(transformed_sub_requirements, unmapped(offered_courses), unmapped(course_crosswalk), unmapped(accept_reject_criteria_query), unmapped(courses_join_crosswalk_query), unmapped(courses_join_sub_reqs_query))
    # put_final_result_in_data_lake.map(final_result)

flow.run()