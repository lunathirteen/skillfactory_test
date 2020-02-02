import json
from datetime import datetime

import pandas as pd
import requests
import sqlalchemy


def get_structure_course(url):
    """Function gets course structure from API"""

    try:
        r = requests.post(url)
        r.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        print('HTTPError: {}'.format(errh))
        print(r.content.decode('UTF-8'))
        raise

    result = json.loads(r.text)

    return result


def print_structure(data, root_id, result={}, output='\t'):
    """Function prints course structure and saves result in pd.DataFrame"""

    if data['blocks'][root_id].get('children', 0) == 0:
        return
    else:
        output_string = output + str(data['blocks'][root_id]['display_name'])+': '+str(data['blocks'][root_id]['block_id'])
        print(output_string)
        result[data['blocks'][root_id]['display_name']] = data['blocks'][root_id]['block_id']
        for child_id in data['blocks'][root_id]['children']:
            print_structure(data, child_id, result, '\t' + output)

    return result


def select(sql, **kwargs):
    return pd.read_sql(sql, **kwargs)


def drop_table(table_name, engine):
    conn = engine.connect()
    conn.execute('DROP TABLE IF EXISTS {};'.format(table_name))


def main():

    url = 'http://84.201.129.203:4545/get_structure_course'

    DB_USER = 'user2'
    DB_PASSWORD = 'qtybcgt++H6'
    DB_HOST = '84.201.129.203'
    DB_PORT = '32769'
    DB_NAME = 'test'

    engine = sqlalchemy.create_engine(
        'mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}'.format(
            user=DB_USER,
            pwd=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            db=DB_NAME))

    # Drop table if exists
    try:
        drop_table('test_lunev', engine)
    except Warning:
        pass

    # Load course structure from API
    course_structure_json = get_structure_course(url)

    children_id = set()
    for block in course_structure_json['blocks']:
        children_id.update(course_structure_json['blocks'][block].get('children', []))

    node_id = dict()
    for block in course_structure_json['blocks']:
        if course_structure_json['blocks'][block]['id'] in children_id:
            node_id[course_structure_json['blocks'][block]['id']] = course_structure_json['blocks'][block]['display_name']
        else:
            root_id = course_structure_json['blocks'][block]['id']

    # Print course structure
    course_structure_dict = print_structure(course_structure_json, root_id)

    course_structure_df = pd.DataFrame(
        course_structure_dict.items(),
        columns=['display_name', 'id'])

    course_structure_df['time'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    # Push course structure to MySQL DB

    course_structure_df.to_sql('test_lunev', con=engine, index=False)

    from pandas.testing import assert_frame_equal
    assert_frame_equal(course_structure_df, select('SELECT * FROM test.test_lunev', con=engine))


if __name__ == "__main__":
    main()
