
import pandas as pd
import psycopg2
#from db_config import db_config




conn = psycopg2.connect(
    host = 'localhost',
    port = 5432,
    database = 'de_learning',
    user = 'de_learner',
    password = 'de_learning'

)


cur = conn.cursor()


