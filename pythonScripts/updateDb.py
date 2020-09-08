import mysql.connector
from random import choice, uniform
from time import sleep

def update_movie():
    # read database configuration
    mydb = mysql.connector.connect(
        host="localhost",
        user="luca",
        password="passwd",
        database="mintopkn"
    )

    # prepare query and data
    query = """ UPDATE MovieIncome
                SET income = %s
                WHERE id = %s """

    movies = [100, 120, 128, 140, 294, 354, 782]

    try:

        # update movie income
        cursor = mydb.cursor()
        for i in range(500):
            sleep_time = uniform(0,2)
            sleep(sleep_time)
            movie_id = choice(movies)
            income = uniform(0,10)
            data = (income, movie_id)

            cursor.execute(query, data)

            # accept the changes
            mydb.commit()

    except mysql.connector.Error as error:
        print(error)

    finally:
        cursor.close()
        mydb.close()


if __name__ == '__main__':
    update_movie()