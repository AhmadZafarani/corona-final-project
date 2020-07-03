# YA REZA
import psycopg2
import time


def most_popular_product_in_province():
    cur.execute('WITH tabtab AS (SELECT province, product_id, SUM(has_sold) AS total_has_sold, SUM(has_sold*price) AS total_price \
        FROM fp_stores_data GROUP BY province, product_id ORDER BY province, product_id) SELECT DISTINCT ON (province) * FROM \
        tabtab ORDER BY province, total_has_sold DESC')
    result = cur.fetchall()
    for r in result:
        print("best seller product in %s"%r[0] + " province was %d."%r[1] + " we sold %d product there"%r[2] + \
        " and earned %d$."%r[3])


def best_and_worst_ten_markets():
    cur.execute('SELECT market_id, total_price FROM fp_store_aggregation ORDER BY total_price DESC')
    result = cur.fetchall()
    print("top ten markets with most income :)\nmarket_id\t\ttotal_income")
    for i in range(10):
        print(result[i][0], "\t\t", result[i][1])
    print("and ten markets with the least income :(\nmarket_id\t\ttotal_income")
    for i in range(-1,-11,-1):
        print(result[i][0], "\t\t", result[i][1])


def best_time_in_city():
    cur.execute('SELECT DISTINCT ON (city) * FROM fp_city_aggregation ORDER BY city, total_has_sold DESC')
    result = cur.fetchall()
    print('rush time in each city:\ncity\t\ttime\t\ttotal_has_sold')
    for r in result:
        print(r[0], "\t\t", r[1], "\t\t", r[3])


def best_and_worst_ten_sellers():
    pass


def best_and_worst_ten_liquity():
    cur.execute('SELECT product_id, SUM(has_sold)*100/SUM(has_sold+quantity) AS liquid FROM fp_stores_data GROUP BY product_id \
        ORDER BY liquid DESC')
    result = cur.fetchall()
    print("top ten products with most liquidity :)\nproduct_id\t\tpercent")
    for i in range(10):
        print(result[i][0], "\t\t\t", result[i][1])
    print("and ten products with the least liquidity :)\nproduct_id\t\tpercent")
    for i in range(-1,-11,-1):
        print(result[i][0], "\t\t\t", result[i][1])


def sell_quantity_change_by_time():
    pass


starttime = time.time()
while True:
    conn = psycopg2.connect("dbname=fpdb user=postgres")
    cur = conn.cursor()

    most_popular_product_in_province()
    best_and_worst_ten_markets()
    best_time_in_city()
    best_and_worst_ten_sellers()
    best_and_worst_ten_liquity()
    sell_quantity_change_by_time()
    
    cur.close()
    conn.close()
    time.sleep(1800.0 - ((time.time() - starttime) % 1800.0))
