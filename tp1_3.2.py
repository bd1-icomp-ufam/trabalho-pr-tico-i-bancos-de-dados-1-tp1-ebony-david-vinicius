import psycopg2
import re
from datetime import datetime
from collections import defaultdict
similar_asin_dict = defaultdict(lambda: [])

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Conectar ao banco de dados
conn = psycopg2.connect(
    
    user='postgres',
    password='root',
    host='localhost'
)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()

with open('bd_criar.sql', 'r') as file:
  cursor.execute(file.read())


def insert_group_name(cursor, group_id, name):
    try:
        # Verificar se o grupo já existe
        cursor.execute('SELECT id FROM group_name WHERE name = %s', (name,))
        existing_group = cursor.fetchone()
        if existing_group:
            return False, existing_group[0]
        else:
            try:
                cursor.execute('''
                    INSERT INTO group_name (id, name)
                    VALUES (%s, %s)
                ''', (group_id, name))
                conn.commit()  
                return True, group_id
            except psycopg2.Error as e:
                conn.rollback()  
                print(f"Error inserting data: {e}")
    except psycopg2.Error as e:
        conn.rollback()  
        print(f"Error inserting data: {e}")
    return False, -1

def insert_product(cursor, asin, title, salesrank, group_id):
    try:
        cursor.execute('''
            INSERT INTO Product (asin, title, salesrank, group_id)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        ''', (asin, title, salesrank, group_id))
        conn.commit()  
        return cursor.fetchone()[0]
    except psycopg2.Error as e:
        conn.rollback()  
        print(f"Error inserting product: {e}")
        return -1

def insert_category(cursor, id, category):
    try:
        # Verificar se a categoria já existe
        cursor.execute('SELECT id FROM Categories WHERE id = %s', (id,))
        existing_category = cursor.fetchone()
        if existing_category:
            pass
        else:
            try:
                cursor.execute('''
                    INSERT INTO Categories (id, category)
                    VALUES (%s, %s)
                ''', (id, category))
                conn.commit()  
            except psycopg2.Error as e:
                conn.rollback()  
                print(f"Error inserting category: {e}")
    except psycopg2.Error as e:
        conn.rollback()  
        print(f"Error inserting category: {e}")

def insert_similar_product(cursor, product_A_asin, product_B_asin ):
    try:
        cursor.execute('''
            INSERT INTO Similar_Product (product_A_asin, product_B_asin)
            VALUES (%s, %s)
        ''', (product_A_asin, product_B_asin))
        conn.commit()  
    except psycopg2.Error as e:
        conn.rollback()  
        print(f"Error inserting Similar Product: {e}")

def insert_category_product(cursor, category_id, product_id):
    try:
        # Verificar se a relação já existe
        cursor.execute('''
            SELECT category_id, product_id
            FROM Category_Product
            WHERE category_id = %s AND product_id = %s
        ''', (category_id, product_id))

        existing_relation = cursor.fetchone()

        if existing_relation:            pass
        else:
            cursor.execute('''
                INSERT INTO Category_Product (category_id, product_id)
                VALUES (%s, %s)
            ''', (category_id, product_id))
            conn.commit()  
    except psycopg2.Error as e:
        conn.rollback()  
        print(f"Error inserting into Category_Product: {e}")

def insert_review(cursor, product_id, date, customer, rating, votes, helpful):
    try:
        cursor.execute('''
            INSERT INTO Review (product_id, date, customer, rating, votes, helpful)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (product_id, date, customer, rating, votes, helpful))
        conn.commit()  
        return True
    except psycopg2.Error as e:
        conn.rollback()  
        print(f"Error inserting into Review: {e}")




with open('amazon-meta.txt', 'r') as file:
    contagem_produtos = 0
    current_id = None
    asin = None
    title = None
    group = None
    salesrank = None
    category_id = None
    group_id = 1
    review_id = 1
    similar_asinA = []
    similar_asinB = []

    categorias_desse_produto = []
    grupo_desse_produto = -1
    reviews_desse_produto = []
    comecou_reviews = False

    for line in file:

        if line.startswith('Id:'):
            comecou_reviews = False
            current_id = int(line.split()[1])
        elif line.startswith('ASIN:'):
            comecou_reviews = False
            asin = line.split()[1]
        elif line.startswith('  title:'):
            comecou_reviews = False
            title = line.split(': ', 1)[1]
        elif line.startswith('  group:'):
            comecou_reviews = False
            group = line.split(': ', 1)[1]
            inseriu, grupo_desse_produto = insert_group_name(cursor, group_id, group)
            if inseriu:	
                group_id += 1
            	#print(group_id)
        elif line.startswith('  salesrank:'):
            comecou_reviews = False
            salesrank = int(line.split()[1])
        elif line.startswith('  similar:'):
            comecou_reviews = False
            similar_asins = line.split()[2:]
            similar_asin_dict[asin] = similar_asins

                
        elif line.startswith('  categories:'):
            comecou_reviews = False
            continue
        

        elif line == '\n' and grupo_desse_produto != -1:
            product_id = insert_product(cursor, asin, title, salesrank, grupo_desse_produto)
            if product_id == -1:
                continue
            for category_id in categorias_desse_produto:
                insert_category_product(cursor, category_id, product_id)
            for review in reviews_desse_produto:
                insert_review(cursor, product_id, review['date'], review['customer'], review['rating'], review['votes'], review['helpful'])
            comecou_reviews = False
            grupo_desse_produto = -1
            categorias_desse_produto = []
            reviews_desse_produto = []
            contagem_produtos += 1
            
            if contagem_produtos % 1000 == 0:
                print(f"inseriu {contagem_produtos} produtos!")


        elif line.startswith('  reviews:'):
            comecou_reviews = True

        elif comecou_reviews:
            comecou_reviews = True
            review_info = line.split()
            date_str = review_info[0]
            date = datetime.strptime(date_str, '%Y-%m-%d').date()
            customer = review_info[2]
            rating = int(review_info[4])
            votes = int(review_info[6])
            helpful = int(review_info[8])
            review_data = {'date': date, 'customer': customer, 'rating': rating,
                            'votes': votes, 'helpful': helpful}
            reviews_desse_produto.append(review_data)

        elif line.startswith('   |'):
            comecou_reviews = False
            #matches = re.findall(r'\[([0-9]+)\]\|([^[]+)', line)
            matches = re.findall(r'\|([^[]+)\[([0-9]+)\]', line)

            #print(matches)
            for match in matches:
                category_name, category_id = match
                insert_category(cursor, category_id, category_name)
                categorias_desse_produto.append(category_id)

    product_id = insert_product(cursor, asin, title, salesrank, grupo_desse_produto)
    for category_id in categorias_desse_produto:
        insert_category_product(cursor, category_id, product_id)
    for review in reviews_desse_produto:
        insert_review(cursor, product_id, review['date'], review['customer'], review['rating'], review['votes'], review['helpful'])
    contagem_produtos += 1
    
    if contagem_produtos % 1000 == 0:
        print(f"inseriu {contagem_produtos} produtos!")

    for asinA, asinsB in similar_asin_dict.items():
        for asinB in asinsB:
            insert_similar_product(cursor, asinA, asinB)


conn.close()

