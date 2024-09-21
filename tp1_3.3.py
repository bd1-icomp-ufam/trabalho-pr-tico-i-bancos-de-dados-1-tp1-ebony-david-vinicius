from collections import defaultdict
import psycopg2

# Conexão com o banco de dados
connection = psycopg2.connect("user=postgres password=root host=localhost")
cursor = connection.cursor()

# Função para executar uma consulta SQL
def run_query(query):
    cursor.execute(query)
    return cursor.fetchall()

# Função para listar os 5 comentários mais úteis e com maior e menor avaliação
def A():
    product_asin = input('Digite o ASIN do produto: ')
    query = f'''
    (SELECT review.id, review.date, review.customer, review.rating, review.votes, review.helpful
     FROM review 
     JOIN product ON review.product_id = product.id
     WHERE product.asin = '{product_asin}'
     ORDER BY review.helpful DESC, review.rating DESC
     LIMIT 5)
    UNION ALL
    (SELECT review.id, review.date, review.customer, review.rating, review.votes, review.helpful
     FROM review 
     JOIN product ON review.product_id = product.id
     WHERE product.asin = '{product_asin}'
     ORDER BY review.helpful DESC, review.rating ASC
     LIMIT 5);
    '''
    return run_query(query)

# Função para listar produtos similares com maiores vendas
def B():
    product_asin = input('Digite o ASIN do produto: ')
    query = f'''
    SELECT p2.*
    FROM similar_product sp
    JOIN product p1 ON sp.product_A_asin = p1.asin
    JOIN product p2 ON sp.product_B_asin = p2.asin
    WHERE p1.asin = '{product_asin}' AND p2.salesrank > p1.salesrank
    UNION
    SELECT p2.*
    FROM similar_product sp
    JOIN product p1 ON sp.product_B_asin = p1.asin
    JOIN product p2 ON sp.product_A_asin = p2.asin
    WHERE p1.asin = '{product_asin}' AND p2.salesrank > p1.salesrank;
    '''
    return run_query(query)

# Função para mostrar a evolução diária das médias de avaliação
def C():
    product_asin = input('Digite o ASIN do produto: ')
    query = f'''
    SELECT review.date, AVG(review.rating)
    FROM review 
    JOIN product ON review.product_id = product.id
    WHERE product.asin = '{product_asin}'
    GROUP BY review.date
    ORDER BY review.date;
    '''
    return run_query(query)

# Função para listar os 10 produtos líderes de venda em cada grupo de produtos
def D():
    query = '''
    SELECT p.*
    FROM product p
    WHERE p.salesrank <= (
        SELECT MAX(p2.salesrank)
        FROM product p2
        WHERE p2.group_id = p.group_id
        GROUP BY p2.salesrank
        ORDER BY p2.salesrank DESC
        LIMIT 10
    );
    '''
    return run_query(query)

# Função para listar os 10 produtos com a maior média de avaliações úteis positivas por produto
def E():
    query = '''
    SELECT p.asin, p.title, AVG(r.helpful) AS avg_helpful
    FROM product p
    JOIN review r ON r.product_id = p.id
    WHERE r.helpful > 0
    GROUP BY p.asin, p.title
    ORDER BY avg_helpful DESC
    LIMIT 10;
    '''
    return run_query(query)

# Função para listar as 5 categorias com a maior média de avaliações úteis positivas
def F():
    query = '''
    SELECT c.category, AVG(r.helpful) AS avg_helpful
    FROM product p
    JOIN review r ON r.product_id = p.id
    JOIN categories c ON p.category_id = c.id
    WHERE r.helpful > 0
    GROUP BY c.category
    ORDER BY avg_helpful DESC
    LIMIT 5;
    '''
    return run_query(query)

# Função para listar os 10 clientes que mais fizeram comentários por grupo de produto
def G():
    query = '''
    SELECT p.group_id, r.customer, COUNT(r.id) AS review_count
    FROM product p
    JOIN review r ON r.product_id = p.id
    GROUP BY p.group_id, r.customer
    ORDER BY review_count DESC
    LIMIT 10;
    '''
    return run_query(query)

# Função para opção inválida
def invalid_option():
    print("Opção inválida selecionada!")
    return None

# Dicionário de opções
dashboard_dict = defaultdict(lambda: invalid_option, {
    "a": A,
    "b": B,
    "c": C,
    "d": D,
    "e": E,
    "f": F,
    "g": G,
    "sair": exit
})

# Função para exibir o menu
def menu():
    print("Escolha uma opção:")
    print(" a) Dado um produto, listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação")
    print(" b) Dado um produto, listar os produtos similares com maiores vendas do que ele")
    print(" c) Dado um produto, mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada")
    print(" d) Listar os 10 produtos líderes de venda em cada grupo de produtos")
    print(" e) Listar os 10 produtos com a maior média de avaliações úteis positivas por produto")
    print(" f) Listar as 5 categorias de produto com a maior média de avaliações úteis positivas por produto")
    print(" g) Listar os 10 clientes que mais fizeram comentários por grupo de produto")
    print(" sair) Sair do dashboard")
    return input("Selecione uma opção (a-g/sair): ")

# Loop principal
while True:
    op = menu()
    print()
    result = dashboard_dict[op]()
    if result is not None:
        print(result)
        print()
