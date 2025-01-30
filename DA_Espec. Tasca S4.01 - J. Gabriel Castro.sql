CREATE TABLE companies (
    id VARCHAR(10) PRIMARY KEY,
    name VARCHAR(50),
    address VARCHAR(100),
    phone VARCHAR(20),
    email VARCHAR(100),
    country VARCHAR(20),
    city VARCHAR(20),
    postal_code VARCHAR(10));
    
    CREATE TABLE products (
    id VARCHAR(10) PRIMARY KEY,
    product_name VARCHAR(50),
    price DECIMAL(10, 2),
    colour VARCHAR(20),
    weight DECIMAL(10, 2),
    warehouse_id VARCHAR(12));

CREATE TABLE transactions (
    id VARCHAR(10) PRIMARY KEY,
    user_id VARCHAR(10),
    product_ids VARCHAR(50),
    amount DECIMAL(10, 2),
    transaction_date DATETIME,
    payment_method VARCHAR(20));

CREATE TABLE user_data (
    id VARCHAR(10),
    name VARCHAR(20),
    surname VARCHAR(40),
    phone VARCHAR(20),
    email VARCHAR(100),
    birth_date VARCHAR(12),
    country VARCHAR(20),
    city VARCHAR(20),
    postal_code VARCHAR(5),
    address VARCHAR(100));
    
CREATE TABLE credit_cards (
    id VARCHAR(11) PRIMARY KEY,
    user_id VARCHAR(10),
    card_number VARCHAR(16),
    card_type VARCHAR(20),
    expiry_date DATE,
    cvv VARCHAR(4));


-- En algunos casos tuve que cambiar el tipo de dato
alter table transactions
modify column amount decimal(10,2);
alter table transactions
modify column product_ids varchar(50);
alter table transactions
modify column user_id varchar(11);
alter table credit_cards
modify column id varchar(11) PRIMARY KEY;

-- Me salia el error 2068, de permisos de FILE. Después de mucha búsqueda y estes códigos, pude hacer el LOAD FILE. (via my.ini)
show global variables like "local_infile"; 
set global local_infile=1;
--
show grants;
GRANT FILE on *.* to 'root'@'localhost';
--
SHOW SESSION VARIABLES LIKE 'local_infile';
show variables like 'pid_file';

------------------------------------------------------------------------------------------------

LOAD DATA LOCAL INFILE 'C:/Users/gbrlc/Downloads/users_usa.csv' 
INTO TABLE user_data
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE 'C:/Users/gbrlc/Downloads/users_uk.csv' 
INTO TABLE user_data
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE 'C:/Users/gbrlc/Downloads/users_ca.csv' 
INTO TABLE user_data
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE 'C:/Users/gbrlc/Downloads/transactions.csv' 
INTO TABLE transactions
FIELDS TERMINATED BY ';' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE 'C:/Users/gbrlc/Downloads/products.csv' 
INTO TABLE products
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE 'C:/Users/gbrlc/Downloads/credit_cards.csv' 
INTO TABLE credit_cards
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE 'C:/Users/gbrlc/Downloads/companies.csv' 
INTO TABLE companies
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;
------------------------------------------------------------------------------------------------

-- CREAR CODIGO DE FK Y CONSTRAINTS
-- Entre user_data e credit_cards
ALTER TABLE credit_cards
ADD CONSTRAINT fk_credit_cards_user_id FOREIGN KEY (user_id)
REFERENCES user_data(id);

ALTER TABLE transactions
ADD CONSTRAINT fk_transactions_product_ids FOREIGN KEY (product_ids)
REFERENCES products(id);

-- Entre transactions e credit_cards
ALTER TABLE transactions
ADD CONSTRAINT fk_transactions_card_id FOREIGN KEY (card_id)
REFERENCES credit_cards(id);
-- Relacionamento entre active_cards e credit_cards
ALTER TABLE active_cards
ADD CONSTRAINT fk_active_cards_card_id FOREIGN KEY (card_id)
REFERENCES credit_cards(id);
-- Entre transactions e user_data
ALTER TABLE transactions
ADD CONSTRAINT fk_transactions_user_id FOREIGN KEY (user_id)
REFERENCES user_data(id);
-- Entre transactions e companies
ALTER TABLE transactions
ADD CONSTRAINT fk_transactions_business_id FOREIGN KEY (business_id)
REFERENCES companies(company_id);

-- Creo una tabla intermediatia porque muchos productos pueden estar en muchas transacciones,
-- y muchas transacciones pueden comprar muchos productos.

create table products_transactions (
	product_id varchar(10),
    transaction_id varchar(10));
-- Entre transactions_products y products
ALTER TABLE products_transactions
ADD CONSTRAINT fk_products_transacctions_product_id FOREIGN KEY (product_id)
REFERENCES products(id);
-- Entre transactions_products y transactions
ALTER TABLE products_transactions
ADD CONSTRAINT fk_products_transacctions_transaction_id FOREIGN KEY (transaction_id)
REFERENCES transactions(id);

########### Nivel l ###########

# Descàrrega els arxius CSV, estudia'ls i dissenya una base de dades amb un esquema d'estrella que contingui,
# almenys 4 taules de les quals puguis realitzar les següents consultes:


###### ** Exercici 1

# Realitza una subconsulta que mostri tots els usuaris amb més de 30 transaccions utilitzant almenys 2 taules.

SELECT 
    u.id AS id,
    u.name AS name,
    u.surname AS surname,
    u.country AS country,
    COUNT(user_id) AS transactions
FROM
    transactions t
        LEFT JOIN
    user_data u ON t.user_id = u.id
GROUP BY id , name , surname , country
HAVING transactions > 30
ORDER BY transactions DESC;

###### Exercici 2
# Mostra la mitjana d'amount per IBAN de les targetes de crèdit a la companyia Donec Ltd, utilitza almenys 2 taules.

select 
    c.company_id,
    c.company_name,
    cc.iban,
    round(avg(amount), 2) as avg_amount
from transactions t
join companies c on c.company_id = t.business_id
join credit_cards cc on cc.id = t.card_id
where c.company_name = 'Donec Ltd'
group by c.company_id, iban, c.company_name;


########### Nivell 2 ###########
# Crea una nova taula que reflecteixi l'estat de les targetes de crèdit basat en si les últimes
# tres transaccions van ser declinades i genera la següent consulta:

CREATE TABLE active_cards AS SELECT card_id, timestamp, declined FROM
    transactions;
    
    
###### Exercici 1
# Quantes targetes estan actives?**

SELECT * FROM active_cards;

with tarjetas_activas as(
	select card_id, declined,
	row_number () over (partition by card_id order by timestamp desc) as partition_time
	from active_cards)

Select card_id as tarjeta,
(case 
	when sum(declined) < 3 then 'Activa'
	else 'Inactiva'
end) as status
from tarjetas_activas
group by card_id
having status = 'activa';

########### Nivell 3 ###########
# Crea una taula amb la qual puguem unir les dades del nou arxiu products.csv amb la base de dades creada,
# tenint en compte que des de transaction tens product_ids. Genera la següent consulta:

-- ANTES DE LA CONSULTA TENEMOS QUE SEPARAR LOS IDS DE PRODUCTOS, HACER COMO UNS "CONCATENACION INVERTIDA"
-- VAMOS A CREAR UNA TABLA TEMPORAL LLAMADA SPLIT_PRODUCTS PARA LOS RESULTADOS DEL PROCESO DE DIVISIÓN DE ID DE PRODUCTOS POR FILAS
-- "RECURSIVE" PERMITE QUE LA TABLA TEMPORAL SE LLAME A SÍ MISMA VARIAS VECES PARA PROCESAR PARTES DE LA COLUMNA REPETIDAMENTE
WITH RECURSIVE split_products AS (
    SELECT
        id,
        TRIM(SUBSTRING_INDEX(product_ids, ',', 1)) AS product_id,
-- ^ EXTRAE EL PRIMER VALOR DE LA LISTA DE IDS DE PRODUCTO. EN "71, 1, 19" EL VALOR EXTRAÍDO SERÁ 71. "TRIM" ELIMINA LOS ESPACIOS SOBRANTES.
        TRIM(SUBSTRING_INDEX(product_ids, ',', -1)) AS remaining_ids,
-- ^ EXTRAE EL RESTO DE LA LISTA DESPUÉS DE ELIMINAR EL PRIMER VALOR. DE "71, 1, 19" EL RESTO SERÁ 1, 19. "TRIM" NOVAMENTE.
        1 AS level
-- ^  "1 AS LEVEL" ES COMO UN «CONTADOR» PARA INDICAR EN QUÉ NIVEL ESTAMOS -EMPEZAMOS POR EL NIVEL 1
    FROM transactions t
    UNION ALL
-- ^ COMBINA EL PRIMER PASO ARRIBA CON LOS SIGUIENTES PASOS DE RECURSIÓN, ES DECIR: "DESPUÉS DE HACER EL PRIMER PASO,
-- ^ SIGUE LLAMÁNDOTE A TI MISMO PARA PROCESAR EL RESTO DE LA LISTA HASTA QUE NO HAYA MÁS COMAS".
    SELECT
        id,
        TRIM(SUBSTRING_INDEX(remaining_ids, ',', 1)) AS product_id,
        TRIM(SUBSTRING_INDEX(remaining_ids, ',', -1)) AS remaining_ids,
        level + 1
-- EL +1 ACTUALIZA EL CONTADOR DE LA ITERACCION PARA INDICAR QUE ESTAMOS A UN NIVEL MÁS
    FROM split_products
    WHERE remaining_ids LIKE '%,%'
-- UNA CONDICIÓN PARA QUE CONTINÚE LA RECURSIÓN QUE SÓLO SE PRODUCE MIENTRAS REMAINING_IDS AÚN CONTIENE UNA COMA
)


-- EL METODO ARRIBA NO HABIA CREADO UNA TABLA CON CLAVE COMPUESTA DE ID DE TRANSACCION CON PRODUCT ID.
--  CONSULTÉ ALEIX PARA EL METODO CON JSON PARA TENERLA.
with split_products as (
select id, productes.product_id
from transactions
join json_table (concat("[", product_ids, "]" ),
"$[*]" columns (product_id varchar(100) path "$")
 )  as productes)
select * from split_products;
SELECT product_id Product, count(product_id) 'Total sales'
FROM split_products
group by product_id;

create table trasac_products as(
select id, productes.product_id
from transactions
join json_table (concat("[", product_ids, "]" ),
"$[*]" columns (product_id varchar(100) path "$")
 )  as productes);
 
ALTER TABLE TRASAC_PRODUCTS
ADD PRIMARY KEY (ID, PRODUCT_ID);

ALTER TABLE trasac_products
ADD CONSTRAINT fk_trasac_products_id FOREIGN KEY (id)
REFERENCES transactions(id);


ALTER TABLE trasac_products
ADD CONSTRAINT fk_trasac_prod_id FOREIGN KEY (product_id)
REFERENCES products(id);

###### **Exercici 1**
# Necessitem conèixer el nombre de vegades que s'ha venut cada producte.

SELECT product_id Product, count(product_id) 'Total sales'
FROM split_products
group by product_id;