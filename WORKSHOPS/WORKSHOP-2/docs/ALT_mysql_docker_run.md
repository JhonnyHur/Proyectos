# Alternativa MySQL via docker run
docker run -d --name mysql-dw   -e MYSQL_ROOT_PASSWORD="etlpass"   -e MYSQL_DATABASE="dw"   -e MYSQL_USER="etluser"   -e MYSQL_PASSWORD="etlpass"   -p 3306:3306   -v mysql_data:/var/lib/mysql   mysql:8.0
