import os
import sys
import pyodbc
from dotenv import dotenv_values

try:
    envs = dotenv_values(".env")

    driver_name = envs['MSSQL_SERV_DRIVER']
    server_name = envs['MSSQL_SERV_NAME']
    db_name = envs['MSSQL_DB_NAME']
    uid = envs['MSSQL_SERV_USR']
    pwd = envs['MSSQL_SERV_PWD']

    for key in envs.keys:
        if envs[key] == None:
            raise EnvironmentError

except Exception as ex:
    print('Hay variables de entorno no están definidas correctamente')
    print(ex)
    exit(1)
    
try:
    conn = pyodbc.connect("Driver={d}"
                      "Server={s};"
                      "Database={db};"
                      "uid={u};pwd={p}".format(d=driver_name, s=server_name, db=db_name, u=uid, p=pwd))
except Exception as ex:
    print('Error al conectar base de datos')
    exit(1)

cls = lambda: os.system('cls')
logging_level = 3 

def log(msg, level):
    if logging_level >= level:
        print(msg)

def is_statement_incomplete(statement : str):
    log('Ultimo caracter:', 4)
    log(statement[len(statement) - 2], 4)
    if statement[len(statement) - 2] == ';':
        return False
    else:
        return True

def execute_statement(statement, connection):
    pass

def execute_batch_statements(statements_batch : str, connection):
    pass

def perform_inserts(sql_file, statements_to_process, commit = False):
    try:
        #Checking file size and input variables
        cls()
        print('Leyendo archivo... aguarde unos segundos')

        with open(sql_file, encoding='utf-8') as f:
            file_size_in_lines = sum(1 for _ in f)
        
        print('\n')
        print('Archivo: ' + sql_file)
        print('Cantidad de líneas en archivo: ' + str(file_size_in_lines))
        print('Cantidad de sentencias a procesar: ' + str(statements_to_process))
        print('Commit: ' + str(commit))
        print('\n')

        action = input('Comenzar la ejecución? S/n')
        print('\n')
        
        with open(sql_file, 'r',encoding='utf-8') as file:
            previousLine = ''
            statements_executed = 0

            if action.upper() != 'S':
                input('Cancelando ejecución. Precione cualquier tecla para salir.')
                exit(1)

            for line in file:
                if(statements_executed < statements_to_process):
                    if previousLine != '':
                        statement = previousLine + ' ' + line
                    else:
                        statement = line

                    if is_statement_incomplete(statement):
                        previousLine = statement
                        log(statement, 3)
                        log('incomplete statement, skipping...', 3)
                        pass
                    else:
                        log('Executing statement:', 3)
                        transaction_action = 'COMMIT TRANSACTION;' if commit else 'ROLLBACK TRANSACTION;'
                        statement = 'USE [TucusMap];\n BEGIN TRANSACTION;\n {s} \n {t}'.format(s = statement, t = transaction_action)
                        log(statement, 3)
                        previousLine = ''
                        statements_executed += 1
    except Exception as ex:
        print(ex)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        input('Falta especificar archivo de entrada.')
        sys.exit(1)

    input_sql_file = sys.argv[1]
    max_statements = input('Cuántas statements quiere correr? (10):')
    if not max_statements.isnumeric():
        max_statements = 10
    else:
        max_statements = int(max_statements)
    perform_inserts(input_sql_file, max_statements)


