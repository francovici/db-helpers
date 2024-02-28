import os
import sys
import pyodbc

def setDBEnvironments():
    from dotenv import dotenv_values
    try:
        envs = dotenv_values(".env")

        driver_name = envs['MSSQL_SERV_DRIVER']
        server_name = envs['MSSQL_SERV_NAME']
        db_name = envs['MSSQL_DB_NAME']
        uid = envs['MSSQL_SERV_USR']
        pwd = envs['MSSQL_SERV_PWD']

        for key in envs.keys():
            if envs[key] == None:
                raise EnvironmentError
            
        return {"driver_name": driver_name,"server_name": server_name,"db_name": db_name,"uid": uid, "pwd": pwd}
    
    except Exception as ex:
        print('Hay variables de entorno no están definidas correctamente')
        print(ex)
        exit(1)

def connectToDatabase(dbconfig: dict):    
    try:
        print('Connecting to database')
        conn = pyodbc.connect("Driver={d}"
                        "Server={s};"
                        "Database={db};"
                        "uid={u};pwd={p}".format(d=dbconfig['driver_name'], s=dbconfig['server_name'], db=dbconfig['db_name'], u=dbconfig['uid'], p=dbconfig['pwd']))
        return conn
    except Exception as ex:
        print('Error al conectar base de datos')
        print(ex)
        exit(1)

cls = lambda: os.system('cls')

def log(msg, level):
    if logging_level >= level:
        print(msg)

def is_statement_incomplete(statement : str):
    if len(statement) > 2:
        log('Ultimo caracter:', 4)
        log(statement[len(statement) - 2], 4)
        if statement[len(statement) - 2] == ';' or statement[len(statement) - 1] == ';':
            return False
        else:
            return True
    else:
        return True

def execute_statement(statement, connection, commit = False):
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(statement)
        except Exception as ex:
            log('Error al ejecutar: ', 1)
            log(statement, 1)
            log(ex, 1)
            connection.rollback()
            exit(1)

        if(commit):
            connection.commit()
        else:
            connection.rollback()

def closeUp(connection, statements_executed, statements_commited):

    if connection:
        connection.close()

    print('\n')
    print('Sentencias leídas: ' + str(statements_executed))
    print('Commits: ' + str(statements_commited))
    print('\n')
    input('Finalizado. Presione ENTER para salir.')



def execute_batch_statements(statements_batch : str, connection):
    pass

def perform_inserts(sql_file, statements_to_process, connection = None, commit : bool = False):
    try:
        #Checking file size and input variables
        cls()
        print('Leyendo archivo... aguarde unos segundos')

        with open(sql_file, encoding='utf-8') as f:
            file_size_in_lines = sum(1 for _ in f)
        
        print('\n')
        print('Archivo: ' + sql_file)
        print('Cantidad de líneas en archivo: ' + str(file_size_in_lines))
        print('Límite de sentencias a procesar: ' + str(statements_to_process))
        print('Commit: ' + str(commit))
        print('Logging level: ' + str(logging_level))
        print('\n')

        action = input('Comenzar la ejecución? s/N')
        print('\n')
        
        with open(sql_file, 'r',encoding='utf-8') as file:
            previousLine = ''
            statements_executed = 0
            statements_commited = 0

            if action.upper() != 'S':
                input('Cancelando ejecución. Presione ENTER para salir.')
                exit(0)
            
            print('Progreso: ')
            lines_read = 0
            for line in file:
                if(statements_executed < statements_to_process):
                    if previousLine != '':
                        statement = previousLine + ' ' + line.strip()
                    else:
                        statement = line.strip()

                    if is_statement_incomplete(statement):
                        previousLine = statement
                        log(statement, 3)
                        log('incomplete statement, skipping...', 3)
                        pass
                    else:
                        log('Executing statement:', 3)
                        log(statement, 3)
                        execute_statement(statement, connection, commit)
                        previousLine = ''
                        statements_executed += 1
                        if(commit): 
                            statements_commited += 1
                lines_read += 1
                progress = round(lines_read / file_size_in_lines * 100,2)
                sys.stdout.write("\r%.2f%%" % progress)

        closeUp(connection, statements_executed, statements_commited)

    except Exception as ex:
        print('\n Error:')
        print(ex)
        closeUp(connection, statements_executed, statements_commited)
        exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        input('Falta especificar archivo de entrada.')
        sys.exit(1)

    input_sql_file = sys.argv[1]
    connection = None
    commit = False
    connect_to_db = input('Conectar a la Base de datos? s/N')

    if connect_to_db.upper() == 'S':
        connection = connectToDatabase(setDBEnvironments())
        commit_answer = input('Ejecutar commits en cada transacción? s/N')
        if commit_answer.upper() == 'S':
            commit = True

    max_statements = input('Cuántas sentencias quiere correr (como máximo)? (10):')
    if not max_statements.isnumeric():
        max_statements = 10
    else:
        max_statements = int(max_statements)
    
    global logging_level

    log_level_answer = input('Nivel de log ?\n (0=Nada, 1=Errores, 3=Sentencias, 4=Todo) (0):')
    if not log_level_answer.isnumeric():
        log_level_answer = 0

    logging_level = int(log_level_answer)

    if connection is not None:
        perform_inserts(input_sql_file, max_statements, connection, commit)
    else:
        perform_inserts(input_sql_file, max_statements)


