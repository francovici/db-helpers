import os
import sys

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

def execute_statement(statement, sql_driver):
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


