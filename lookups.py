from enum import Enum


class ErrorHandling(Enum):
    DB_CONNECT_ERROR = "DB Connect Error"
    DB_RETURN_QUERY_ERROR = "DB Return Query Error"
    API_ERROR = "Error calling API"
    RETURN_DATA_CSV_ERROR = "Error returning CSV"
    RETURN_DATA_EXCEL_ERROR = "Error returning Excel"
    RETURN_DATA_SQL_ERROR = "Error returning SQL"
    RETURN_DATA_UNDEFINED_ERROR = "Cannot find File type"
    EXECUTE_QUERY_ERROR = "Error executing the query"
    NO_ERROR = "No Errors"
    PREHOOK_SQL_ERROR = "Prehook: SQL Error"

class InputTypes(Enum):
    SQL = "SQL"
    CSV = "CSV"
    EXCEL = "Excel"
    
class PreHookSteps(Enum):
    EXECUTE_SQL_QUERY = "execute_sql_folder"

class SourceName(Enum):
    DVD_RENTAL = "dvd_rental"
    COLLEGE = "college"
    DATASET_MAIN="Dataset-main"

class SQLTablesToReplicate(Enum):
    RENTAL = "dvd_rental.rental"
    FILM = "dvd_rental.film"
    STUDENTS = "college.student"

