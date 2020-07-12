// YA MASOOMEH
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#define unsuccessfull_query_retry_times 5

FILE * log_file;


void do_exit(PGconn *conn, PGresult *res) {
    fprintf(log_file, "%s\n", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    exit(1);
}


void create_fpdb(PGconn *conn1)  {
    PGresult *res1;
    int i;
    for (i = 0; i < unsuccessfull_query_retry_times; i++) {
        res1 = PQexec(conn1, "CREATE DATABASE fpdb");
        if (PQresultStatus(res1) == PGRES_COMMAND_OK) {
            break;
        }
    }
    if (i == unsuccessfull_query_retry_times && PQresultStatus(res1) != PGRES_COMMAND_OK){
        do_exit(conn1, res1); 
    }
    
    PQclear(res1);
    PQfinish(conn1);
}


int main() {

    log_file = fopen("report.log", "w");
    PGconn *conn = PQconnectdb("user=postgres dbname=fpdb");
    if (PQstatus(conn) == CONNECTION_BAD && strstr(PQerrorMessage(conn), "not exist") != NULL) {
    /*  if fpdb does not exists, server will raise an error with this message : 'FATAL : database fpdb not exist'
        in this case, we close the connection, connect to server again (connect to default database), create fpdb database 
        and close the connection again!*/

        PQfinish(conn);
        PGconn *conn1 = PQconnectdb("user=postgres");
        if (PQstatus(conn1) == CONNECTION_BAD)   {
            fprintf(log_file, "Connection to database failed: %s\n", PQerrorMessage(conn1));
            PQfinish(conn1);
            exit(1);
        }

        create_fpdb(conn1);        
        PGconn *conn = PQconnectdb("user=postgres dbname=fpdb");
    } else if (PQstatus(conn) == CONNECTION_BAD)    {
        //unhandled error occured, print error message and close the connection

        fprintf(log_file, "Connection to database failed: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        exit(1);
    }
    fprintf(log_file, "connection to server was successfull :)\nnow you are connected to fpdb database!\n");

    /*successfully connected to fpdb database! create the tables with specified columns*/

    PGresult *res;
    int i;
    for (i = 0; i < unsuccessfull_query_retry_times; i++) {
        res = PQexec(conn, "CREATE TABLE IF NOT EXISTS fp_stores_data ( time int, province varchar(50), city " \
            "varchar(50), market_id int, product_id int, price int, quantity int, has_sold int, PRIMARY KEY(time, province, " \
            "city, market_id, product_id) )");
        if (PQresultStatus(res) == PGRES_COMMAND_OK) {
            fprintf(log_file, "fp_stores_data table created :)\n");
            break;
        }
    }
    if (i == unsuccessfull_query_retry_times && PQresultStatus(res) != PGRES_COMMAND_OK){
        do_exit(conn, res);
    }
    
    PQclear(res);
    PQfinish(conn);
    fprintf(log_file, "connection to server closed :(\n");
    return 0;
}
