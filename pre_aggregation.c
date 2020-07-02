// YA MASOOMEH
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>


void do_exit(PGconn *conn, PGresult *res) {
    fprintf(stderr, "%s\n", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    exit(1);
}

int main() {

    PGconn *conn = PQconnectdb("user=postgres dbname=fpdb");
    if (PQstatus(conn) == CONNECTION_BAD && strstr(PQerrorMessage(conn), "not exist") != NULL) {
    /*  if fpdb does not exists, server will raise an error with this message : FATAL : database fpdb not exist
        in this case, we close the connection, connect to server again, create fpdb database and close the connection again!*/

        PQfinish(conn);
        PGconn *conn1 = PQconnectdb("user=postgres");
        PGresult *res1 = PQexec(conn1, "CREATE DATABASE fpdb");
        if (PQresultStatus(res1) != PGRES_COMMAND_OK) {
            do_exit(conn1, res1);     
        }
        PQclear(res1);
        PQfinish(conn1);
        PGconn *conn = PQconnectdb("user=postgres dbname=fpdb");
    } else if (PQstatus(conn) == CONNECTION_BAD)    {
        //unhandled error occured, print error message and close the connection

        fprintf(stderr, "Connection to database failed: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        exit(1);
    }
    printf("connection to server was successfull :)\nnow you are connected to fpdb database!\n");

    /*successfully connected to fpdb database! create the tables with specified columns*/
    
    PGresult *res = PQexec(conn, "DROP TABLE IF EXISTS fp_stores_data");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        do_exit(conn, res);     
    }
    PQclear(res);
    res = PQexec(conn, "CREATE TABLE fp_stores_data ( time varchar(50), province varchar(50), city varchar(50), " \
        "market_id int,product_id int, price int, quantity int, has_sold int)");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        do_exit(conn, res);     
    }   else    {
        printf("fp_stores_data table created :)\n");
    }
    PQclear(res);    


    // res = PQexec(conn, "DROP TABLE IF EXISTS fp_city_aggregation");
    // if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    //     do_exit(conn, res);     
    // }
    // PQclear(res);
    // res = PQexec(conn, "CREATE TABLE fp_city_aggregation ( city varchar(50), time varchar(50), total_quantity int, " \
    //     "total_has_sold int, price_avg float, PRIMARY KEY (city, time) )");
    // if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    //     do_exit(conn, res);     
    // }   else    {
    //     printf("fp_city_aggregation table created :)\n");
    // }
    // PQclear(res);


    // res = PQexec(conn, "DROP TABLE IF EXISTS fp_store_aggregation");
    // if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    //     do_exit(conn, res);     
    // }
    // PQclear(res);
    // res = PQexec(conn, "CREATE TABLE fp_store_aggregation ( market_id int, total_has_sold int, total_price int, " \
    //     "PRIMARY KEY (market_id) );");
    // if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    //     do_exit(conn, res);     
    // }   else    {
    //     printf("fp_store_aggregation table created :)\n");
    // }
    // PQclear(res);


    PQfinish(conn);
    printf("connection to server closed :(\n");
    return 0;
}
