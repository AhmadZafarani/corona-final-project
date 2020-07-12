// YA SADEGH
#include <stdio.h>
#include <dirent.h>
#include <string.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <regex.h>
#define unsuccessfull_query_retry_times 5

FILE* log_file;
const char * directory = "/tmp/final_project/";


int regex_validation(const char *reg, char *token)   {
    regex_t regex;
    regcomp(&regex, reg, REG_EXTENDED);
    int retval = regexec(&regex, token, 0, NULL, 0);
    return retval == 0;
}


int line_to_command(char *line, char *command) {
    strcpy(command, "INSERT INTO fp_stores_data (time, province, city, market_id, product_id, price, quantity, " \
        "has_sold) VALUES (");
    int data_index = 1;
    char * token = strtok(line, ",");

    while (token != NULL) {

        if (data_index == 1) {      //  time validation : must be 10 digits
            int l = strlen(token);
            if (l != 10)    {
                fprintf(log_file, "one%s\t%d\n", line, data_index);
                return 0;
            }
        }

        if (data_index == 2 || data_index == 3) {       //  province and city validation
            strcat(command, "'");
            if (regex_validation("^[چژئجحخهعغفقثصضشسیِبُلاتنمکگوپدؤَذرزطظآ‌ ]+$", token)) {
                strcat(command, token);
            } else  {
                fprintf(log_file, "two%s\t%d\n", line, data_index);
                return 0;
            }
            strcat(command, "'");
        }   else if (data_index == 1 || data_index > 3)  {      //  number validation
            if (regex_validation("^[[:digit:]]+$", token)) {
                strcat(command, token);
            } else  {
                fprintf(log_file, "three%s\t%d\n", line, data_index);
                return 0;
            }
        }
        
        if (data_index < 8) {
            strcat(command, ", ");
        }   else if ( data_index > 8)   {       //  error : more than 8 items
            fprintf(log_file, "four%s\t%d\n", line, data_index);
            return 0;
        }
        token = strtok(NULL, ",");
        data_index++;
    }
    strcat(command, ")");
    return data_index == 9;
}


void execute_query(const char* query, PGconn *conn) {
    PGresult *res;
    int i;
    for (i = 0; i < unsuccessfull_query_retry_times; i++) {
        res = PQexec(conn, query);
        if (PQresultStatus(res) == PGRES_COMMAND_OK) {
            break;
        }
    }
    if (i == unsuccessfull_query_retry_times && PQresultStatus(res) != PGRES_COMMAND_OK){
        fprintf(log_file, "Couldn't insert the data in database because: %s\n", PQerrorMessage(conn));
    }
    PQclear(res);  
}


DIR *open_directory(struct dirent *de)   {
	DIR *dr = opendir(directory);   // set a pointer to directory and open it
	if (dr == NULL) {
		fprintf(log_file, "Could not open %s\n", directory); 
		exit(1);
	}
    fprintf(log_file, "directory opened successfully!\n");
    return dr;
}


PGconn *connect_to_database()   {
    PGconn *conn = PQconnectdb("user=postgres dbname=fpdb");
    if (PQstatus(conn) == CONNECTION_BAD) {
        fprintf(log_file, "Connection to database failed: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        exit(1);
    }
    return conn;
}


int read_file(FILE *file, PGconn *conn) {
    int is_text_file = 0;       //  if the file isn't a text file, continue
    char line[1000];
    while (fgets(line, sizeof(line), file)) {   //  step 1
        is_text_file = 1;

        int l = strlen(line);
        line[l-1] = '\0';       //  remove last '\n' from the read line
        line[l-2] = '\0';       //  remove last '\r' from the read line

        char command[1000];
        int valid = line_to_command(line, command);
        
        if (valid)    {
            execute_query(command, conn);
        }
    }
    return is_text_file;
}


void aggregation(PGconn * conn)  {    //  step 2: aggregations
    execute_query("CREATE TABLE IF NOT EXISTS fp_city_aggregation AS SELECT city, time, SUM(quantity) AS total_quantity, " \
        "SUM(has_sold) AS total_has_sold FROM fp_stores_data GROUP BY city, time ORDER BY city, time", conn);
    fprintf(log_file, "fp_city_aggregation table created! :)\n");
    execute_query("CREATE TABLE IF NOT EXISTS fp_store_aggregation AS SELECT market_id, SUM(has_sold) AS total_has_sold, " \
        "SUM(has_sold * price) AS total_price FROM fp_stores_data GROUP BY market_id", conn);
    fprintf(log_file, "fp_store_aggregation table created! :)\n");
}   // todo 


void delete_file(char *name)  {
    char command[50];       // delete the files after reading them
    strcpy(command, "rm -f ");
    strcat(command, name);
    system(command);
    fprintf(log_file, "file %s deleted!\n", name);
}


int main(void) { 
	struct dirent *de;
    log_file = fopen("report.log", "a");
    DIR *dr = open_directory(de);
    PGconn *conn = connect_to_database();

    while ((de = readdir(dr)) != NULL) {        // read all files in this directory, line-by-line, and store their data in database
        char name[50];
        strcpy(name, directory);
        strcat(name, de->d_name);
        FILE* file = fopen(name, "r");
        if (file == NULL) {
            fprintf(log_file, "Opening file %s failed!\n", name);
        }

        if (!read_file(file, conn))   {
            continue;
        }
        fprintf(log_file, "file %s opened!\nall VALID data of %s file inserted in fp_stores_data table!\n", name, name);
        fclose(file);

        delete_file(name);
    }

    aggregation(conn);
    PQfinish(conn); 
	closedir(dr);
	return 0;
}
