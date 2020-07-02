// YA SADEGH
#include <stdio.h>
#include <dirent.h>
#include <string.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <regex.h>


const char * directory = "/tmp/final_project/";


char *trim_white_space(char *str) {
    char *end;
    while((unsigned char)*str == ' ')
        str++;
    if(*str == 0)
        return str;
    end = str + strlen(str) - 1;
    while(end > str && ((unsigned char)*end) == ' ')
        end--;
    end[1] = '\0';
    return str;
}


void print_result(int return_value) {
    if (return_value == 0) {
        printf("Pattern found.\n");
    }   else if (return_value == _REG_NOMATCH) {
        printf("Pattern not found.\n");
    }   else {
        printf("An error occured.\n");
    }
}


int line_to_command(char *line, char *command) {
    strcpy(command, "INSERT INTO fp_stores_data (time, province, city, market_id, product_id, price, quantity, " \
        "has_sold) VALUES (");
    int data_index = 1;
    char * token = strtok(line, ",");

    while (token != NULL) {

        // if (data_index == 1) {
        //     strcat(command, "'");
        //     regex_t regex;
        //     int compile = regcomp(&regex, "^(?:(?:([01]?\\d|2[0-3]):)?([0-5]?\\d):)?([0-5]?\\d)$", 0);
        //     if (compile != 0)   {
        //         printf("regex compile error\n");
        //     }
        //     int return_value = regexec(&regex, token, 0, NULL, 0);
        //     print_result(return_value);
        //     // if (return_value != 0)  {
        //     //     return NULL;
        //     // }   else    {
        //     //     strcat(command, token);
        //     // } 
        //     strcat(command, "'");
        // }

    //     }   else if (data_index == 2 || data_index == 3) {
    //         strcat(command, "'");
    //         regex_t regex;
    //         regcomp(&regex, "^[A-Za-z]+$", 0);
    //         int return_value = regexec(&regex, token, 0, NULL, 0);
    //         print_result(return_value);
    //         strcat(command, "'");

    //     }   else {
    //         regex_t regex;
    //         regcomp(&regex, "^\\d+$", 0);
    //         int return_value = regexec(&regex, token, 0, NULL, 0);
    //         print_result(return_value);
    //     }
        
        if (data_index <= 3)    {
            strcat(command, "'");
        }
        token = trim_white_space(token);
        strcat(command, token);
        if (data_index <= 3)    {
            strcat(command, "'");
        }
        if (data_index < 8) {
            strcat(command, ", ");
        }   else if ( data_index > 8)   {
            return 0;
        }
        token = strtok(NULL, ",");
        data_index++;
    }
    strcat(command, ")");
    // printf("%s\n", command);
    return data_index == 9;
}


void execute_query(const char* query, PGconn *conn) {
    PGresult *res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "%s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        exit(1);
    }
    PQclear(res);  
}


DIR *open_directory(struct dirent *de)   {
	DIR *dr = opendir(directory);   // set a pointer to directory and open it
	if (dr == NULL) {
		printf("Could not open %s\n", directory); 
		exit(1);
	}
    printf("directory opened successfully!\n");
    return dr;
}


PGconn *connect_to_database()   {
    PGconn *conn = PQconnectdb("user=postgres dbname=fpdb");
    if (PQstatus(conn) == CONNECTION_BAD) {
        fprintf(stderr, "Connection to database failed: %s\n", PQerrorMessage(conn));
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

        char command[1000];
        int valid = line_to_command(line, command);
        if (valid)    {
            execute_query(command, conn);
        }
    }
    return is_text_file;
}


void delete_file(char *name)  {
    char command[50];       // delete the files after reading them
    strcpy(command, "rm -f ");
    strcat(command, name);
    system(command);
    printf("file %s deleted!\n", name);
}


int main(void) { 
	struct dirent *de;
    DIR *dr = open_directory(de);
    PGconn *conn = connect_to_database();

    while ((de = readdir(dr)) != NULL) {        // read all files in this directory, line-by-line, and store their data in database
        char name[50];
        strcpy(name, directory);
        strcat(name, de->d_name);
        FILE* file = fopen(name, "r");
        if (file == NULL) {
            printf("Opening file %s failed!\n", name);
            exit(1);
        }

        if (!read_file(file, conn))   {
            continue;
        }
        printf("file %s opened!\nall VALID data of %s file inserted in fp_stores_data table!\n", name, name);
        fclose(file);

        // delete_file(name);
    }

    execute_query("DROP TABLE IF EXISTS fp_city_aggregation", conn);        //  step 2: aggregations
    execute_query("CREATE TABLE fp_city_aggregation AS SELECT city, time, SUM(quantity) AS total_quantity, SUM(has_sold) AS " \
        "total_has_sold FROM fp_stores_data GROUP BY city, time ORDER BY city, time", conn);
    execute_query("DROP TABLE IF EXISTS fp_store_aggregation", conn);    
    execute_query("CREATE TABLE fp_store_aggregation AS SELECT market_id, SUM(has_sold) AS total_has_sold, SUM(has_sold * price)" \
        " AS total_price FROM fp_stores_data GROUP BY market_id", conn);

    PQfinish(conn); 
	closedir(dr);
	return 0;
}
