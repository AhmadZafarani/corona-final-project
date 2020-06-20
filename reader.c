// YA SADEGH
#include <stdio.h>
#include <dirent.h>
#include <string.h>
#include <stdlib.h>

const char * directory = "/tmp/final_project/";

void insert_into_stores(char* line) {
}

int main(void) { 
	struct dirent *de;
	DIR *dr = opendir(directory);   // set a pointer to directory and open it
	if (dr == NULL) {
		printf("Could not open %s\n", directory); 
		return 0;
	}

	while ((de = readdir(dr)) != NULL && de->d_type != DT_DIR) {    // read all "files" in this directory, line-by-line
        char name[50];
        strcpy(name, directory);
        strcat(name, de->d_name);
        FILE* file = fopen(name, "r");
        if (file == NULL)
            printf("Opening file %s failed!\n", name);

        char line[256];
        while (fgets(line, sizeof(line), file)) {   //  step 1
            insert_into_stores(line);
        }

        fclose(file);

        // char command[50];       // delete the files after reading them
        // strcpy(command, "rm -f ");
        // strcat(command, name);
        // system(command);
    }

	closedir(dr);
	return 0;
}
