# corona-final-project

## SHAHRVAND chain stores, aggregate sell information and analyze the data!

### Prerequired Packages
for running the program, you need to install this packages : 
1. $ **'sudo apt-get install libpq-dev'**
2. $ **'sudo apt install python3-dev libpq-dev'**
3. $ **'pip3 install psycopg2'**
4. $ **'sudo nano /etc/postgresql/11/main/pg_hba.conf'**

   find the following part of file:
   
            # Database administrative login by Unix domain socket
            local   all             postgres                                peer

   change **'peer'** to **'trust'** and save the file (press : ctrl+x then press y)

   reload the server to apply the changes : $ **'sudo service postgresql restart'**


### Run the Project
to use the project; follow the below steps:

1. clone the project
2. open a terminal and cd into **'corona-final-project'** directory
3. type following command : **'chmod +x getter.sh'**
4. and finally type this command to start the project : **'./getter.sh'**
5. open new terminal and run the python program to get the analyzing reports

