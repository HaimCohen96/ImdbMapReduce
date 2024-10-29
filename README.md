# ImdbMapReduce

<img src = "gitImages\erlangIcon.png" wdith = 50 height = 50>

In this project I implemented a map reduce system. The system includes three entities:

•	Master

•	Client

•	Server

Using Master entity who control on the queries, data and merging all together. All servers all located in different computers as well as the master and the client. 
All the clients available on the graphic user interfaces.

Instruction YouTube link - https://youtu.be/Z8F9l-jdFbo 

MapReduce is like a team effort for handling big chunks of data. When we are working a lot of information that needs sorting and summarizing. The Map phase is where the work begins: each team member takes a piece of the data, processes it, and turns it into small, manageable key-value pairs. Then, in the Reduce phase, all those pairs are gathered together, and the team collaborates to combine them, making sense of the information and producing a final result. This method not only speeds up the process by working in parallel but also ensures that if something goes wrong, the system can recover and keep going. It’s a smart way to tackle large-scale data challenges!

## Architecture
• Master: The master manages all interactions from clients and servers, handling each request with a dedicated process.

• Multi-Server: Each server receives an equal amount of the data and executes the Map-Reduce algorithm as needed, based on the client’s query.

• Multi-Client: Each client can submit queries to the master and receive targeted responses, displayed in the client’s GUI as graph.

![alt text](gitImages\architecture.png)

## Instructions:
Requirements - erlang OTP 24 using Linux machine.

•	Make sure servers and clients txt files are updated with the correct data. 

Example – Name of entity@IP

Make sure that the master is on the first line on clients.txt.

1. Compile all the files:
   
        erl

        c(actor_graph). c(clientGUI). c(csv_to_ets). c(dataToServers). c(mapReduce). c(mapReduce). c(master). c(movie_graph). c(parse_csv). c(server).

2. Run servers:

        erl -name <serverName@IP> -setcookie <cookieName> -run server start_link

a.	Make sure the serverName and its IP pairs are in the servers.txt file

b.	Choose your own cookieName – this cookie serve all the entities.

3. Run master:

        erl -name <masterName@IP> -setcookie <cookieName> -run master start_link

a.	Make sure the masterName and its IP is the first line in clients.txt file and there is only one.

b.	Make sure all the servers finish receives and process data.

c.	Make sure master finish to create ETS file.

4. Run clients:

        erl -name <masterName@IP> -setcookie <cookieName>
        clientGUI:start().
a.	Make sure the clientName and its IP pairs are in the clients.txt file start from second row.
