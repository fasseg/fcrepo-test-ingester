run this test tool by building it and ssueing a commandline like:

$ mvn package
$ java -jar target/test-ingester-0.0.1-SNAPSHOT.jar http://localhost:8080/fedora/ fedoraAdmin fed 1000 500000

                                                                  |                    |       |    |    |

                                                                  h                    u       p    n    s

h = the foedora host to write to
u = the fedora username
p = the fedora password
n = the number of datastreams to create
s = the size of the created datastreams

