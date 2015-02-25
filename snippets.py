import logging
import argparse
import sys
import psycopg2



# Set the log output file, and the log level
logging.basicConfig(filename="snippets.log", level=logging.DEBUG)

logging.debug("Connecting to PostgreSQL")
connection = psycopg2.connect("dbname='snippets' user='action' host='localhost'")
logging.debug("Database connection established.")

def put(name, snippet):
    """Store a snippet with an associated name."""
    logging.info("Storing snippet {!r}: {!r}".format(name, snippet))
    cursor = connection.cursor()
    try:
        command = "insert into snippets values (%s, %s)"
        cursor.execute(command, (name, snippet))
    except psycopg2.IntegrityError as e:
        connection.rollback()
        command = "update snippets set message=%s where keyword=%s"
        cursor.execute(command, (snippet, name))
    connection.commit()
    logging.debug("Snippet stored successfully.")
    return name, snippet


def get(name):   
    """Retrieve a snippet with an associated name."""
    logging.info("Retrieving snippet with name {!r}".format(name))
    cursor = connection.cursor()
    command = "select message from snippets where keyword= '{}'".format(name)
    cursor.execute(command)
    snippet = cursor.fetchone() 
    logging.debug("Snippet retreived successfully.")
    if not snippet:
        # No snippet was found with that name.
        print "Warning - {} has no snippet stored against it".format(name)
        return 'N/A'
    else:
        return snippet[0]


def main():
    """Main function"""
    logging.info("Constructing parser")
    parser = argparse.ArgumentParser(description="Store and retrieve snippets of text")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Subparser for the put command
    logging.debug("Constructing put subparser")

    put_parser = subparsers.add_parser("put", help="Store a snippet")
    put_parser.add_argument("name", help="The name of the snippet")
    put_parser.add_argument("snippet", help="The snippet text")

    # Subparser for the get command
    logging.debug("Constructing get subparser")

    get_parser = subparsers.add_parser("get", help="Retreive a snippet")
    get_parser.add_argument("name", help="The name of the snippet")
    
    arguments = parser.parse_args(sys.argv[1:])
    
    # Convert parsed arguments from Namespace to dictionary
    arguments = vars(arguments)
    command = arguments.pop("command")

    if command == "put":
        name, snippet = put(**arguments)
        print("Stored {!r} as {!r}".format(snippet, name))
    elif command == "get":
        snippet = get(**arguments)
        print("Retrieved snippet: {!r}".format(snippet))
        
        
    # Code without argument unpacking
    #put(name="list", snippet="A sequence of things - created using []")

    # Identical code which uses argument unpacking
    #arguments = {
    #    "name": "list",
    #    "snippet": "A sequence of things - created using []"
    #}
    #put(**arguments)
    

if __name__ == "__main__":
    main()