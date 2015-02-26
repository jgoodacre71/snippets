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
    
    with connection, connection.cursor() as cursor:
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
    with connection, connection.cursor() as cursor:
        cursor.execute("select message from snippets where keyword=%s", (name,))
        snippet = cursor.fetchone()
    
    logging.debug("Snippet retreived successfully.")
    if not snippet:
        # No snippet was found with that name.
        print "Warning - {} has no snippet stored against it".format(name)
        return 'N/A'
    else:
        return snippet[0]
    
def catalog():   
    """Retrieve available keywords"""
    logging.info("Retrieving catalog")
    with connection, connection.cursor() as cursor:
        cursor.execute("select keyword from snippets order by keyword")
        cat_keywords = cursor.fetchall()
    
    logging.debug("catalog retreived successfully.")
    return cat_keywords

def search(fragment):   
    """Retrieve snippet containing the fragment."""
    logging.info("Retrieving snippet with fragment {!r}".format(fragment))
    print "select message from snippets where keyword like {!r}".format('%'+fragment+'%')
    with connection, connection.cursor() as cursor:
        cursor.execute("select message from snippets where message like {!r}".format('%'+fragment+'%'))
        snippet = cursor.fetchall()
    
    logging.debug("Snippets retreived successfully.")
    if not snippet:
        # No snippet was found with that name.
        print "Warning no snippets found"
        return 'N/A'
    else:
        return snippet

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
    
    # Subparser for the catalog command
    logging.debug("Constructing catalog subparser")
    get_parser = subparsers.add_parser("catalog", help="Return available keywords")
    
    # Subparser for the search command
    logging.debug("Constructing search subparser")

    get_parser = subparsers.add_parser("search", help="Retreive snippets with a fragment")
    get_parser.add_argument("fragment", help="The fragment")
    
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
    elif command == "catalog":
        cat_items = catalog()
        for item in cat_items:
            print("Catalog item: {!r}".format(item[0]))
    elif command == "search":
        snippets = search(**arguments)
        for item in snippets:
            print("retreived snippet: {!r}".format(item[0]))
        
        
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