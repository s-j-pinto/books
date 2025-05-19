
import schemas, database
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# book: Book
# author:Author

app = FastAPI()
app.include_router(database.graphql_app, prefix="/graphql")

@app.get("/")
def get_root(path):
    return "You are inside books api"


@app.post("/book/")
def create_book(request: schemas.BookAuthorPayloads):
    database.add_book(convert_to_book_db_model(request.book), convert_to_author_db_model(request.author))
    
def convert_to_book_db_model(book: schemas.Book)->database.Book:
    return database.Book(title=book.title, number_of_pages=book.number_of_pages)

def convert_to_author_db_model(author: schemas.Author)->database.Author:
    return database.Author(first_name=author.first_name, last_name=author.last_name)


@app.get("/book/{book_id}")
def get_book_from_db(book_id:int)->schemas.BookAuthorPayloads:
    bapl = schemas.BookAuthorPayloads
    b = database.Book
    a = database.Author
    try:
        a , b = database.get_book_author(book_id)
        bapl.author = convert_author_from_db_model(a)
        bapl.book= convert_book_from_db_model(b)
        return bapl
    except Exception as e:
        print(e)
        raise HTTPException(status_code=404, detail=repr(e))


def convert_book_from_db_model(dbBook: database.Book)->schemas.Book:
    book = schemas.Book
    book.title = dbBook.title
    book.number_of_pages = dbBook.number_of_pages
    return book

def convert_author_from_db_model(dbAuthor: database.Author)->schemas.Author:
    author = schemas.Author
    author.first_name = dbAuthor.first_name
    author.last_name = dbAuthor.last_name
    return author

    #code to insert book and author records into database
    # if bookauthorpayload:
    #     book = bookauthorpayload.book
    #     author = bookauthorpayload.author
        
    #     # Create the 'authors' table in the 'books' database
    #     with database.engine.cursor() as cursor:
    #              # Create a new author record
    #             sql = "INSERT INTO `author` (`first_name`, `last_name`) VALUES (%s, %s)"
    #             cursor.execute(sql, (author.first_name, author.last_name))
    #             author_id = cursor.lastrowid  # Retrieve the ID of the inserted author
    #             print("'authors' record inserted created successfully.")

    #              # Create a new book record
    #             sql = "INSERT INTO `book` (`title`, `no_of_pages`) VALUES (%s, %s)"
    #             cursor.execute(sql, (book.title  , author.no_of_pages))
    #             book_id = cursor.lastrowid
    #             print("'book' record inserted created successfully.")

    #             #code to insert record into bookauthorpayload table
    #             sql = "INSERT INTO `bookauthorpayload` (`book_id`, `author_id`) VALUES (%s, %s)"
    #             cursor.execute(sql, (book_id, author_id))
    #             print("'bookauthorpayload' record inserted created successfully.")
    #             # connection is not autocommit by default. So you must commit to save
    #             # your changes.
    #             cursor.connection.commit()