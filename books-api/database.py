from sqlalchemy import create_engine, Column, String, Integer, ForeignKey, select
import pymysql
from sqlalchemy.orm import registry, relationship, Session
import strawberry
from strawberry.fastapi import GraphQLRouter
from typing import Optional, List
# Replace with your MySQL credentials
username = 'root'
password = 'Cloud03%40pin'
host = 'localhost'  # or your database host
port = 3306         # default MySQL port
database_name = 'books'

# Create an engine and connect to MySQL
engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}:{port}/{database_name}',echo=True)
#engine = pymysql.connect(host=host, user=username, passwd='Cloud03@pin', database=database_name)

mapper_registry = registry()

Base = mapper_registry.generate_base()

class Author(Base):
    __tablename__ = 'authors'
    author_id = Column(Integer, primary_key=True)
    first_name = Column(String(length = 50))
    last_name = Column(String(length = 50))

    def __repr__(self):
        return f"<Author(author_id='{self.author_id}', first_name='{self.first_name}', last_name='{self.last_name}')>"
    
class Book(Base):
    __tablename__ = 'books'
    book_id = Column(Integer, primary_key=True)
    title = Column(String(length=255), nullable=False)
    number_of_pages = Column(Integer, nullable=False)

    def __repr__(self):
        return "<Book(book_id='{0}', title='{1}', number_of_pages='{2}')>".format(self.book_id, self.title, self.number_of_pages)
    
#create BookAuthor ORM
class BookAuthor(Base):
    __tablename__ = 'bookauthors'
    bookauthor_id = Column(Integer, primary_key=True)
    book_id = Column(Integer, ForeignKey('books.book_id'), nullable=False)
    author_id = Column(Integer, ForeignKey('authors.author_id'), nullable=False)

    book = relationship("Book", backref="book_authors")
    author = relationship("Author", backref="book_authors")

    def __repr__(self):
        return "<BookAuthor(bookauthor_id='{0}', book_id='{1}', author_id='{2}')>".format(
            self.bookauthor_id, self.book_id, self.author_id
        )
Base.metadata.create_all(engine)

def add_book(book: Book, author: Author):
    with Session(engine) as session:
        existing_book = session.execute(select(Book).filter(Book.title==book.title, Book.number_of_pages==book.number_of_pages)).scalar()
        if existing_book is not None:
            print("Book already exists, not adding to db")
            session.flush()
            paring = BookAuthor(author_id = author.author_id, book_id = existing_book.book_id)
            return
        else:
            print("book does not existm Adding book")
            session.add(book)
            session.flush()

        existing_author = session.execute(select(Author).filter(Author.first_name==author.first_name, Author.last_name==author.last_name)).scalar()
        if existing_author is not None:
            print("Author is already added to db, not adding")
            session.flush()
            paring = Author(author_id = existing_author.author_id, book_id = Book.book_id)
        else:
            print("Author does not exist, adding to db")
            session.add(author)
            session.flush()
        # code to add BookAuthor
        # Ensure both book and author exist in the database before creating the relationship
        book_id = existing_book.book_id if existing_book is not None else book.book_id
        author_id = existing_author.author_id if existing_author is not None else author.author_id

        # Check if the BookAuthor relationship already exists
        existing_pair = session.execute(
            select(BookAuthor).filter(BookAuthor.book_id == book_id, BookAuthor.author_id == author_id)
        ).scalar()
        if existing_pair is not None:
            print("BookAuthor relationship already exists, not adding.")
        else:
            print("Adding BookAuthor relationship.")
            pairing = BookAuthor(book_id=book_id, author_id=author_id)
            session.add(pairing)
            session.commit()

def get_book_author(bk_id:int):
    with Session(engine) as session:
        existing_book = session.execute(select(Book).filter(Book.book_id==bk_id)).scalar()
        if existing_book is not None:
            print("Book found !!, returning to api")
            session.flush()
            bookauthor = session.execute(select(BookAuthor).filter(BookAuthor.book_id==bk_id)).scalar()
            if bookauthor is not None:
                author = session.execute(select(Author).filter(Author.author_id==bookauthor.author_id)).scalar()
                session.flush()
                return (author , existing_book)
            else:
                return (None, existing_book)
        else:
            print("book does not raising error")
            raise Exception("Book id is invalid")

# code to add strawberry types
@strawberry.type
class AuthorType:
    author_id: int
    first_name: str
    last_name: str

@strawberry.type
class BookType:
    book_id: int
    title: str
    number_of_pages: int

@strawberry.type
class BookAuthorType:
    bookauthor_id: int
    book_id: int
    author_id: int
    book: Optional[BookType] = None
    author: Optional[AuthorType] = None

@strawberry.type
class Query:
    @strawberry.field
    def book_by_id(self, book_id: int) -> Optional[BookType]:
        with Session(engine) as session:
            book = session.get(Book, book_id)
            if book:
                return BookType(
                    book_id=book.book_id,
                    title=book.title,
                    number_of_pages=book.number_of_pages
                )
            return None

    @strawberry.field
    def author_by_id(self, author_id: int) -> Optional[AuthorType]:
        with Session(engine) as session:
            author = session.get(Author, author_id)
            if author:
                return AuthorType(
                    author_id=author.author_id,
                    first_name=author.first_name,
                    last_name=author.last_name
                )
            return None

    @strawberry.field
    def book_author_by_book_id(self, book_id: int) -> Optional[BookAuthorType]:
        with Session(engine) as session:
            bookauthor = session.execute(
                select(BookAuthor).filter(BookAuthor.book_id == book_id)
            ).scalar()
            if bookauthor:
                book = session.get(Book, bookauthor.book_id)
                author = session.get(Author, bookauthor.author_id)
                return BookAuthorType(
                    bookauthor_id=bookauthor.bookauthor_id,
                    book_id=bookauthor.book_id,
                    author_id=bookauthor.author_id,
                    book=BookType(
                        book_id=book.book_id,
                        title=book.title,
                        number_of_pages=book.number_of_pages
                    ) if book else None,
                    author=AuthorType(
                        author_id=author.author_id,
                        first_name=author.first_name,
                        last_name=author.last_name
                    ) if author else None
                )
            return None
        
@strawberry.type
class Mutation:
    @strawberry.mutation
    def add_book_with_author(
        self,
        title: str,
        number_of_pages: int,
        first_name: str,
        last_name: str
    ) -> Optional[BookAuthorType]:
        with Session(engine) as session:
            # Check if book exists
            existing_book = session.execute(
                select(Book).filter(Book.title == title, Book.number_of_pages == number_of_pages)
            ).scalar()
            if existing_book is None:
                book = Book(title=title, number_of_pages=number_of_pages)
                session.add(book)
                session.flush()
            else:
                book = existing_book

            # Check if author exists
            existing_author = session.execute(
                select(Author).filter(Author.first_name == first_name, Author.last_name == last_name)
            ).scalar()
            if existing_author is None:
                author = Author(first_name=first_name, last_name=last_name)
                session.add(author)
                session.flush()
            else:
                author = existing_author

            # Check if BookAuthor relationship exists
            existing_pair = session.execute(
                select(BookAuthor).filter(BookAuthor.book_id == book.book_id, BookAuthor.author_id == author.author_id)
            ).scalar()
            if existing_pair is not None:
                return BookAuthorType(
                    bookauthor_id=existing_pair.bookauthor_id,
                    book_id=existing_pair.book_id,
                    author_id=existing_pair.author_id,
                    book=BookType(
                        book_id=book.book_id,
                        title=book.title,
                        number_of_pages=book.number_of_pages
                    ),
                    author=AuthorType(
                        author_id=author.author_id,
                        first_name=author.first_name,
                        last_name=author.last_name
                    )
                )
            # Create new BookAuthor relationship
            pairing = BookAuthor(book_id=book.book_id, author_id=author.author_id)
            session.add(pairing)
            session.commit()
            return BookAuthorType(
                bookauthor_id=pairing.bookauthor_id,
                book_id=pairing.book_id,
                author_id=pairing.author_id,
                book=BookType(
                    book_id=book.book_id,
                    title=book.title,
                    number_of_pages=book.number_of_pages
                ),
                author=AuthorType(
                    author_id=author.author_id,
                    first_name=author.first_name,
                    last_name=author.last_name
                )
            )

schema = strawberry.Schema(query=Query, mutation=Mutation)
            
graphql_app = GraphQLRouter(schema)


# Check if we have a valid engine object
# if engine:
#     print("Engine object created successfully.")
# else:
#     print("Failed to create engine object.")
# # Create the database
# with engine:
#    # Create the 'authors' table in the 'books' database
#    with engine.cursor() as cursor:
#         cursor.execute("""
#         CREATE TABLE IF NOT EXISTS authors (
#             author_id INT AUTO_INCREMENT PRIMARY KEY,
#             first_name VARCHAR(255) NOT NULL,
#             last_name VARCHAR(255) NOT NULL
#             )
#         """)
#         print("Table 'authors' created successfully.")

#     # Create table books with columns title and number_of_pages
#         cursor.execute("""
#         CREATE TABLE IF NOT EXISTS books (
#             book_id INT AUTO_INCREMENT PRIMARY KEY,
#             title VARCHAR(255) NOT NULL,
#             number_of_pages INT NOT NULL
#             )
#         """)
#         print("Table 'books' created successfully.")
    

#     # Create table bookauthors with reference columns book_id and author_id as foreign keys
#         cursor.execute("""
#             CREATE TABLE IF NOT EXISTS bookauthors (
#             bookauthor_id INT AUTO_INCREMENT PRIMARY KEY,
#             book_id INT NOT NULL,
#             author_id INT NOT NULL,
#             FOREIGN KEY (book_id) REFERENCES books(book_id),
#             FOREIGN KEY (author_id) REFERENCES authors(author_id)
#             )
#         """)
#         print("Table 'bookauthors' created successfully.")

