bookdata = load '/BX-Books.csv' using PigStorage (';') as (ISBN:chararray, Booktitle: chararray, BookAuthor: chararray, Year: chararray, Publisher: chararray, Image_URL_S: chararray, Image_URL_M: chararray, Image_URL_L: chararray);

bookrating = load '/BX-Book-Ratings.csv' using PigStorage (';') as  (User: chararray, rating_ISBN: chararray, BookRating: chararray);

bookdataResult = foreach bookdata generate  ISBN, Year;

bookratingResult = foreach bookrating generate  rating_ISBN, BookRating;

bookdataFilter = FILTER bookdataResult by (Year == '"2002"');

bookdataISBN = foreach bookdataFilter generate  ISBN;

bookdata_rating = join bookdataISBN by ISBN  , bookratingResult by rating_ISBN;

bookdataISBNRes = foreach bookdata_rating generate  ISBN, BookRating;

BookdataRatingGroup = group bookdataISBNRes by BookRating;

cnt = foreach BookdataRatingGroup  generate group , COUNT(bookdataISBNRes);

dump cnt ;

