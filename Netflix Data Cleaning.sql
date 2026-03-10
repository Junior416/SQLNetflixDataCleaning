-- The objective of this case is to clean Netflix retail data; a necessary steps before any analytics are done
-- I downloaded Netflix sample data from Kaggle.com. Here is the URL to the data: https://www.kaggle.com/datasets/ariyoomotade/netflix-data-cleaning-analysis-and-visualization
-- I uploaded the excel file on MySQL Workbench
-- Create a staging table by duplicating netflix1

CREATE TABLE NETFLIX_STAGING
LIKE netflix1;

-- Inserting Data from netflix1 to NETFLIX_STAGING
INSERT NETFLIX_STAGING
SELECT * FROM netflix1;

SELECT * FROM NETFLIX_STAGING;

-- STEP 1: LOOKING FOR DUPLICATES
-- I start by looking for duplicate values in fields that should be unique such as show_id, title
-- I use PARTITION to include a column that show the count of the unique values in the column in question
-- A partition value of 2 or more indicates that there are duplicate values

-- The first duplicate scan is to ensure that no rows of data are duplicated

SELECT * FROM NETFLIX_STAGING;

SELECT *, ROW_NUMBER()
OVER(
PARTITION BY show_id, `type`, title, director, country, date_added, release_year, rating, duration, listed_in) as DUPLICATE_ROW
FROM NETFLIX_STAGING;

-- In order to filter on the DUPLICATE_ROW column we added, I need to create a CTE

WITH DUPLICATE_CTE AS
(SELECT *, ROW_NUMBER()
OVER(
PARTITION BY show_id, `type`, title, director, country, date_added, release_year, rating, duration, listed_in) as DUPLICATE_ROW
FROM NETFLIX_STAGING)

SELECT * FROM DUPLICATE_CTE
WHERE DUPLICATE_ROW > 1;

-- Running the script above returned 0 rows, meaning there are no duplicate rows

-- Next, I run the same exercise as above, but check to see if columns that should have unique values have no duplicates
-- Based on the table, it looks like it show_id and title should not have duplicates

SELECT SHOW_ID, ROW_NUMBER () OVER ()
FROM NETFLIX_STAGING;

WITH DUPLICATE_SHOWID_CTE AS
(SELECT *, ROW_NUMBER()
OVER(
PARTITION BY show_id) as DUPLICATE_SHOWID_ROW
FROM NETFLIX_STAGING)

SELECT * FROM DUPLICATE_SHOWID_CTE
WHERE DUPLICATE_SHOWID_ROW > 1;

-- The above returned 0 rows, so we know there are no duplicate values in the Show_id

WITH DUPLICATE_TITLE_CTE AS
(SELECT *, ROW_NUMBER()
OVER(
PARTITION BY title) as DUPLICATE_TITLE_ROW
FROM NETFLIX_STAGING)

SELECT * FROM DUPLICATE_TITLE_CTE
WHERE DUPLICATE_TITLE_ROW > 1;

-- The above returned 0 rows, so we know there are no duplicate values in the title

-- Looking for duplicate rows and values is complete. We move onto standardizing columns

-- STEP 2: STANDARDIZING DATA
-- Standardizing data requires visual examination of the data. I pulled various columns that would contain signs of incorrectly formatted data or data that should be overwritten to match other data
-- I start with types to see if all kinds of types are unique. I sort the result to make it easier to read

SELECT DISTINCT `TYPE`
FROM netflix_staging
ORDER BY 1;

-- The only types are Movie and TV Show, which is what we'd expect
-- Next, I do titles

SELECT DISTINCT title
FROM netflix_staging
ORDER BY 1;

-- I noticed that some of the titles have special character in them, such as 'CharitÃ© at War'
-- I create a where clause to visually see these title

SELECT *
FROM netflix_staging
WHERE title REGEXP '[^ -~]';

-- about 10 title return with special characters in their title. 
-- This is a uploading formatting error. However, because it is a small amount of lines, I created a UPDATE + CASE script to replace these titles with the correct one
-- the script simply looks for the name with the special character and replaces it with the correct title

UPDATE netflix_staging
SET title = CASE title
    WHEN 'PokÃ©mon Master Journeys: The Series'
        THEN 'Pokémon Master Journeys: The Series'
    WHEN 'PokÃ©mon Journeys: The Series'
        THEN 'Pokémon Journeys: The Series'
    WHEN 'Naruto ShippÃ»den the Movie: Bonds'
        THEN 'Naruto Shippuden the Movie: Bonds'
    WHEN 'Naruto ShippÃ»den the Movie: The Will of Fire'
        THEN 'Naruto Shippuden the Movie: The Will of Fire'
    WHEN 'CharitÃ© at War'
        THEN 'Charité at War'
    WHEN 'Elite Short Stories: Nadia GuzmÃ¡n'
        THEN 'Elite Short Stories: Nadia Guzmán'
    WHEN 'Elite Short Stories: GuzmÃ¡n Caye Rebe'
        THEN 'Elite Short Stories: Guzmán Caye Rebe'
    WHEN 'El patrÃ³n, radiografÃ­a de un crimen'
        THEN 'El patrón, radiografía de un crimen'
    WHEN 'Ya no estoy aquÃ­: Una conversaciÃ³n entre Guillermo del Toro y Alfonso CuarÃ³n'
        THEN 'Ya no estoy aquí: Una conversación entre Guillermo del Toro y Alfonso Cuarón'
    WHEN 'DÃ©rÃ¨: An African Tale'
        THEN 'Dépôt : An African Tale'
    ELSE title
END
WHERE title REGEXP 'Ã|Å|Â|©|³|¡|´|ô';

-- This converted the majority of the titles. For the remaining, I copied the values from the Result grid into the Case clause and it updated the value correctly
-- In hindsight, I realized the best fix would be a format (utf) update

-- I performed another visual scan on country

SELECT DISTINCT country
FROM netflix_staging
ORDER BY 1;

-- it looks like the countries are recorded correctly, but there is a country labeled as ""
-- I'd like to know how many rows contain "" as it could effect the usefulness of the data

SELECT country, count(country) from netflix_staging group by country
order by 2 DESC;

select count(country) from netflix_staging; 

-- There are 20 out of 266 or 7.5%  rows have a country as "". But it does represent the 3rd highest amount of countries
-- Because of the latter, I believe it is best to not modify "". When presenting the results from analysis, any lack in the ability to
-- make management decisions could be the catalyst to advocate for better data capturing

-- Lastly, I did a visual inspection on the rating field

SELECT DISTINCT rating
FROM netflix_staging

-- The result grid showed that the ratings looked correct, with no "near" repeating values
ORDER BY 1;

-- At this point, I'm content with the visual scans and the data standardization. Next, I look for Null and Blank values

-- STEP 3: NULLS & BLANK VALUES
-- I will look at rows that are Null and are blank; ie ""
-- To see which columns have a nulls or blank, I used the following Count + Where script to see which columns contain the value NULL or ""
SELECT
SUM(CASE WHEN SHOW_ID IS NULL THEN 1 ELSE 0 END) AS SHOW_ID_NULL,
SUM(CASE WHEN 'TYPE' IS NULL THEN 1 ELSE 0 END) AS TYPE_NULL,
SUM(CASE WHEN TITLE IS NULL THEN 1 ELSE 0 END) AS TITLE_NULL,
SUM(CASE WHEN DIRECTOR IS NULL THEN 1 ELSE 0 END) AS DIRECTOR_NULL,
SUM(CASE WHEN COUNTRY IS NULL THEN 1 ELSE 0 END) AS COUNTRY_NULL,
SUM(CASE WHEN DATE_ADDED IS NULL THEN 1 ELSE 0 END) AS DATE_ADDED_NULL,
SUM(CASE WHEN RELEASE_YEAR IS NULL THEN 1 ELSE 0 END) AS RELEASE_YEAR_NULL,
SUM(CASE WHEN RATING IS NULL THEN 1 ELSE 0 END) AS RATING_NULL,
SUM(CASE WHEN DURATION IS NULL THEN 1 ELSE 0 END) AS DURATION_ID_NULL,
SUM(CASE WHEN LISTED_IN IS NULL THEN 1 ELSE 0 END) AS LISTED_IN_ID_NULL
FROM netflix_staging;
-- All columns = 0, therefore, no nulls
-- We search for blanks ie ""
SELECT
SUM(CASE WHEN SHOW_ID="" THEN 1 ELSE 0 END) AS SHOW_ID_NULL,
SUM(CASE WHEN 'TYPE'="" THEN 1 ELSE 0 END) AS TYPE_NULL,
SUM(CASE WHEN TITLE="" THEN 1 ELSE 0 END) AS TITLE_NULL,
SUM(CASE WHEN DIRECTOR="" THEN 1 ELSE 0 END) AS DIRECTOR_NULL,
SUM(CASE WHEN COUNTRY="" THEN 1 ELSE 0 END) AS COUNTRY_NULL,
SUM(CASE WHEN DATE_ADDED="" THEN 1 ELSE 0 END) AS DATE_ADDED_NULL,
SUM(CASE WHEN RELEASE_YEAR="" THEN 1 ELSE 0 END) AS RELEASE_YEAR_NULL,
SUM(CASE WHEN RATING="" THEN 1 ELSE 0 END) AS RATING_NULL,
SUM(CASE WHEN DURATION="" THEN 1 ELSE 0 END) AS DURATION_ID_NULL,
SUM(CASE WHEN LISTED_IN="" THEN 1 ELSE 0 END) AS LISTED_IN_ID_NULL
FROM NETFLIX_STAGING;

-- All columns = 0, therefore, no blanks
-- Because I've seen the value "Not Given" in the title column, I check to see if there are other columns that have this value
SELECT
SUM(CASE WHEN SHOW_ID="Not Given" THEN 1 ELSE 0 END) AS SHOW_ID_NULL,
SUM(CASE WHEN 'TYPE'="Not Given" THEN 1 ELSE 0 END) AS TYPE_NULL,
SUM(CASE WHEN TITLE="Not Given" THEN 1 ELSE 0 END) AS TITLE_NULL,
SUM(CASE WHEN DIRECTOR="Not Given" THEN 1 ELSE 0 END) AS DIRECTOR_NULL,
SUM(CASE WHEN COUNTRY="Not Given" THEN 1 ELSE 0 END) AS COUNTRY_NULL,
SUM(CASE WHEN DATE_ADDED="Not Given" THEN 1 ELSE 0 END) AS DATE_ADDED_NULL,
SUM(CASE WHEN RELEASE_YEAR="Not Given" THEN 1 ELSE 0 END) AS RELEASE_YEAR_NULL,
SUM(CASE WHEN RATING="Not Given" THEN 1 ELSE 0 END) AS RATING_NULL,
SUM(CASE WHEN DURATION="Not Given" THEN 1 ELSE 0 END) AS DURATION_ID_NULL,
SUM(CASE WHEN LISTED_IN="Not Given" THEN 1 ELSE 0 END) AS LISTED_IN_ID_NULL
FROM NETFLIX_STAGING;

-- I discover that Directory has 125 rows where the director is "Not Given". It would appear that using the Director column in any analytics would not be beneficial due to the lack of complete data
-- Aside from the Country Column having 20 "Not Given" values, all other rows have no "Not Given" values

-- The cleaning process is completed! Scans for duplicate rows and duplicate fields, scans for irregular field formats, and scans for blank or missing fields is complete. We can use this data for analytics
select * from netflix_staging;
