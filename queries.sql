-- 1) Which movie has the highest average rating?
SELECT m.movieId, m.title, AVG(r.rating) AS avg_rating, COUNT(*) AS rating_count
FROM ratings r
JOIN movies m ON m.movieId = r.movieId
GROUP BY r.movieId
ORDER BY avg_rating DESC, rating_count DESC
LIMIT 1;

-------------------------------------------------------------

-- 2) Top 5 movie genres that have the highest average rating
WITH movie_avg AS (
    SELECT movieId, AVG(rating) AS avg_rating
    FROM ratings
    GROUP BY movieId
)
SELECT g.name AS genre, AVG(ma.avg_rating) AS genre_avg_rating, COUNT(ma.movieId) AS movies_count
FROM movie_avg ma
JOIN movie_genres mg ON mg.movie_id = ma.movieId
JOIN genres g ON g.id = mg.genre_id
GROUP BY g.name
ORDER BY genre_avg_rating DESC
LIMIT 5;

-------------------------------------------------------------

-- 3) Who is the director with the most movies in this dataset?
SELECT director, COUNT(*) AS movie_count
FROM movies
WHERE director IS NOT NULL AND director != ''
GROUP BY director
ORDER BY movie_count DESC
LIMIT 1;

-------------------------------------------------------------

-- 4) What is the average rating of movies released each year?
SELECT m.year, ROUND(AVG(r.rating), 3) AS avg_rating, COUNT(DISTINCT m.movieId) AS movies_count
FROM movies m
JOIN ratings r ON r.movieId = m.movieId
WHERE m.year IS NOT NULL
GROUP BY m.year
ORDER BY m.year ASC;
