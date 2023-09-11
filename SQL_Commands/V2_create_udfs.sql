-- get rental duration function
CREATE OR REPLACE FUNCTION dw_reporting.get_rental_duration(rental_date TIMESTAMP, return_date TIMESTAMP)
RETURNS INTEGER AS $$
BEGIN
    RETURN EXTRACT(AGE FROM (return_date - rental_date));
END;
$$LANGUAGE plpgsql;


-- get film category name
CREATE OR REPLACE FUNCTION dw_reporting.get_film_category(film_id INTEGER)
RETURNS TEXT AS $$
DECLARE 
    category_name TEXT;
BEGIN
    SELECT 
        categ.name INTO category_name
    FROM public.film_category film_category
    INNER JOIN public.category AS categ
    ON categ.category_id = film_category.category_id
    WHERE film_category.film_id = film_id
    LIMIT 1;
    RETURN category_name;
END;
$$ LANGUAGE plpgsql;

