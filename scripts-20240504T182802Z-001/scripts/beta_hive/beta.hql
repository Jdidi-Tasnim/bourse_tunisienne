CREATE TABLE stock_returns AS
SELECT 
    seance,
    code_entreprise,
    nom_entreprise,
    (cloture / LAG(cloture, 1) OVER (PARTITION BY nom_entreprise ORDER BY seance) - 1) AS return
FROM 
    cotation;

CREATE TABLE beta as
SELECT 
    nom_entreprise,
    REGR_SLOPE(stock_return, index_return) AS beta
FROM 
    combined_returns
GROUP BY 
     nom_entreprise;

CREATE TABLE beta AS 
SELECT 
    nom_entreprise,
    REGR_SLOPE(stock_return, index_return) AS beta
FROM 
    combined_returns
GROUP BY 
    nom_entreprise;
