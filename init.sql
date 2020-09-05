CREATE DATABASE IF NOT EXISTS mintopkn;
USE mintopkn;

DROP TABLE IF EXISTS MovieIncome;

CREATE TABLE IF NOT EXISTS MovieIncome (
  id serial NOT NULL PRIMARY KEY,
  title varchar(100),
  income double,
  modified TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO MovieIncome (id, title, income) VALUES (100, "Spiderman", 0);
INSERT INTO MovieIncome (id, title, income) VALUES (120, "Pirates of The Caribbean", 0);
INSERT INTO MovieIncome (id, title, income) VALUES (128, "The Big Lebowski", 0);
INSERT INTO MovieIncome (id, title, income) VALUES (140, "La Grande Bellezza", 0);
INSERT INTO MovieIncome (id, title, income) VALUES (294, "Die Hard", 0);
INSERT INTO MovieIncome (id, title, income) VALUES (354, "Tree of Life", 0);
INSERT INTO MovieIncome (id, title, income) VALUES (782, "A Walk in the Clouds", 0);
