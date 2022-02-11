import sqlite3 as lite
import csv
import re
import pandas as pd
import argparse
import collections
import json
import glob
import math
import os
import requests
import string
import sqlite3
import sys
import time
import xml


class Movie_db(object):
    def __init__(self, db_name):
        self.con = lite.connect(db_name)
        self.cur = self.con.cursor()
    
    #q0 is an example 
    def q0(self):
        query = '''SELECT COUNT(*) FROM Actors'''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q1(self):
        # Set up views for query
        query = '''
            DROP VIEW IF EXISTS movie_actors_80s;
            CREATE VIEW IF NOT EXISTS movie_actors_80s AS
            SELECT aid
            FROM Cast as c JOIN Movies AS m on c.mid = m.mid
            WHERE year >= 1980 AND year <= 1990;
            DROP VIEW IF EXISTS movie_actors_20s;
            CREATE VIEW IF NOT EXISTS movie_actors_20s AS
            SELECT aid
            FROM Cast as c JOIN Movies AS m ON c.mid = m.mid
            WHERE year >= 2000;
        '''
        # Execute query
        self.cur.executescript(query)
        query = '''
            SELECT fname, lname FROM Actors
            WHERE aid IN movie_actors_80s AND aid IN movie_actors_20s
            ORDER BY lname ASC, fname ASC
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows
        

    def q2(self):
        query = '''
            SELECT title, year 
            FROM Movies
            WHERE year = 
                (SELECT year FROM Movies
                WHERE title = 'Rogue One: A Star Wars Story')
            AND rank > 
                (SELECT rank FROM Movies 
                WHERE title = 'Rogue One: A Star Wars Story')
            ORDER BY title ASC
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q3(self):
        query = '''
            DROP VIEW IF EXISTS star_wars_actors;
            CREATE VIEW IF NOT EXISTS star_wars_actors AS
            SELECT *
            FROM Cast AS c JOIN Movies AS m
            ON c.mid = m.mid
            WHERE title LIKE "%Star Wars%"
            GROUP BY aid, title;
            SELECT * FROM star_wars_actors;
        '''
        self.cur.executescript(query)
        query = '''
            SELECT fname, lname, COUNT(*)
            FROM Actors AS a JOIN star_wars_actors as s
            ON a.aid = s.aid
            GROUP BY a.aid
            ORDER BY COUNT(*) DESC, lname ASC, fname ASC

        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows


    def q4(self):
        # Set up views for query
        query = '''
            DROP VIEW IF EXISTS movie_actors_pre_80s;
            CREATE VIEW IF NOT EXISTS movie_actors_pre_80s AS
            SELECT aid
            FROM Cast as c JOIN Movies AS m on c.mid = m.mid
            WHERE year < 1980;
            DROP VIEW IF EXISTS movie_actors_post_80s;
            CREATE VIEW IF NOT EXISTS movie_actors_post_80s AS
            SELECT aid
            FROM Cast as c JOIN Movies AS m ON c.mid = m.mid
            WHERE year >= 1980;
        '''
        # Execute query
        self.cur.executescript(query)
        query = '''
            SELECT fname, lname FROM Actors
            WHERE aid IN movie_actors_pre_80s 
            AND aid NOT IN movie_actors_post_80s
            ORDER BY lname ASC, fname ASC
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q5(self):
        query = '''
            SELECT fname, lname, COUNT(*)
            FROM Movie_Director AS md 
            JOIN Directors AS d
            ON md.did = d.did
            GROUP BY fname, lname
            ORDER BY COUNT(*) DESC, lname ASC, fname ASC
            LIMIT 10
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q6(self):
        query = '''
            SELECT title, COUNT(*)
            FROM Cast AS c JOIN Movies AS m
            ON c.mid = m.mid
            GROUP BY title
            HAVING COUNT(*) IN
                (SELECT COUNT(*) AS members
                FROM Cast AS c JOIN Movies AS m
                ON c.mid = m.mid
                GROUP BY m.mid
                HAVING members > 0
                ORDER BY members DESC
                LIMIT 10)
            ORDER BY COUNT(*) DESC
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows
              
    def q7(self):
        query = '''
            DROP VIEW IF EXISTS female_actors;
            CREATE VIEW IF NOT EXISTS female_actors AS
            SELECT m.mid, COUNT(*) AS f_actors
            FROM Actors AS a JOIN Cast AS c
            ON a.aid = c.aid
            JOIN Movies AS m
            ON m.mid = c.mid
            GROUP BY title, gender
            HAVING gender = "Female";
            DROP VIEW IF EXISTS male_actors;
            CREATE VIEW IF NOT EXISTS male_actors AS
            SELECT m.mid, COUNT(*) AS m_actors
            FROM Actors AS a JOIN Cast AS c
            ON a.aid = c.aid
            JOIN Movies AS m
            ON m.mid = c.mid
            GROUP BY title, gender
            HAVING gender = "Male";
        '''
        self.cur.executescript(query)
        query = '''
            SELECT title, f_actors, m_actors FROM Movies
            NATURAL JOIN female_actors
            NATURAL JOIN male_actors
            WHERE f_actors > m_actors
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q8(self):
        query = '''
            DROP VIEW IF EXISTS actor_directors;
            CREATE VIEW actor_directors AS
            SELECT DISTINCT a.fname AS actor_fname, a.lname AS actor_lname, d.fname AS director_fname, d.lname AS director_lname
            FROM Actors AS a JOIN Cast AS c
            ON a.aid = c.aid 
            JOIN Movies AS m
            ON m.mid = c.mid
            JOIN Movie_Director AS md
            ON m.mid = md.mid
            JOIN Directors AS d
            ON d.did = md.did
            WHERE d.fname != a.fname AND d.lname != a.lname
        '''
        self.cur.executescript(query)
        query = '''
            SELECT actor_fname, actor_lname, COUNT(*)
            FROM actor_directors
            GROUP BY actor_fname, actor_lname
            HAVING COUNT(*) >= 7
            ORDER BY COUNT(*) DESC
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows


    def q9(self):
        query = '''
            DROP VIEW IF EXISTS debut_year;
            CREATE VIEW IF NOT EXISTS debut_year AS
            SELECT a.aid, MIN(year) AS debut FROM Actors AS a
            JOIN Cast AS c ON a.aid = c.aid
            JOIN Movies AS m ON m.mid = c.mid
            GROUP BY a.aid;
        '''
        self.cur.executescript(query)
        query = '''
            SELECT fname, lname, COUNT(*)
            FROM Actors AS a JOIN Cast AS c
            ON a.aid = c.aid
            JOIN Movies AS m
            ON m.mid = c.mid
            JOIN debut_year AS dy
            ON a.aid = dy.aid AND m.year = debut
            WHERE fname LIKE "D%"
            GROUP BY fname, lname
            ORDER BY COUNT(*) DESC, fname ASC, lname ASC;
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q10(self):
        query = '''
            SELECT a.lname, m.title
            FROM Actors AS a JOIN Cast AS c
            ON a.aid = c.aid
            JOIN Movie_Director AS md
            ON md.mid = c.mid
            JOIN Directors AS d
            ON d.did = md.did
            JOIN Movies AS m
            ON m.mid = c.mid
            WHERE a.lname = d.lname AND a.fname != d.fname
            ORDER BY a.lname ASC, title ASC
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q11(self):
        query = '''
            DROP VIEW IF EXISTS kevin_bacon_movies;
            CREATE VIEW IF NOT EXISTS kevin_bacon_movies AS
            SELECT m.mid
            FROM Actors AS a JOIN Cast AS c
            ON a.aid = c.aid
            JOIN Movies AS m 
            ON m.mid = c.mid
            WHERE fname || lname = "KevinBacon";

            DROP VIEW IF EXISTS one_degree_bacon;
            CREATE VIEW IF NOT EXISTS one_degree_bacon AS
            SELECT a.aid
            FROM Actors AS a JOIN Cast AS c
            ON a.aid = c.aid
            JOIN Movies AS m 
            ON m.mid = c.mid
            WHERE m.mid IN kevin_bacon_movies
            AND fname || lname != "KevinBacon";
            
            DROP VIEW IF EXISTS one_degree_bacon_movies;
            CREATE VIEW IF NOT EXISTS one_degree_bacon_movies AS
            SELECT m.mid
            FROM Cast AS c
            JOIN Movies AS m
            ON c.mid = m.mid
            JOIN Actors AS a
            ON a.aid = c.aid
            WHERE a.aid IN one_degree_bacon;

        '''
        self.cur.executescript(query)
        query = '''
            SELECT DISTINCT fname, lname FROM Actors AS a
            JOIN Cast AS c
            ON a.aid = c.aid
            JOIN Movies AS m
            ON c.mid = m.mid
            WHERE m.mid IN one_degree_bacon_movies
            AND a.aid NOT IN one_degree_bacon
            AND m.mid NOT IN kevin_bacon_movies
            ORDER BY lname ASC, fname ASC;
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q12(self):
        query = '''
            SELECT fname, lname, COUNT(*), AVG(rank) AS popularity
            FROM Actors AS a JOIN Cast AS c
            ON a.aid = c.aid
            JOIN Movies AS m
            ON m.mid = c.mid
            GROUP BY a.aid
            ORDER BY popularity DESC
            LIMIT 20
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

if __name__ == "__main__":
    task = Movie_auto("cs1656-public.db")
    rows = task.q0()
    print(rows)
    print()
    rows = task.q1()
    print(rows)
    print()
    rows = task.q2()
    print(rows)
    print()
    rows = task.q3()
    print(rows)
    print()
    rows = task.q4()
    print(rows)
    print()
    rows = task.q5()
    print(rows)
    print()
    rows = task.q6()
    print(rows)
    print()
    rows = task.q7()
    print(rows)
    print()
    rows = task.q8()
    print(rows)
    print()
    rows = task.q9()
    print(rows)
    print()
    rows = task.q10()
    print(rows)
    print()
    rows = task.q11()
    print(rows)
    print()
    rows = task.q12()
    print(rows)
    print()
