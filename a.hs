-- Copyright (C) 2024 Serguey Zefirov
module Main where

import System.Environment

import QU

query96 db = do
	-- USE store_sales
	rs <- use db "store_sales" []
	-- SET RELATION TO household_demographics (ss_hdemo_sk = hd_demo_sk) CHECK (hd_dep_count = 5)
	f1 <- field db "ss_hdemo_sk"
	f2 <- field db "hd_demo_sk"
	f3 <- field db "hd_dep_count"
	rs <- relationTo db rs "household_demographics" [f1 .=. f2] [f3 .=. ConstInt 5]
	-- SET RELATION TO time_dim (ss_sold_time_sk = t_time_sk) CHECK (t_hour = 8) CHECK (t_minute >= 30)
	f1 <- field db "ss_sold_time_sk"
	f2 <- field db "t_time_sk"
	f3 <- field db "t_hour"
	f4 <- field db "t_minute"
	rs <- relationTo db rs "time_dim" [f1 .=. f2] [f3 .=. ConstInt 8, f4 .>=. ConstInt 30]
	-- SET RELATION TO store (ss_store_sk = s_store_sk) CHECK (s_store_name = 'ese')
	f1 <- field db "ss_store_sk"
	f2 <- field db "s_store_sk"
	f3 <- field db "s_store_name"
	rs <- relationTo db rs "store" [f1 .=. f2] [f3 .=. ConstStr "ese"]
	print rs
	a <- scanAnalyze db rs
	print a
main = do
	db <- readDB "tpcds.sql" "set/"
	query96 db
t = main

