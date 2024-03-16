-- Copyright (C) 2024 Serguey Zefirov
module SQL
	( module SQL
	) where

import Data.Char

import qualified Data.List as L

data DDL =
	CreateTable
		{ ctName		:: String
		, ctFields		:: [(String, Type, Constr)]
		}
	deriving (Eq, Ord, Show)

data Type = TyInt | TyString | TyTime | TyDate | TyDecimal Int
	deriving (Eq, Ord, Show)
data Constr = CNULL Bool
	deriving (Eq, Ord, Show)

ddl :: String -> ([DDL], String)
ddl text = loop [] ls
	where
		ltrim = dropWhile isSpace
		rtrim = reverse . ltrim . reverse
		trim = ltrim . rtrim
		dropComma cs = reverse $ case reverse cs of
			',' : cs -> cs
			cs -> cs
		ls = map (map (map toLower) . words . dropComma . trim) (lines text)
		loop :: [DDL] -> [[String]] -> ([DDL], String)
		loop acc [] = (reverse acc, "")
		loop acc (("--":_):ls) = loop acc ls
		loop acc ([]:ls) = loop acc ls
		loop acc (["create" , "table", tn] : ["("] : ls) = case err of
			Just msg -> (reverse acc, msg)
			Nothing -> loop (CreateTable tn fs : acc) ls'
			where
				(fs, err, ls') = fields [] ls
		fields acc ([");"] : ls) = (reverse acc, Nothing, ls)
		fields acc (("primary" : "key" : _) : ls) = fields acc ls
		fields acc ((fn : tys : cos) : ls)
			| Just ty <- readType tys
			, Just co <- readConstr cos
			= fields ((fn, ty, co) : acc) ls
		fields acc (s : ls) = (acc, Just ("error here: " ++ unwords s), ls)
		readConstr ["null"] = Just $ CNULL True
		readConstr ["not", "null"] = Just $ CNULL False
		readConstr [] = Just $ CNULL True
		readConstr _ = Nothing
		readType "integer" = Just TyInt
		readType "date" = Just TyDate
		readType "time" = Just TyTime
		readType s
			| L.isPrefixOf "varchar" s || L.isPrefixOf "char" s = Just TyString
		readType s
			| Just ps <- L.stripPrefix "decimal" s = case reads s :: [((Int,Int), String)] of
				((_, s), "") : _ -> Just $ TyDecimal s
				_ -> Just $ TyDecimal 0
		readType _ = Nothing

