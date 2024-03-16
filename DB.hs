-- Copyright (C) 2024 Serguey Zefirov
module DB
	( module SQL
	, module DB
	) where

import Control.Monad

import qualified Data.Map.Strict as Map
import qualified Data.Set as Set

import SQL

data DB = DB
	{ dbSetDir		:: String
	, dbFields		:: Map.Map String FieldInfo
	}
	deriving (Eq, Ord, Show)

data FieldInfo = FieldInfo
	{ fiTable		:: String
	, ftName		:: String
	, fiIndex		:: Int
	, fiType		:: Type
	}
	deriving (Eq, Ord, Show)

readDB :: String -> String -> IO DB
readDB ddlFN dataDir = do
	text <- readFile ddlFN
	let	(tables, err) = ddl text
	when (not $ null err) $ error $ "error parsing " ++ show ddlFN ++ ": " ++ err
	let	fieldsList = [ (f, (FieldInfo tn f i ty))
				| CreateTable tn fs <- tables, (i, (f, ty, _)) <- zip [0..] fs]
		insF k fi1 fi2 = error $ "field " ++ show k ++ " is in table " ++ show (fiTable fi1) ++ " and in table " ++ show (fiTable fi2)
		db = DB dataDir (Map.fromListWithKey insF fieldsList)
	return db
