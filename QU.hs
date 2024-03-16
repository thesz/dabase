{-# LANGUAGE BangPatterns #-}

module QU
	( module QU
	, module DB
	) where

import Control.Monad

import qualified Data.ByteString as BS
import qualified Data.IntSet as IntSet
import qualified Data.List as List
import qualified Data.Map.Strict as Map

import qualified Data.Vector as V

import System.IO

import DB

numDiffValues :: Int
numDiffValues = 3000000 -- enough for query at hand, should be determined dynamically.

data Relation =
		USE { useTable	:: String, useCheck :: [Expr] }
	|	SETREL 
		{ setrTable		:: String
		, setrEqualities	:: [Rel]
		, setrCheck		:: [Expr]
		, setrBefore		:: Relation
		}
	deriving (Eq, Ord, Show)

data Rel = Rel Type FieldInfo FieldInfo
	deriving (Eq, Ord, Show)

data Expr =
		Null
	|	ConstInt	Int
	|	ConstStr	String
	|	Field	FieldInfo
	|	Compare	Cmp	Expr	Expr
	deriving (Eq, Ord, Show)

data Cmp = Eq | GE
	deriving (Eq, Ord, Show)

(.=.) :: Expr -> Expr -> Expr
a .=. b = Compare Eq a b
(.>=.) :: Expr -> Expr -> Expr
a .>=. b = Compare GE a b

field :: DB -> String -> IO Expr
field (DB _ fs) s
	| Just fi <- Map.lookup s fs = return $ Field fi
	| otherwise = error $ "can't find field " ++ show s

use :: DB -> String -> [Expr] -> IO Relation
use db table checks = do
	let	fs = Map.elems $ dbFields db
		tns = map fiTable fs
		hasTable = elem table tns
	when (not hasTable) $ error $ "unknown table " ++ show table
	return $ USE table checks

relationTo :: DB -> Relation -> String -> [Expr] -> [Expr] -> IO Relation
relationTo db prev table how checks = do
	return $ SETREL table rels checks prev
	where
		rels = toRels [] how
		toRels acc (e:es) = case e of
			Compare Eq (Field a) (Field b)
				| fiType a == fiType b
				  && fiTable a /= fiTable b
				  && (fiTable a == table || fiTable b == table) -> toRels (Rel (fiType a) a b : acc) es
			_ -> error "not a relation"
		toRels acc [] = reverse acc

data Analysis = Analysis
	deriving (Eq, Ord, Show)

scanAnalyze :: DB -> Relation -> IO Analysis
scanAnalyze db rels = do
	print tables
	let	huh _ _ = error $ "duplicate key?"
	(sets, counts) <- (\sc -> (Map.unionsWith huh $ map  fst sc, map snd sc)) <$>
			forM tables (\ t -> scan Map.empty t)
	forM_ (Map.toList sets) $ \(f, s) -> do
		putStrLn $ "  field " ++ show f ++ ": " ++ show (IntSet.size s)
	let	combine sets [] = sets
		combine sets (Rel _ a b : rs) = combine sets' rs
			where
				s x = Map.findWithDefault IntSet.empty x sets
				combined = IntSet.intersection (s a) (s b)
				sets' = Map.insert a combined $ Map.insert b combined sets
		combined = combine sets relEqs
	putStrLn $ "table rows' counts:"
	forM_ (zip tables counts) $ \(t, (rc, rp)) -> do
		putStrLn $ "    table " ++ show t ++ ": " ++ show rc ++ " rows read, " ++ show rp ++ " rows pass filters."
	putStrLn $ "after combination:"
	forM_ (Map.toList combined) $ \(f, s) -> do
		putStrLn $ "  field " ++ show f ++ ": " ++ show (IntSet.size s)
	(sets, counts) <- (\sc -> (Map.unionsWith huh $ map  fst sc, map snd sc)) <$>
			forM tables (\ t -> scan combined t)
	putStrLn $ "table rows' counts with sets filtering:"
	forM_ (zip tables counts) $ \(t, (rc, rp)) -> do
		putStrLn $ "    table " ++ show t ++ ": " ++ show rc ++ " rows read, " ++ show rp ++ " rows pass filters."
	return Analysis
	where
		relEqs = toRelEqs rels
		toRelEqs (USE _ _) = []
		toRelEqs (SETREL _ re _ rs) = re ++ toRelEqs rs
		tables = List.nub $ toTables rels
		toTables (USE t _) = [t]
		toTables (SETREL t _ _ rs) = t : toTables rs
		relFields = List.nub $ toRelFields rels
		expandRelFields (Rel _ a b) = [a, b]
		toRelFields (USE _ _) = []
		toRelFields (SETREL _ rfs _ rs) = concatMap expandRelFields rfs ++ toRelFields rs
		tableChecks t (USE t' chks)
			| t == t' = chks
			| otherwise = []
		tableChecks t (SETREL t' _ chks rs)
			| t == t' = chks
			| otherwise = tableChecks t rs
		fn table = dbSetDir db ++ "/" ++ table ++ ".dat"
		scan sets table = do
			putStrLn $ "scanning " ++ show table
			let	relfs = filter ((==table) . fiTable) relFields
				checks = tableChecks table rels
			h <- openBinaryFile (fn table) ReadMode
			scanLoop sets 0 0 (Map.fromList [(rf, IntSet.empty) | rf <- relfs]) h relfs checks
		eval fields e@(Field (FieldInfo _ _ i ty)) = do
			let	bs = fields V.! i
				s = map (toEnum . fromIntegral) $ BS.unpack bs :: String
			case ty of
				TyString -> return $ ConstStr s
				TyInt -> case reads s of
					(x,"") : _ -> return $ ConstInt x
					_
						| BS.length bs == 0 -> return Null
						| otherwise -> error $ "can't read " ++ show s ++ " for " ++ show e ++ ", i " ++ show i ++ ", bs " ++show bs ++", fields " ++ show fields
		eval fields x@(ConstInt _) = return x
		eval fields x@(ConstStr _) = return x
		eval fields x = error $ "eval " ++ show x
		interpret fields (Compare op a b) = do
			a <- eval fields a
			b <- eval fields b
			case op of
				Eq -> return $ a == b
				GE -> return $ a >= b
		validate fields [] = return True
		validate fields (e : es) = do
			--putStrLn $ "  validating " ++ show e
			x <- interpret fields e
			if x
				then validate fields es
				else return False
		hash (ConstInt i) = mod i numDiffValues
		hash Null = numDiffValues - 1
		scanLoop sets !rc !rp !digests h relFields checks = do
			eof <- hIsEOF h
			if eof
				then return (digests, (rc, rp))
				else do
					l <- BS.hGetLine h
					let	fs = V.fromList $ BS.split (fromIntegral $ fromEnum '|') l
					b <- validate fs checks
					b <- if Map.null sets
						then return b
						else if b
							then do
								let	chk False _ = return False
									chk _ f = do
										x <- hash <$> eval fs (Field f)
										return $ IntSet.member x $
											Map.findWithDefault
												IntSet.empty
												f
												sets
								foldM chk b relFields
							else return False
					digests <- if b
						then do
							let	add ds fi = do
									x <- hash <$> eval fs (Field fi)
									return $ Map.insertWith IntSet.union fi (IntSet.singleton x) ds
							foldM add digests relFields
						else return digests
					scanLoop sets (rc + 1) (rp + fromEnum b) digests h relFields checks

