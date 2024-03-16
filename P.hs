-- Copyright (C) 2024 Serguey Zefirov
module P
	( module Control.Applicative
	, module P
	)where

import Control.Applicative

import Control.Monad.Fail (MonadFail)
import qualified Control.Monad.Fail as F

import Data.Char

newtype P i o = P { runP :: [i] -> [(o, [i])] }

instance Functor (P i) where
	fmap f (P p) = P $ \is -> [(f o, is') | (o, is') <- p is]

instance Applicative (P i) where
	pure x = P $ \is -> [(x, is)]
	f <*> a = do
		f <- f
		a <- a
		return $ f a

instance Alternative (P i) where
	empty = P $ const []
	P f <|> P g = P $ \is -> case f is of { [] -> g is; ss -> ss}

instance Monad (P i) where
	return = pure
	P p >>= q = P $ \is ->
		[ (x, is'')
		| (o, is') <- p is
		, let P qp = q o
		, (x, is'') <- qp is'
		]
	fail _ = empty

instance F.MonadFail (P i) where
	fail _ = empty

input :: P i i
input = P $ \is -> case is of
	i : is -> [(i, is)]
	_ -> []

cond :: (a -> Bool) -> P i a -> P i a
cond c p = do
	x <- p
	if c x then pure x else fail "no"

applyP :: P i o -> [i] -> (Maybe o, [i])
applyP (P p) is = case p is of
	[] -> (Nothing, is)
	(o, is') : _ -> (Just o, is')

