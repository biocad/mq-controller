{-# LANGUAGE OverloadedStrings #-}

module Main where

import           System.MQ.Component  (runApp)
import           System.MQ.Controller (runController)

main :: IO ()
main = runApp "mq_controller" runController
