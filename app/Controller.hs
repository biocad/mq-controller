{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Control.Concurrent     (forkIO, threadDelay)
import           Control.Monad          (forever)
import           Control.Monad.IO.Class (liftIO)
import           Control.Monad.State    (StateT, evalStateT, get, put)
import           Data.Aeson.Picker      ((|--))
import           Data.Map.Strict        (Map)
import qualified Data.Map.Strict        as M (difference, toList)
import           Data.Text              (pack)
import           System.BCD.Config      (getConfigText)
import           System.MQ.Component    (runApp)
import           System.MQ.Controller   (ControllerConfig (..), runController)
import           System.MQ.Protocol     (Spec)
import           System.MQ.Transport    (Port)

-- | Map that maps specs of messages to ports to which controllers that handle these messages bind
--
type ControllerMap = Map Spec Port

-- | Name of controller
--
controllerName :: String
controllerName = "mq_controller"

main :: IO ()
main = evalStateT startControllers mempty

startControllers :: StateT ControllerMap IO ()
startControllers = forever $ do
    curMap    <- get
    configMap <- liftIO getControllerMap

    -- If new controllers were added to the list of connections during last minute,
    -- we will run them. If any controllers were changed or deleted during this period of time,
    -- these changes in list of connections won't be noticed and won't take place until
    -- restart of this application.
    let diff = toConfigs $ M.difference configMap curMap
    liftIO $ mapM_ (forkIO . runApp controllerName . runController) diff

    put configMap
    liftIO (threadDelay oneMinute)

  where
    getControllerMap :: IO ControllerMap
    getControllerMap = (|-- ["params", pack controllerName, "connections"]) <$> getConfigText

    toConfigs :: ControllerMap -> [ControllerConfig]
    toConfigs = fmap (uncurry ControllerConfig) . M.toList

    oneMinute :: Int
    oneMinute = 60 * 10^(6 :: Int)
