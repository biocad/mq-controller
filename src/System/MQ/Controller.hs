{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module System.MQ.Controller
  ( runController
  ) where

import           Control.Concurrent    (forkIO, threadDelay)
import           Control.Monad         (forever, when)
import           Control.Monad.Except  (catchError, liftIO)
import           Control.Monad.State   (StateT, evalStateT, get, put)
import           Data.Aeson.Picker     ((|--))
import qualified Data.ByteString.Char8 as BSC8 (unpack)
import           Data.Map.Strict       (Map)
import qualified Data.Map.Strict       as M (difference, toList)
import           Data.String           (fromString)
import           Data.Text             (pack)
import           System.BCD.Config     (getConfigText)
import           System.Log.Logger     (infoM)
import           System.MQ.Component   (Env (..), TwoChannels (..),
                                        load2Channels, push, sub)
import           System.MQ.Monad       (MQMonad, errorHandler, foreverSafe,
                                        runMQMonad)
import           System.MQ.Protocol    (Condition (..), Message (..),
                                        MessageTag, MessageType (..), Spec,
                                        matches, messageSpec, messageType)
import           System.MQ.Transport   (HostPort (..), Port, PushChannel,
                                        anyHost, bindTo, context)

-- | Map that maps specs of messages to ports to which controllers that handle these messages bind
--
type ControllerMap = Map Spec Port

-- | Configuration to run controller
--
data ControllerConfig = ControllerConfig { spec :: Spec  -- ^ spec of messages that controller lets through
                                         , port :: Port  -- ^ port to which controller binds
                                         }
  deriving (Eq, Show)

runController :: Env -> MQMonad ()
runController env = liftIO $ evalStateT (runControllerS env) mempty

runControllerS :: Env -> StateT ControllerMap IO ()
runControllerS env@Env{..} = forever $ do
    curMap    <- get
    configMap <- liftIO getControllerMap

    -- If new controllers were added to the list of connections during last minute,
    -- we will run them. If any controllers were changed or deleted during this period of time,
    -- these changes in list of connections won't be noticed and won't take place until
    -- restart of this application.
    let diff = toConfigs $ M.difference configMap curMap
    liftIO $ mapM_ (forkIO . startController env) diff

    put configMap
    liftIO (threadDelay oneMinute)

  where
    getControllerMap :: IO ControllerMap
    getControllerMap = (|-- ["params", pack name, "connections"]) <$> getConfigText

    toConfigs :: ControllerMap -> [ControllerConfig]
    toConfigs = fmap (uncurry ControllerConfig) . M.toList

    oneMinute :: Int
    oneMinute = 60 * 10^(6 :: Int)

-- | Given 'ControllerConfig' produces action to run on communication level
--
startController :: Env -> ControllerConfig -> IO ()
startController env@Env{..} ControllerConfig{..} = runMQ $ do
    TwoChannels{..} <- load2Channels
    toComponent     <- connectController port

    foreverSafe name $ do
        (tag, msg@Message{..}) <- sub fromScheduler env
        liftIO $ infoM name $ "Received message with id: " ++ BSC8.unpack msgId
        when (filterMsg tag) (push toComponent env msg >> liftIO (infoM name $ "Sent message with id: " ++ BSC8.unpack msgId))

  where
    connectController :: Port -> MQMonad PushChannel
    connectController port' = do
        context' <- liftIO context
        bindTo (HostPort anyHost port') context'

    filterMsg :: MessageTag -> Bool
    filterMsg = (`matches` (messageType :== Config :&& messageSpec :== fromString spec))

    runMQ :: MQMonad () -> IO ()
    runMQ = runMQMonad . (`catchError` errorHandler name)
