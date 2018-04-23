{-# LANGUAGE RecordWildCards #-}

module System.MQ.Controller
  ( ControllerConfig (..)
  , runController
  ) where

import           Control.Monad                 (when)
import           Control.Monad.IO.Class        (liftIO)
import qualified Data.ByteString.Char8         as BSC8 (unpack)
import           Data.String                   (fromString)
import           System.Log.Logger             (infoM)
import           System.MQ.Component           (Env (..), TwoChannels (..),
                                                load2Channels)
import           System.MQ.Component.Transport (push, sub)
import           System.MQ.Monad               (MQMonad, foreverSafe)
import           System.MQ.Protocol            (Condition (..), Message (..),
                                                MessageTag, MessageType (..),
                                                Spec, matches, messageSpec,
                                                messageType)
import           System.MQ.Transport           (HostPort (..), Port,
                                                PushChannel, anyHost, bindTo,
                                                context)

-- | Configuration to run controller
--
data ControllerConfig = ControllerConfig { spec :: Spec  -- ^ spec of messages that controller lets through
                                         , port :: Port  -- ^ port to which controller binds
                                         }
  deriving (Eq, Show)

-- | Given 'ControllerConfig' produces action to run on communication level
--
runController :: ControllerConfig -> Env -> MQMonad ()
runController ControllerConfig{..} env@Env{..} = do
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
