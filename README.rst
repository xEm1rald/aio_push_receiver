Subscribe to GCM/FCM and receive notifications

python implementation of https://github.com/MatthieuLemoine/push-receiver

Used by https://github.com/olijeffers0n/rustplus

Async usage
-----------

``AsyncPushReceiver`` uses ``asyncio`` streams directly, so receiving FCM
messages does not occupy a worker thread.  Its callback may be either a normal
function or an ``async def`` function.

.. code-block:: python

   import asyncio
   from push_receiver import AsyncPushReceiver


   async def main(credentials):
       async with AsyncPushReceiver(credentials) as receiver:
           async def on_notification(obj, notification, data_message):
               print(notification)
               if notification.get("channelId") == "pairing":
                   await receiver.stop()

           await receiver.listen(on_notification)


   asyncio.run(main(credentials))

Call ``await receiver.stop()`` from any task to close the active socket and
make ``listen`` return without waiting for the read timeout.  The existing
threaded ``PushReceiver`` remains available for backwards compatibility.
