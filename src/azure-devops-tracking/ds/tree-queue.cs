////////////////////////////////////////////////////////////////////////////////
//
// Module: tree-queue.cs
//
//
////////////////////////////////////////////////////////////////////////////////

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

namespace ev27 {

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

public class TreeQueue<T>
{
    ////////////////////////////////////////////////////////////////////////////
    // Constructor
    ////////////////////////////////////////////////////////////////////////////

    public TreeQueue(int maxLeafSize = 500)
    {
        MaxLeafQueueSize = maxLeafSize;
        
        EnqueueQueue = new T[MaxLeafQueueSize];
        DequeueQueue = new T[MaxLeafQueueSize];

        EnqueueQueueSize = 0;
        DequeueQueueSize = 0;

        Finished = false;
        Finishing = false;

        TransportQueue = new Queue<T>();
        QueueLock = new Lock();
    }

    ////////////////////////////////////////////////////////////////////////////
    // Member variables
    ////////////////////////////////////////////////////////////////////////////

    public int MaxLeafQueueSize { get; set; }

    public bool Finished
    { 
        get
        {
            bool finished = false;
            lock(QueueLock)
            {
                finshed = _finished;
            }

            return finished;
        }

        private set;
    }

    private bool _finished { get; set; }
    private bool Finishing { get; set; }

    private T[] EnqueueQueue { get; set; }
    private long EnqueueQueueSize { get; set; }
    private T[] DequeueQueue { get; set; }
    private long DequeueQueueSize { get; set; }
    private Lock QueueLock { get; set; }
    private Queue<T> TransportQueue { get; set; }

    ////////////////////////////////////////////////////////////////////////////
    // Member methods
    ////////////////////////////////////////////////////////////////////////////

    public void Enqueue(T item)
    {
        _Enqueue(item);
    }

    public T Dequeue(Action waitCallback)
    {
        return _Dequeue(waitCallback);
    }

    public void SignalToFinish()
    {
        lock (QueueLock)
        {
            Finishing = true;
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // Helper methods
    ////////////////////////////////////////////////////////////////////////////

    private void _AddToTransportQueue()
    {
        // Under a lock, we can mess with Transport queue
        for (long index = 0; index < EnqueueQueueSize; ++index)
        {
            TransportQueue.Enqueue(EnqueueQueue[index]);
        }

        EnqueueQueueSize = 0;
    }

    private void _AddToDequeueQueue()
    {
        // Under a lock, we can mess with Transport queue
        if (TransportQueue.Count > 0)
        {
            for (long index = 0; index < MaxLeafQueueSize - 1; ++index)
            {
                DequeueQueue[index] = TransportQueue.Dequeue();
                ++DequeueQueueSize;
            }
        }
    }

    private void _Enqueue(T item)
    {
        if (EnqueueQueueSize + 1 == MaxLeafQueueSize)
        {
            QueueLock.GetLock();
            _AddToTransportQueue();
            QueueLock.Unlock();
        }

        EnqueueQueue[EnqueueQueueSize++] = item;
    }

    private T _Dequeue(Action waitCallback)
    {
        bool requireWaitCallback = true;
        do
        {
            if (DequeueQueueSize - 1 == -1)
            {
                QueueLock.GetLock();
                _AddToDequeueQueue();
                QueueLock.Unlock();
            }

            // If there is still not anything in the queue, we will call into
            // a callback which should either do more work or wait.
            if (DequeueQueueSize != 0)
            {
                requireWaitCallback = false;
            }
            else
            {
                lock (QueueLock)
                {
                    if (Finishing)
                    {
                        if (TransportQueue.Count == 0)
                        {
                            _finished = True;
                        }
                    }
                }
                
                waitCallback();

                lock (QueueLock)
                {
                    if (_finished)
                    {
                        return null;
                    }
                }
            }
        } while (requireWaitCallback);

        Debug.Assert(DequeueQueueSize != 0);

        var returnValue = DequeueQueue[DequeueQueueSize - 1];
        --DequeueQueueSize;
        return returnValue;
    }
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

} // end of namespace(ev27)

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////