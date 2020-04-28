////////////////////////////////////////////////////////////////////////////////
//
// Module: lock.cs
//
//
////////////////////////////////////////////////////////////////////////////////

using System;
using System.Threading;

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

namespace ev27 {

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

public class Lock
{
    ////////////////////////////////////////////////////////////////////////////
    // Constructor
    ////////////////////////////////////////////////////////////////////////////

    public Lock()
    {
        LockObject = new object();
        Locked = false;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Member variables
    ////////////////////////////////////////////////////////////////////////////

    public bool Locked { get; private set; }

    private object LockObject { get; set; }

    ////////////////////////////////////////////////////////////////////////////
    // Member functions
    ////////////////////////////////////////////////////////////////////////////

    public void GetLock()
    {
        Monitor.Enter(LockObject);
        Locked = true;
    }

    public bool TryLock()
    {
        if (Monitor.TryEnter(LockObject))
        {
            Locked = true;
        }

        return Locked;
    }

    public void Unlock()
    {
        if (Locked)
        {
            Locked = false;
            Monitor.Exit(LockObject);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////