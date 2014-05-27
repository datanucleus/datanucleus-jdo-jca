/**********************************************************************
Copyright (c) 2008 Erik Bengtson and others. All rights reserved. 
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. 

Contributors:
    ...
**********************************************************************/
package org.datanucleus.jdo.connector;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionEvent;
import javax.resource.spi.ConnectionEventListener;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.LocalTransaction;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionMetaData;
import javax.resource.spi.security.PasswordCredential;
import javax.security.auth.Subject;
import javax.transaction.xa.XAResource;

import org.datanucleus.api.jdo.JDOPersistenceManager;

/**
 * Implementation of ManagedConnection persistence manager
 * Handle is the Object Instance of the API the user application is interacting with.
 */
public class ManagedConnectionImpl implements ManagedConnection
{
    private final PasswordCredential credential;

    /**
     * If {@link #notifyClosed(PersistenceManagerImpl)} is called during a
     * transaction, the handles are copied here in order to be able to call
     * their {@link ConnectionEventListener}s during
     * {@link #notifyTxCommit(PersistenceManagerImpl)} and
     * {@link #notifyTxRollback(PersistenceManagerImpl)}. These methods will
     * remove the handles here. 
     */
    //private final List closedHandles = new ArrayList();

    /**
     * Those instances of {@link PersistenceManagerImpl}, which have been
     * opened for this managed connection.
     */
    private final List<PersistenceManagerImpl> handles = new ArrayList();

    /** event listeners **/
    private final Collection<ConnectionEventListener> cels = new ArrayList();

    private PrintWriter logWriter;
    
    /** The application server enlists this XAResource into a XA transaction */
    private XAResource xares;
    
    private ContainerLocalTransaction localTx;
    
    private JDOPersistenceManager pm;
    
    private final ManagedConnectionFactoryImpl mcf;
    
    /**
     * Constructor.
     * @param mcf the ManagedConnectionFactory
     * @param credential the PasswordCredential
     * @throws ResourceException if error occurs
     */
    public ManagedConnectionImpl(ManagedConnectionFactoryImpl mcf, PasswordCredential credential)
    throws ResourceException
    {
        this.credential = credential;
        this.mcf = mcf;
    }

    PasswordCredential getPasswordCredential()
    {
        return credential;
    }

    ManagedConnectionFactoryImpl getManagedConnectionFactory()
    {
        return mcf;
    }

    /**
     * Method to start the Transaction
     */
    public void begin()
    {
        if (getPersistenceManager().getExecutionContext().getTransaction() == null)
        {
            PersistenceManagerImpl.LOGGER.error("Invalid state during begin invoke. Transaction is closed.");
            return;
        }
        PersistenceManagerImpl.LOGGER.debug("Beginning ManagedConnection "+this);
        if (!getPersistenceManager().getExecutionContext().getTransaction().isActive())
        {
        	getPersistenceManager().getExecutionContext().getTransaction().begin();
        }

        notifyBegin();
    }

    // implementation of javax.resource.spi.ManagedConnection interface

    /**
     * Destroy method
     * @exception javax.resource.ResourceException if error occurs
     */
    public void destroy()
    throws ResourceException
    {
        PersistenceManagerImpl.LOGGER.debug("Destroying ManagedConnection "+this);
        
        // if the user has not closed it, we close it
        if (!handles.isEmpty())
        {
            List handlesToClose = new ArrayList(handles);
            for (Iterator it = handlesToClose.iterator(); it.hasNext(); )
            {
                PersistenceManagerImpl om = ((PersistenceManagerImpl)it.next());
                if (!om.isClosed())
                {
                    om.close();
                }
            }
        }
        if (pm != null)
        {
        	pm.close();
        }
        //xares = null;
        localTx = null;
        pm = null;
    }

    /**
     * Cleanup method
     * @exception javax.resource.ResourceException if error occurs
     */
    public synchronized void cleanup()
    throws ResourceException
    {
        PersistenceManagerImpl.LOGGER.debug("Cleaning up ManagedConnection "+this);
        if (pm != null)
        {
        	pm.getExecutionContext().closeCallbackHandler();
        }

        for (Iterator<PersistenceManagerImpl> i = handles.iterator(); i.hasNext();)
        {
            i.next().setManagedConnection(null);
        }
        handles.clear();
        //closedHandles.clear();
        //xares = null;

        // added by Marco: without the following call, all PMs ever used add up in JDOPersistenceManagerFactory.pmCache! 
        // However, I'm not sure whether this is really correct - what's the difference to destroy() then?
        if (pm != null)
        {
            pm.close();
        }

        localTx = null;
        pm = null;
        //after calling this, this MC goes to the connection pool in the application server...
        //TODO really clean out resources here
    }

    public JDOPersistenceManager getPersistenceManager()
    {
    	if (pm == null)
    	{
    		if (getPasswordCredential() == null)
    		{
    			pm = (JDOPersistenceManager) mcf.getPersistenceManagerFactory().getPersistenceManager();
    		}
    		else
    		{
    			pm = (JDOPersistenceManager) mcf.getPersistenceManagerFactory().getPersistenceManager(getPasswordCredential().getUserName(),new String(getPasswordCredential().getPassword()));
    		}
    	}
		return pm;
	}
    
    /**
     * Accessor for the connection
     * @param subject The subject
     * @param cri request info
     * @return The connection
     * @exception javax.resource.ResourceException if error occurs
     */
    public Object getConnection(Subject subject, ConnectionRequestInfo cri)
    throws ResourceException
    {
        PersistenceManagerImpl.LOGGER.debug("Obtaining Connection for this ManagedConnection "+this);
        PasswordCredential pc = getManagedConnectionFactory().getPasswordCredential(subject);
        if (credential != pc && credential != null && pc != null && !credential.equals(pc))
        {
            throw new ResourceException("Wrong subject: "+subject+" MCF credentials: "+pc+" MC credentials: "+credential);
        }

        PersistenceManagerImpl pm = new PersistenceManagerImpl(this);
        handles.add(0, pm);
        return pm;
    }

    /**
     * Accessor for the Log
     * @return The Log writer
     * @exception javax.resource.ResourceException if error occurs
     */
    public PrintWriter getLogWriter()
    throws ResourceException
    {
        return logWriter;
    }

    /**
     * Mutator for the Log
     * @param writer PrintWriter to use for Log
     * @exception javax.resource.ResourceException if error occurs
     */
    public void setLogWriter(PrintWriter writer)
    throws ResourceException
    {
        this.logWriter = writer;
    }

    /**
     * Mutator to add a connection listener
     * @param cel event listener
     */
    public void addConnectionEventListener(ConnectionEventListener cel)
    {
        synchronized (cels)
        {
            cels.add(cel);
        }
    }

    /**
     * Mutator to remove a connection listener
     * @param cel event listener
     */
    public void removeConnectionEventListener(ConnectionEventListener cel)
    {
        synchronized (cels)
        {
            cels.remove(cel);
        }
    }

    /**
     * Mutator to associate a connection
     * @param c connection
     * @exception javax.resource.ResourceException if error occurs
     */
    public void associateConnection(Object c)
    throws ResourceException
    {
        if (!(c instanceof PersistenceManagerImpl)) 
        {
            throw new ResourceException("wrong Connection type!");
        }
        PersistenceManagerImpl.LOGGER.debug("Associating "+c+" to this ManagedConnection "+this);
        ((PersistenceManagerImpl)c).setManagedConnection(this);
        if (!handles.contains(c))
        {
        	handles.add(0, (PersistenceManagerImpl)c);
        }
    }

    /**
     * Accessor for the local transaction
     * @return local txn
     * @exception javax.resource.ResourceException if error occurs
     */
    public LocalTransaction getLocalTransaction()
    throws ResourceException
    {
        PersistenceManagerImpl.LOGGER.debug("ManagedConnectionImpl.getLocalTransaction() invoked");
        if (localTx==null)
        {
            localTx = new ContainerLocalTransaction(this);
        }
        return localTx;
    }

    /**
     * Accessor for the connection MetaData
     * @return metadata for the connection
     * @exception javax.resource.ResourceException if error occurs
     */
    public ManagedConnectionMetaData getMetaData()
    throws ResourceException
    {
        throw new ResourceException("Not Yet Implemented");
    }

    /**
     * Accessor for the XA resource. The application server enlists
     * this XAResource into a XA transaction
     * This is invoked only once per instance.
     * @return XA resource
     * @exception javax.resource.ResourceException if error occurs
     */
    public XAResource getXAResource()
    throws ResourceException
    {
        PersistenceManagerImpl.LOGGER.debug("ManagedConnectionImpl.getXAResource() invoked");
        if (xares == null)
        {
            xares = new ConnectionXAResource(this);
        }
        return xares;
    }

    //ConnectionEvent management

    /**
     * Called by the PM handle, whenever it gets closed
     */
    void notifyClosed(PersistenceManagerImpl handle)
    {
        //closedHandles.add(handle);

        //do not disconnect SM objects, since we may have to run reachability later during commit/rollback
        //if( closedHandles.size() < handles.size() )
        //    return;
        //see ConnectionEventListeners 6.5.6.1
        //the application server is listening to these events

        //only notify closed, if all handles are closed
        ConnectionEvent ce = new ConnectionEvent(this, ConnectionEvent.CONNECTION_CLOSED, null);
        ce.setConnectionHandle(handle);
        Collection<ConnectionEventListener> localCels = null;
        synchronized (cels)
        {
            localCels = new ArrayList(cels);
        }

        for (Iterator<ConnectionEventListener> i = localCels.iterator(); i.hasNext(); )
        {
            i.next().connectionClosed(ce);
        }
    }

    void notifyBegin()
    {
        for (Iterator<PersistenceManagerImpl> it = handles.iterator(); it.hasNext(); ) 
        {
            notifyTxBegin(it.next());
        }
    }

    void notifyCommit()
    {
        for (Iterator<PersistenceManagerImpl> it = handles.iterator(); it.hasNext(); ) 
        {
            notifyTxCommit(it.next());
        }
        /*
        List ch = new ArrayList(closedHandles);
        ch.removeAll(handles);
        for (Iterator it = ch.iterator(); it.hasNext(); )
        {
            notifyTxCommit((PersistenceManagerImpl)it.next());
        }
        */
    }

    void notifyRollback()
    {
        List h = new ArrayList(handles);
        for (Iterator<PersistenceManagerImpl> it = h.iterator(); it.hasNext(); )
        {
            notifyTxRollback(it.next());
        }

        /*
        List ch = new ArrayList(closedHandles);
        ch.removeAll(h);
        for (Iterator it = ch.iterator(); it.hasNext(); )
        {
            notifyTxRollback((PersistenceManagerImpl)it.next());
        }
        */
    }

    void clearHandles()
    {
        handles.clear();
        //closedHandles.clear();
    }
    
    void notifyTxBegin(PersistenceManagerImpl handle)
    {
        /*
        ConnectionEvent ce = new ConnectionEvent(this, ConnectionEvent.LOCAL_TRANSACTION_STARTED, null);
        ce.setConnectionHandle(handle);
        Collection localCels = null;
        synchronized (cels)
        {
            localCels = new ArrayList(cels);
        }

        for (Iterator i = localCels.iterator(); i.hasNext(); )
        {
            ((ConnectionEventListener)i.next()).localTransactionStarted(ce);

        }
        */
    }

    void notifyTxCommit(PersistenceManagerImpl handle)
    {
        /*
        ConnectionEvent ce = new ConnectionEvent(this, ConnectionEvent.LOCAL_TRANSACTION_COMMITTED, null);
        ce.setConnectionHandle(handle);
        Collection localCels = null;
        synchronized (cels)
        {
            localCels = new ArrayList(cels);
        }
        for (Iterator i = localCels.iterator(); i.hasNext(); )
        {
            ((ConnectionEventListener)i.next()).localTransactionCommitted(ce);

        }
        */
    }

    void notifyTxRollback(PersistenceManagerImpl handle)
    {
        /*
        ConnectionEvent ce = new ConnectionEvent(this, ConnectionEvent.LOCAL_TRANSACTION_ROLLEDBACK, null);
        ce.setConnectionHandle(handle);
        Collection localCels = null;
        synchronized (cels)
        {
            localCels = new ArrayList(cels);
        }

        for (Iterator i = localCels.iterator(); i.hasNext(); )
        {
            ((ConnectionEventListener)i.next()).localTransactionRolledback(ce);

        }
        */
    }
}