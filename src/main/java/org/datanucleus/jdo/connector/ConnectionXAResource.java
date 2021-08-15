/**********************************************************************
Copyright (c) 2007 Erik Bengtson and others. All rights reserved. 
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

import java.util.Hashtable;
import java.util.Map;

import javax.jdo.JDOException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.datanucleus.transaction.Transaction;
import org.datanucleus.api.jdo.JDOPersistenceManager;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.util.StringUtils;

/**
 * XAResource for the Connector. 
 * It is enlisted in the XA transaction when the DataNucleus Connector is allocated by the Application Server.
 */
public class ConnectionXAResource implements XAResource
{
    Map<Xid, JDOPersistenceManager> table = new Hashtable<Xid, JDOPersistenceManager>();

    ManagedConnectionImpl mc;

    ConnectionXAResource(ManagedConnectionImpl mc)
    {
        this.mc = mc;
    }
    
    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#commit(javax.transaction.xa.Xid, boolean)
     */
    public void commit(Xid xid, boolean flags) throws XAException
    {
        PersistenceManagerImpl.LOGGER.debug("Committing DataNucleus XA Resource of transaction "+xid.toString()+" - one-phase: "+flags);

        try 
        {
            internalCommit(table.remove(xid));
        }
        catch (JDOException e)
        {
            if (PersistenceManagerImpl.LOGGER.isInfoEnabled())
            {
                PersistenceManagerImpl.LOGGER.info("Exception during commit: ", e);
            }
            throw new XAException(StringUtils.getStringFromStackTrace(e));
        }
    }

    /**
     * Method to commit the Transaction
     * @param pm PersistenceManager
     */
    public void internalCommit(JDOPersistenceManager pm)
    {
        if (pm.getExecutionContext().getTransaction() == null)
        {
            PersistenceManagerImpl.LOGGER.error("Invalid state during commit invoke. Transaction is closed.");
            return;
        }
        PersistenceManagerImpl.LOGGER.debug("Committing ManagedConnection "+this);
        
        pm.getExecutionContext().getTransaction().commit();
    }
    
    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#end(javax.transaction.xa.Xid, int)
     */
    public void end(Xid xid, int flags) throws XAException
    {
        PersistenceManagerImpl.LOGGER.debug("Ending DataNucleus XA Resource of transaction "+xid.toString()+" with flags "+flags);
        
        if (flags == XAResource.TMSUCCESS || flags == XAResource.TMSUSPEND)
        {
            JDOPersistenceManager pm = table.get(xid);
            try
            {
                pm.flush();
            }
            finally
            {
                pm.getExecutionContext().getTransaction().end();
            }
        }
    }

    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#forget(javax.transaction.xa.Xid)
     */
    public void forget(Xid xid) throws XAException
    {
        table.remove(xid);    	
    }

    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#getTransactionTimeout()
     */
    public int getTransactionTimeout() throws XAException
    {
        return 0;
    }

    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#isSameRM(javax.transaction.xa.XAResource)
     */
    public boolean isSameRM(XAResource xares) throws XAException
    {
        return this==xares;
    }

    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#prepare(javax.transaction.xa.Xid)
     */
    public int prepare(Xid xid) throws XAException
    {
        PersistenceManagerImpl.LOGGER.debug("Preparing DataNucleus XA Resource of transaction "+xid.toString());
        
        return XAResource.XA_OK;
    }

    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#recover(int)
     */
    public Xid[] recover(int flags) throws XAException
    {
        return new Xid[0];
    }

    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#rollback(javax.transaction.xa.Xid)
     */
    public void rollback(Xid xid) throws XAException
    {
        PersistenceManagerImpl.LOGGER.debug("Rolling Back DataNucleus XA Resource of transaction "+xid.toString());
        
        try
        {
            internalRollback(table.remove(xid));
        }
        catch (NucleusException e)
        {
            if (PersistenceManagerImpl.LOGGER.isInfoEnabled())
            {
                PersistenceManagerImpl.LOGGER.info("Exception during commit: ", e);
            }
            throw new XAException(StringUtils.getStringFromStackTrace(e));
        }
    }
    
    /**
     * Method to rollback the Transaction
     * @param pm PersistenceManager
     */
    public void internalRollback(JDOPersistenceManager pm)
    {
        if (pm.getExecutionContext().getTransaction() == null)
        {
            PersistenceManagerImpl.LOGGER.error("Invalid state during rollback invoke. Transaction is closed.");
            return;
        }
        PersistenceManagerImpl.LOGGER.debug("Rolling back ManagedConnection "+this);
        
        pm.getExecutionContext().getTransaction().rollback();
    }    

    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#setTransactionTimeout(int)
     */
    public boolean setTransactionTimeout(int seconds) throws XAException
    {
        PersistenceManagerImpl.LOGGER.debug("Setting DataNucleus XA Resource transaction timeout to "+seconds+" seconds, however transaction timeout is not supported.");
        
        return false;
    }

    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#start(javax.transaction.xa.Xid, int)
     */
    public void start(Xid xid, int flags) throws XAException
    {
        PersistenceManagerImpl.LOGGER.debug("Starting DataNucleus XA Resource of transaction "+xid.toString()+" with flags "+flags);
        table.put(xid,mc.getPersistenceManager());
        try
        {
        	internalStart(mc.getPersistenceManager());
        }
        catch (JDOException e)
        {
            throw new XAException("JDOException: " + e);
        }  
    }
    
    /**
     * Method to start the Transaction
     * @param pm PersistenceManager
     */
    public void internalStart(JDOPersistenceManager pm)
    {
        Transaction tx = pm.getExecutionContext().getTransaction();
        if (tx == null)
        {
            PersistenceManagerImpl.LOGGER.error("Invalid state during begin invoke. Transaction is closed.");
            return;
        }
        PersistenceManagerImpl.LOGGER.debug("Beginning ManagedConnection "+this);
        if (!tx.isActive())
        {
        	tx.begin();
        }
    }
}