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

import javax.jdo.JDOException;
import javax.resource.ResourceException;
import javax.resource.spi.LocalTransaction;

/**
 * Container-demarcated local transaction.
 * Starts the LocalTransaction of the Resource. The JCA container uses
 * this interface to control the lifecycle of local transactions.
 */
public class ContainerLocalTransaction implements LocalTransaction
{
    private ManagedConnectionImpl mc;

    ContainerLocalTransaction(ManagedConnectionImpl mc)
    {
        this.mc = mc;
    }
    
    /**
     * Method to start the Transaction
     * @exception javax.resource.ResourceException <description>
     */
    public void begin()
    throws ResourceException
    {
        PersistenceManagerImpl.LOGGER.debug("LocalResource.begin()");
        try 
        {
            internalBegin();
        }
        catch (JDOException e)
        {
            throw new ResourceException("JDOException: " + e);
        }
    }

    /**
     * Method to commit the Transaction
     * @exception javax.resource.ResourceException <description>
     */
    public void commit()
    throws ResourceException
    {
        PersistenceManagerImpl.LOGGER.debug("LocalResource.commit()");
        try 
        {
            internalCommit();
        }
        catch (JDOException e)
        {
            if (PersistenceManagerImpl.LOGGER.isInfoEnabled())
            {
                PersistenceManagerImpl.LOGGER.info("Exception during commit: ", e);
            }
            throw new ResourceException("JDOException: " + e);
        }

        mc.notifyCommit();
        mc.clearHandles();
    }

    /**
     * Method to rollback the Transaction
     * @exception javax.resource.ResourceException <description>
     */
    public void rollback()
    throws ResourceException
    {
        PersistenceManagerImpl.LOGGER.debug("Local Resource.rollback()");
        try 
        {
            internalRollback();
        }
        catch (JDOException e)
        {
            if (PersistenceManagerImpl.LOGGER.isInfoEnabled())
            {
                PersistenceManagerImpl.LOGGER.info("Exception during rollback: ", e);
            }
            throw new ResourceException("JDOException: " + e);
        }
    }

    /**
     * Method to rollback the Transaction
     */
    public void internalRollback()
    {
        if (mc.getPersistenceManager().getExecutionContext().getTransaction() == null)
        {
            PersistenceManagerImpl.LOGGER.error("Invalid state during rollback invoke. Transaction is closed.");
            return;
        }
        PersistenceManagerImpl.LOGGER.debug("Rolling back ManagedConnection "+this);
        
        mc.getPersistenceManager().getExecutionContext().getTransaction().rollback();
    } 
    
    /**
     * Method to start the Transaction
     */
    public void internalBegin()
    {
        if (mc.getPersistenceManager().getExecutionContext().getTransaction() == null)
        {
            PersistenceManagerImpl.LOGGER.error("Invalid state during begin invoke. Transaction is closed.");
            return;
        }
        PersistenceManagerImpl.LOGGER.debug("Beginning ManagedConnection "+this);
        if (!mc.getPersistenceManager().getExecutionContext().getTransaction().isActive())
        {
        	mc.getPersistenceManager().getExecutionContext().getTransaction().begin();
        }
    }
    
    /**
     * Method to commit the Transaction
     */
    public void internalCommit()
    {
        if (mc.getPersistenceManager().getExecutionContext().getTransaction() == null)
        {
            PersistenceManagerImpl.LOGGER.error("Invalid state during commit invoke. Transaction is closed.");
            return;
        }
        PersistenceManagerImpl.LOGGER.debug("Committing ManagedConnection "+this);
        
        mc.getPersistenceManager().getExecutionContext().getTransaction().commit();
    }
}