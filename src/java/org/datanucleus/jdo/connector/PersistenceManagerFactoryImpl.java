/**********************************************************************
Copyright (c) 2003 David Jencks and others. All rights reserved.
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
2004 Erik Bengtson - method close is delegated to AbstractPersistenceManagerFactory
2005 Marco Schulze - implemented copying the lifecycle listeners in j2ee environment 
    ...
**********************************************************************/
package org.datanucleus.jdo.connector;

import java.util.Collection;
import java.util.Properties;
import java.util.Set;

import javax.jdo.FetchGroup;
import javax.jdo.JDOException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.datastore.DataStoreCache;
import javax.jdo.listener.InstanceLifecycleListener;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.resource.ResourceException;
import javax.resource.cci.Connection;
import javax.resource.cci.ConnectionFactory;
import javax.resource.cci.ConnectionSpec;
import javax.resource.cci.RecordFactory;
import javax.resource.cci.ResourceAdapterMetaData;
import javax.resource.spi.ConnectionManager;

/**
 * PersistenceManagerFactoryImpl for J2EE.
 */
public class PersistenceManagerFactoryImpl implements ConnectionFactory, PersistenceManagerFactory
{
    private final ManagedConnectionFactoryImpl mcf;
    private final ConnectionManager cm;

    private Reference ref;

    /**
     * Constructor 
     * @param mcf the ManagedConnectionFactory
     * @param cm the ConnectionManager
     */
    public PersistenceManagerFactoryImpl(final ManagedConnectionFactoryImpl mcf, final ConnectionManager cm) 
    {
        this.mcf = mcf;
        this.cm = cm;      
    }

    // implementation of javax.resource.Referenceable interface

    /**
     *
     * @param ref <description>
     */
    public void setReference(Reference ref)
    {
        this.ref = ref;
    }

    // implementation of javax.naming.Referenceable interface

    /**
     *
     * @return <description>
     * @exception javax.naming.NamingException <description>
     */
    public Reference getReference() throws NamingException
    {
        return ref;
    }

    // implementation of javax.resource.cci.ConnectionFactory interface

    /**
     * This is invoked when the user asks for an instance of the PersistenceManager;
     * If this method is invoked multiple times in a single transaction, it's invoked on the same instance of this PMF;
     * @return <description>
     * @exception javax.resource.ResourceException <description>
     */
    public Connection getConnection() throws ResourceException
    {
        return (Connection)cm.allocateConnection(mcf, null);
    }

    /**
     *
     * @param cs <description>
     * @return <description>
     * @exception javax.resource.ResourceException <description>
     */
    public Connection getConnection(ConnectionSpec cs) throws ResourceException
    {
        return getConnection();
    }

    /**
     *
     * @return <description>
     * @exception javax.resource.ResourceException <description>
     */
    public ResourceAdapterMetaData getMetaData() throws ResourceException
    {
        throw new ResourceException("Not Yet Implemented");
    }

    /**
     *
     * @return <description>
     * @exception javax.resource.ResourceException <description>
     */
    public RecordFactory getRecordFactory() throws ResourceException
    {
        return null;
    }

    // implementation of javax.jdo.PersistenceManagerFactory interface

    /**
     *
     * @return <description>
     */
    public Properties getProperties()
    {
        throw new JDOException("Not available in managed environment");
    }

    /**
     *
     * @return <description>
     */
    public PersistenceManager getPersistenceManager()
    {
        try 
        {
            return (PersistenceManager)getConnection();
        }
        catch (ResourceException e)
        {
            throw new JDOException("Problem getting PersistenceManager:", new Exception[] {e});
        }
    }

    /**
     *
     * @param user Username
     * @param pw password
     * @return PersistenceManager
     */
    public PersistenceManager getPersistenceManager(String user, String pw)
    {
        throw new JDOException("Not available in managed environment");
    }

    /**
     * Mutator for the Username 
     *
     * @param username Username
     */
    public void setConnectionUserName(String username)
    {
        throw new JDOException("Not available in managed environment");
    }

    /**
     * Accessor for the username 
     *
     * @return Username
     */
    public String getConnectionUserName()
    {
        throw new JDOException("Not available in managed environment");
    }

    /**
     * Mutator for the Password 
     *
     * @param password Password
     */
    public void setConnectionPassword(String password)
    {
        throw new JDOException("Not available in managed environment");
    }

    /**
     * Mutator for the connection URL 
     *
     * @param url Connection URL for the datasource
     */
    public void setConnectionURL(String url)
    {
        throw new JDOException("Not available in managed environment");
    }

    /**
     * Accessor for the connection URL 
     *
     * @return The connection URL
     */
    public String getConnectionURL()
    {
        throw new JDOException("Not available in managed environment");
    }

    /**
     *
     * @param drivername Driver name
     */
    public void setConnectionDriverName(String drivername)
    {
        throw new JDOException("Not available in managed environment");
    }

    /**
     *
     * @return The connection driver name
     */
    public String getConnectionDriverName()
    {
        throw new JDOException("Not available in managed environment");
    }

    /**
     *
     * @param conn_factory_name <description>
     */
    public void setConnectionFactoryName(String conn_factory_name)
    {
        throw new JDOException("Not available in managed environment");
    }

    /**
     *
     * @return Connection Factory name
     */
    public String getConnectionFactoryName()
    {
        throw new JDOException("Not available in managed environment");
    }

    /**
     *
     * @param conn_factory Connection factory
     */
    public void setConnectionFactory(Object conn_factory)
    {
        throw new JDOException("Not available in managed environment");
    }

    /**
     *
     * @return The connection factory
     */
    public Object getConnectionFactory()
    {
        throw new JDOException("Not available in managed environment");
    }

    /**
     *
     * @param conn_factory_name Connection factory name
     */
    public void setConnectionFactory2Name(String conn_factory_name)
    {
        throw new JDOException("Not available in managed environment");
    }

    /**
     *
     * @return Connection factory name
     */
    public String getConnectionFactory2Name()
    {
        throw new JDOException("Not available in managed environment");
    }

    /**
     *
     * @param conn_factory Connection Factory
     */
    public void setConnectionFactory2(Object conn_factory)
    {
        throw new JDOException("Not available in managed environment");
    }

    /**
     *
     * @return <description>
     */
    public Object getConnectionFactory2()
    {
        throw new JDOException("Not available in managed environment");
    }

    /**
     *
     * @param multithreaded Whether is multithreaded or not
     */
    public void setMultithreaded(boolean multithreaded)
    {
        mcf.getPersistenceManagerFactory().setMultithreaded(multithreaded);
    }

    /**
     *
     * @return <description>
     */
    public boolean getMultithreaded()
    {
        return mcf.getPersistenceManagerFactory().getMultithreaded();
    }

    /**
     *
     * @param optimistic <description>
     */
    public void setOptimistic(boolean optimistic)
    {
    	mcf.getPersistenceManagerFactory().setOptimistic(optimistic);
    }

    /**
     *
     * @return <description>
     */
    public boolean getOptimistic()
    {
        return mcf.getPersistenceManagerFactory().getOptimistic();
    }

    /**
     *
     * @param retain_values <description>
     */
    public void setRetainValues(boolean retain_values)
    {
    	mcf.getPersistenceManagerFactory().setRetainValues(retain_values);
    }

    /**
     *
     * @return <description>
     */
    public boolean getRetainValues()
    {
        return mcf.getPersistenceManagerFactory().getRetainValues();
    }

    /**
     *
     * @param restore_values <description>
     */
    public void setRestoreValues(boolean restore_values)
    {
    	mcf.getPersistenceManagerFactory().setRestoreValues(restore_values);
    }

    /**
     *
     * @return <description>
     */
    public boolean getRestoreValues()
    {
        return mcf.getPersistenceManagerFactory().getRestoreValues();
    }

    /**
     *
     * @param nontrans_read <description>
     */
    public void setNontransactionalRead(boolean nontrans_read)
    {
    	mcf.getPersistenceManagerFactory().setNontransactionalRead(nontrans_read);
    }

    /**
     *
     * @return <description>
     */
    public boolean getNontransactionalRead()
    {
        return mcf.getPersistenceManagerFactory().getNontransactionalRead();
    }

    /**
     *
     * @param nontrans_write <description>
     */
    public void setNontransactionalWrite(boolean nontrans_write)
    {
    	mcf.getPersistenceManagerFactory().setNontransactionalWrite(nontrans_write);
    }

    /**
     *
     * @return <description>
     */
    public boolean getNontransactionalWrite()
    {
        return mcf.getPersistenceManagerFactory().getNontransactionalWrite();
    }

    /**
     *
     * @param ignore <description>
     */
    public void setIgnoreCache(boolean ignore)
    {
    	mcf.getPersistenceManagerFactory().setIgnoreCache(ignore);
    }

    /**
     *
     * @return <description>
     */
    public boolean getIgnoreCache()
    {
        return mcf.getPersistenceManagerFactory().getIgnoreCache();
    }

    /**
     * Mutator for whether to detach all objects on commit of the txn.
     * @param detach Whether to detach objects on commit of the txn.
     */
    public void setDetachAllOnCommit(boolean detach)
    {
    	mcf.getPersistenceManagerFactory().setDetachAllOnCommit(detach);
    }

    /**
     * Accessor for whether to detach objects on commit of the txn.
     * @return Whether to detach objects on commit of the txn.
     */
    public boolean getDetachAllOnCommit()
    {
        return mcf.getPersistenceManagerFactory().getDetachAllOnCommit();
    }
    
    /**
     * Mutator for the Mapping name
     * @param mapping mapping name
     */
    public void setMapping(String mapping)
    {
    	mcf.getPersistenceManagerFactory().setMapping(mapping);
    }
    
    /**
     * Accessor for the mapping name
     * @return mapping name
     */
    public String getMapping()
    {
        return mcf.getPersistenceManagerFactory().getMapping();
    }
   
    /**
     * Accessor for the supported options 
     *
     * @return Collection of supported options
     */
    public Collection<String> supportedOptions()
    {
        return mcf.getPersistenceManagerFactory().supportedOptions();
    }
    
    /**
     * Accessor for the Datastore Cache
     * @return The datastore cache
     */
    public DataStoreCache getDataStoreCache()
    {
        return mcf.getPersistenceManagerFactory().getDataStoreCache();
    }
    
    /**
     * @see javax.jdo.PersistenceManagerFactory#close()
     */
    public void close()
    {
    	mcf.getPersistenceManagerFactory().close();
    }

    /**
     * Accessor for whether the PMF is closed.
     * @return Whether it is closed.
     */
    public boolean isClosed()
    {
        return mcf.getPersistenceManagerFactory().isClosed();
    }

	/**
     * JDO 2.0 spec 12.15 "LifecycleListeners".
     * @param listener The instance lifecycle listener to sends events to
     * @param classes The classes that it is interested in
     * @since 1.1
     */
    public void addInstanceLifecycleListener(InstanceLifecycleListener listener, Class[] classes)
    {
    	mcf.getPersistenceManagerFactory().addInstanceLifecycleListener(listener, classes);
    }

    /**
     * JDO 2.0 spec 12.15 "LifecycleListeners".
     * @param listener The instance lifecycle listener to remove.
     * @since 1.1
     */
    public void removeInstanceLifecycleListener(InstanceLifecycleListener listener)
    {
    	mcf.getPersistenceManagerFactory().removeInstanceLifecycleListener(listener);
    }

    public PersistenceManager getPersistenceManagerProxy()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Accessor for the persistence-unit name.
     * @return Name of the "persistence-unit"
     */
    public String getPersistenceUnitName()
    {
        return mcf.getPersistenceManagerFactory().getPersistenceUnitName();
    }

    /**
     * Accessor for the server timezone id.
     * @return Server timezone id
     */
    public String getServerTimeZoneID()
    {
        return mcf.getPersistenceManagerFactory().getServerTimeZoneID();
    }

    /**
     * Accessor for the transaction type.
     * @return Transaction type
     */
    public String getTransactionType()
    {
        return mcf.getPersistenceManagerFactory().getTransactionType();
    }

    /**
     * Mutator for the persistence-unit name
     * @param name Name of the "persistence-unit"
     */
    public void setPersistenceUnitName(String name)
    {
        mcf.setPersistenceUnitName(name);
    }

    /**
     * Mutator for the server timezone id.
     * @param id Server timezone id
     */
    public void setServerTimeZoneID(String id)
    {
    	mcf.getPersistenceManagerFactory().setServerTimeZoneID(id);
    }

    /**
     * Mutator for the transaction type.
     * @param type Transaction type
     */
    public void setTransactionType(String type)
    {
    	mcf.getPersistenceManagerFactory().setTransactionType(type);
    }

    /**
     *
     * @param readOnly <description>
     */
    public void setReadOnly(boolean readOnly)
    {
        mcf.getPersistenceManagerFactory().setReadOnly(readOnly);
    }

    /**
     *
     * @return <description>
     */
    public boolean getReadOnly()
    {
        return mcf.getPersistenceManagerFactory().getReadOnly();
    }

    /**
     * Accessor for the name of the PMF.
     * @return Name of the PMF
     */
    public String getName()
    {
        return mcf.getPersistenceManagerFactory().getName();
    }

    /**
     * Mutator for the name of the PMF.
     * @param name Name of the PMF
     */
    public void setName(String name)
    {
    	mcf.getPersistenceManagerFactory().setName(name);
    }

    public boolean getCopyOnAttach()
    {
        return mcf.getPersistenceManagerFactory().getCopyOnAttach();
    }

    public void setCopyOnAttach(boolean flag)
    {
    	mcf.getPersistenceManagerFactory().setCopyOnAttach(flag);
    }

    /* (non-Javadoc)
     * @see javax.jdo.PersistenceManagerFactory#getTransactionIsolationLevel()
     */
    public String getTransactionIsolationLevel()
    {
        return mcf.getPersistenceManagerFactory().getTransactionIsolationLevel();
    }

    /* (non-Javadoc)
     * @see javax.jdo.PersistenceManagerFactory#setTransactionIsolationLevel(java.lang.String)
     */
    public void setTransactionIsolationLevel(String level)
    {
        mcf.getPersistenceManagerFactory().setTransactionIsolationLevel(level);
    }

    /* (non-Javadoc)
     * @see javax.jdo.PersistenceManagerFactory#addFetchGroups(javax.jdo.FetchGroup[])
     */
    public void addFetchGroups(FetchGroup... groups)
    {
        mcf.getPersistenceManagerFactory().addFetchGroups(groups);
    }

    /* (non-Javadoc)
     * @see javax.jdo.PersistenceManagerFactory#getFetchGroup(java.lang.Class, java.lang.String)
     */
    public FetchGroup getFetchGroup(Class cls, String name)
    {
        return mcf.getPersistenceManagerFactory().getFetchGroup(cls, name);
    }

    /* (non-Javadoc)
     * @see javax.jdo.PersistenceManagerFactory#getFetchGroups()
     */
    public Set getFetchGroups()
    {
        return mcf.getPersistenceManagerFactory().getFetchGroups();
    }

    /* (non-Javadoc)
     * @see javax.jdo.PersistenceManagerFactory#removeAllFetchGroups()
     */
    public void removeAllFetchGroups()
    {
        mcf.getPersistenceManagerFactory().removeAllFetchGroups();
    }

    /* (non-Javadoc)
     * @see javax.jdo.PersistenceManagerFactory#removeFetchGroups(javax.jdo.FetchGroup[])
     */
    public void removeFetchGroups(FetchGroup... groups)
    {
        mcf.getPersistenceManagerFactory().removeFetchGroups(groups);
    }

    /* (non-Javadoc)
     * @see javax.jdo.PersistenceManagerFactory#getMetadata(java.lang.String)
     */
    public javax.jdo.metadata.TypeMetadata getMetadata(String className)
    {
        return mcf.getPersistenceManagerFactory().getMetadata(className);
    }

    /* (non-Javadoc)
     * @see javax.jdo.PersistenceManagerFactory#newMetadata()
     */
    public javax.jdo.metadata.JDOMetadata newMetadata()
    {
        return mcf.getPersistenceManagerFactory().newMetadata();
    }

    /* (non-Javadoc)
     * @see javax.jdo.PersistenceManagerFactory#registerMetadata(javax.jdo.metadata.JDOMetadata)
     */
    public void registerMetadata(javax.jdo.metadata.JDOMetadata md)
    {
        mcf.getPersistenceManagerFactory().registerMetadata(md);
    }

    /* (non-Javadoc)
     * @see javax.jdo.PersistenceManagerFactory#getDatastoreReadTimeoutMillis()
     */
    public Integer getDatastoreReadTimeoutMillis()
    {
        return mcf.getPersistenceManagerFactory().getDatastoreReadTimeoutMillis();
    }

    /* (non-Javadoc)
     * @see javax.jdo.PersistenceManagerFactory#setDatastoreReadTimeoutMillis(int)
     */
    public void setDatastoreReadTimeoutMillis(Integer timeout)
    {
        mcf.getPersistenceManagerFactory().setDatastoreReadTimeoutMillis(timeout);
    }

    /* (non-Javadoc)
     * @see javax.jdo.PersistenceManagerFactory#getDatastoreWriteTimeoutMillis()
     */
    public Integer getDatastoreWriteTimeoutMillis()
    {
        return mcf.getPersistenceManagerFactory().getDatastoreWriteTimeoutMillis();
    }

    /* (non-Javadoc)
     * @see javax.jdo.PersistenceManagerFactory#setDatastoreWriteTimeoutMillis(int)
     */
    public void setDatastoreWriteTimeoutMillis(Integer timeout)
    {
        mcf.getPersistenceManagerFactory().setDatastoreWriteTimeoutMillis(timeout);
    }

    /* (non-Javadoc)
     * @see javax.jdo.PersistenceManagerFactory#getManagedClasses()
     */
    public Collection<Class> getManagedClasses()
    {
        return mcf.getPersistenceManagerFactory().getManagedClasses();
    }
}