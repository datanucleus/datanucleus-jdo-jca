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
2003 Erik Bengtson - added getObjectbyAID
2004 Andy Jefferson - coding standards and javadocs
2005 Marco Schulze - implemented copying the lifecycle listeners in j2ee environment
2005 Marco Schulze - added delegate methods to add/remove a ConnectionEventListener
    ...
**********************************************************************/
package org.datanucleus.jdo.connector;

import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import javax.jdo.Extent;
import javax.jdo.FetchGroup;
import javax.jdo.FetchPlan;
import javax.jdo.JDOException;
import javax.jdo.JDOFatalUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;
import javax.jdo.datastore.JDOConnection;
import javax.jdo.datastore.Sequence;
import javax.jdo.listener.InstanceLifecycleListener;
import javax.resource.ResourceException;
import javax.resource.cci.Connection;
import javax.resource.cci.ConnectionMetaData;
import javax.resource.cci.Interaction;
import javax.resource.cci.LocalTransaction;
import javax.resource.cci.ResultSetInfo;

import org.datanucleus.api.jdo.JDOPersistenceManager;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Implementation of the PersistenceManager for use in JCA environments.
 * An application handle associated to the {@link org.datanucleus.jdo.connector.ManagedConnectionImpl}.
 */
public class PersistenceManagerImpl implements Connection, PersistenceManager
{
    /** Localisation utility for output messages */
    protected static final Localiser LOCALISER = Localiser.getInstance("org.datanucleus.Localisation",
        org.datanucleus.ClassConstants.NUCLEUS_CONTEXT_LOADER);

    /** Logger for JCA. */
    public static final NucleusLogger LOGGER = NucleusLogger.getLoggerInstance("DataNucleus.JCA");

    private final ApplicationLocalTransaction tx;

    /** Underlying JDOPersistenceManager */
    JDOPersistenceManager pm;

    /** Underlying ManagedConnection. */
    ManagedConnectionImpl mc;

    /** Whether the PM is closed. */
    private boolean closed;

    /**
     * Constructor.
     * @param mc The connection implementation.
     */
    public PersistenceManagerImpl(final ManagedConnectionImpl mc) 
    {
        LOGGER.debug("Instantiating handle " + this + " for ManagedConnection " + mc);

        tx = new ApplicationLocalTransaction(this);
        setManagedConnection(mc);
    }

    public JDOPersistenceManager getJDOPersistenceManager()
    {
        return pm;
    }

    /**
     * Mutator for the connection implementation.
     * @param mc The managed connection implementation
     */
    void setManagedConnection(final ManagedConnectionImpl mc)
    {
        if (mc == null)
        {
        	this.mc = null;
        	this.pm = null;
        	return;
        }
        this.mc = mc;
        this.pm = mc.getPersistenceManager();
    }

    // implementation of javax.resource.cci.Connection interface

    /**
     * Accessor for the local transaction.
     * @return The local transaction
     * @exception javax.resource.ResourceException Thrown when an error occurs
     */
    public LocalTransaction getLocalTransaction()
    throws ResourceException
    {
        return tx;
    }

    /**
     * Accessor for Meta-Data.
     * @return The Connection Meta-Data.
     * @exception javax.resource.ResourceException <description>
     **/
    public ConnectionMetaData getMetaData() throws ResourceException
    {
        throw new ResourceException("Not Yet Implemented");
    }

    /**
     *
     * @return <description>
     * @exception javax.resource.ResourceException <description>
     **/
    public Interaction createInteraction() throws ResourceException
    {
        throw new ResourceException("Not Yet Implemented");
    }

    /**
     *
     * @return <description>
     * @exception javax.resource.ResourceException <description>
     **/
    public ResultSetInfo getResultSetInfo() throws ResourceException
    {
        throw new ResourceException("Not Yet Implemented");
    }

    // implementation of javax.jdo.PersistenceManager interface

    /**
     * Method to assert if this Persistence Manager is open. Throws a
     * JDOFatalUserException if it has been closed.
     */
    private void assertIsOpen()
    {
        if (isClosed())
        {
            throw new JDOFatalUserException(LOCALISER.msg("011000"));
        }
    }
    
    /**
     * Method to close the Persistence Manager.
     * This is invoked by the application server if this handle is not closed
     **/
    public void close()
    {
        assertIsOpen();
        LOGGER.debug("Closing handle "+this);

        if (pm != null) 
        {
            pm.flush();
            mc.notifyClosed(this);
        }
        closed = true;
    }

    /**
     * Accessor for whether the Persistence Manager is closed.
     * @return Whether the Persistence Manager is closed.
     **/
    public boolean isClosed()
    {
        return closed;
    }

    /**
     * Mutator for whether to detach all objects on commit of the txn.
     * @param detach Whether to detach objects on commit of the txn.
     */
    public void setDetachAllOnCommit(boolean detach)
    {
        pm.setDetachAllOnCommit(detach);
    }

    /**
     * Accessor for whether to detach objects on commit of the txn.
     * @return Whether to detach objects on commit of the txn.
     */
    public boolean getDetachAllOnCommit()
    {
        return pm.getDetachAllOnCommit();
    }

    /**
     * Method to refresh an object 
     * @param o Object to refresh
     **/
    public void refresh(Object o)
    {
        checkStatus();
        pm.refresh(o);
    }

    /**
     * Method to retrieve an object 
     * @param o Object to retrieve
     * @param fgOnly Whether to include only fields in current fetch group
     **/
    public void retrieve(Object o, boolean fgOnly)
    {
        checkStatus();
        pm.retrieve(o, fgOnly);
    }

    /**
     * Method to retrieve an object 
     * @param o Object to retrieve
     **/
    public void retrieve(Object o)
    {
        checkStatus();
        pm.retrieve(o);
    }

    /**
     * Retrieve a Collection of Persistence-Capable objects 
     * @param pcs Collection of Persistence-Capable objects
     */
    public void retrieveAll(Collection pcs)
    {
        checkStatus();
        pm.retrieveAll(pcs);
    }

    /**
     * Retrieve an array of Persistence-Capable objects 
     * @param pcs Array of Persistence-Capable objects
     */
    public void retrieveAll(Object... pcs)
    {
        checkStatus();
        pm.retrieveAll(pcs);
    }

    /**
     * Retrieve field values of instances from the store. This tells
     * the <code>PersistenceManager</code> that the application intends to use
     * the instances, and their field values should be retrieved. The fields
     * in the current fetch group must be retrieved, and the implementation
     * might retrieve more fields than the current fetch-group.
     * <P>The <code>PersistenceManager</code> might use policy information
     * about the class to retrieve associated instances.
     * @param pcs the instances
     * @param fgOnly whether to retrieve only the current fetch-group fields
     * @since JDO 1.0.1
     */
    public void retrieveAll(Object[] pcs, boolean fgOnly)
    {
        pm.retrieveAll(fgOnly, pcs);
    }

    /**
     * Retrieve field values of instances from the store.
     * @param fgOnly whether to retrieve only the current fetch-group fields
     * @param pcs the instances
     * @since JDO 2.1
     */
    public void retrieveAll(boolean fgOnly, Object... pcs)
    {
        pm.retrieveAll(fgOnly, pcs);
    }

    /** Retrieve field values of instances from the store.  This tells
     * the <code>PersistenceManager</code> that the application intends to use
     * the instances, and their field values should be retrieved. The fields
     * in the current fetch group must be retrieved, and the implementation
     * might retrieve more fields than the current fetch-group.
     * <P>The <code>PersistenceManager</code> might use policy information
     * about the class to retrieve associated instances.
     * @param pcs the instances
     * @param fgOnly whether to retrieve only the current fetch group fields
     * @since JDO 1.0.1
     */
    public void retrieveAll(Collection pcs, boolean fgOnly)
    {
        pm.retrieveAll(pcs, fgOnly);
    }

    /**
     * Accessor for the current transaction 
     * @return The current transaction
     **/
    public Transaction currentTransaction()
    {
        return pm.currentTransaction();
    }

    /**
     * Method to evict an object 
     * @param o Object to evict
     **/
    public void evict(Object o)
    {
        checkStatus();
        pm.evict(o);
    }

    /**
     * Method to evict an array of objects 
     * @param os Array of objects to evict
     **/
    public void evictAll(Object... os)
    {
        checkStatus();
        pm.evictAll(os);
    }

    /**
     * Method to evict all of the specified objects from the PM.
     * @param os Collection of objects to evict
     **/
    public void evictAll(Collection os)
    {
        checkStatus();
        pm.evictAll(os);
    }

    /**
     * Method to evict all objects of the specified type (and optionally its subclasses).
     * @param cls Type of persistable object
     * @param subclasses Whether to include subclasses
     */
    public synchronized void evictAll(Class cls, boolean subclasses)
    {
        checkStatus();
        pm.evictAll(subclasses, cls);
    }

    /**
     * Method to evict all objects of the specified type (and optionally its subclasses).
     * @param subclasses Whether to include subclasses
     * @param cls Type of persistable object
     */
    public synchronized void evictAll(boolean subclasses, Class cls)
    {
        checkStatus();
        pm.evictAll(subclasses, cls);
    }

    /**
     * Method to evict all objects from the PM.
     **/
    public void evictAll()
    {
       checkStatus();
       pm.evictAll();
    }

    /**
     * Method to refresh an array of objects.
     * @param os Array of objects to refresh
     **/
    public void refreshAll(Object... os)
    {
        checkStatus();
        pm.refreshAll(os);
    }

    /**
     * Method to refresh a collection of objects.
     * @param os Collection of objects to refresh
     */
    public void refreshAll(Collection os)
    {
        checkStatus();
        pm.refreshAll(os);
    }

    /**
     * Method to refresh objects that failed verification in a JDOException
     * @param exc Exception containing objects that failed verification
     */
    public void refreshAll(JDOException exc)
    {
        checkStatus();
        pm.refreshAll(exc);
    }

    /**
     * Method to refresh all objects in the Persistence Manager.
     */
    public void refreshAll()
    {
        checkStatus();
        pm.refreshAll();
    }

    /**
     * Accessor for a new query.
     * @return The query.
     */
    public Query newQuery()
    {
        checkStatus();
        return pm.newQuery();
    }

    /**
     * Accessor for a new query.
     * @param obj object
     * @return The new query
     */
    public Query newQuery(Object obj)
    {
        checkStatus();
        return pm.newQuery(obj);
    }

    /**
     * Accessor for a single-string query.
     * @param query The single-string query
     * @return The Query
     */
    public Query newQuery(String query)
    {
        checkStatus();
        return pm.newQuery(query);
    }

    /**
     * Accessor for a new query in the specified query language etc.
     * @param language The query language
     * @param query The query definition
     * @return The new query
     */
    public Query newQuery(String language, Object query)
    {
        checkStatus();
        return pm.newQuery(language, query);
    }

    /**
     * Accessor for a new query using this candidate class.
     * @param cls The candidate class to use.
     * @return The new query
     */
    public Query newQuery(Class cls)
    {
        checkStatus();
        return pm.newQuery(cls);
    }

    /**
     * Accessor for a new query based on the provided extent.
     * @param ext The extent to use
     * @return The new query
     */
    public Query newQuery(Extent ext)
    {
        checkStatus();
        return pm.newQuery(ext);
    }

    /**
     * Accessor for a new query.
     * @param cls The candidate class.
     * @param cln The collection to use.
     * @return The new query
     */
    public Query newQuery(Class cls, Collection cln)
    {
        checkStatus();
        return pm.newQuery(cls, cln);
    }

    /**
     * Accessor for a new query.
     * @param cls The candidate class
     * @param filter The filter to use
     * @return The new query
     */
    public Query newQuery(Class cls, String filter)
    {
        checkStatus();
        return pm.newQuery(cls, filter);
    }

    /**
     * Accessor for a new query.
     * @param cls The candidate class
     * @param cln The collection
     * @param filter The filter to use
     * @return The new query
     */
    public Query newQuery(Class cls, Collection cln, String filter)
    {
        checkStatus();
        return pm.newQuery(cls, cln, filter);
    }

    /**
     * Accessor for a new query.
     * @param cln The collection
     * @param filter The filter to use
     * @return The new query
     */
    public Query newQuery(Extent cln, String filter)
    {
        checkStatus();
        return pm.newQuery(cln, filter);
    }

    /**
     * Accessor for a new named query.
     * @param cls The candidate class
     * @param queryName The name of the query
     * @return The new query
     */
    public Query newNamedQuery(Class cls, String queryName)
    {
        checkStatus();
        return pm.newNamedQuery(cls, queryName);
    }

    /**
     * Accessor for the extent of a candidate class.
     * @param cls The candidate class
     * @param subclasses Whether to include subclasses
     * @return The extent for these classes
     */
    public Extent getExtent(Class cls, boolean subclasses)
    {
        checkStatus();
        return pm.getExtent(cls, subclasses);
    }

    /**
     * Accessor for the extent of a candidate class.
     * @param cls The candidate class
     * @return The extent for these classes
     */
    public Extent getExtent(Class cls)
    {
        checkStatus();
        return pm.getExtent(cls);
    }
    
    /**
     * Accessor for the current FetchPlan
     * @return FetchPlan
     */
    public FetchPlan getFetchPlan()
    {
        //TODO this should be enabled, but with detachAllOnCommit=true it raises exception 
        //checkStatus();
        return pm.getFetchPlan();
    }

    /**
     * Accessor for an object given its id.
     * @param id Id of the object.
     * @return The object with this id
     */
    public Object getObjectById(Object id)
    {
        return getObjectById(id, true);
    }

    /**
     * Accessor for an object given its id.
     * @param id Id of the object.
     * @param validate Whether to validate the object before returning it
     * @return The object with this id
     */
    public Object getObjectById(Object id, boolean validate)
    {
        checkStatus();
        return pm.getObjectById(id, validate);
    }

    public Collection getObjectsById (Collection oids)
    {
        checkStatus();
        return pm.getObjectsById(oids);
    }

    public Object[] getObjectsById (Object... oids)
    {
        checkStatus();
        return pm.getObjectsById(oids);
    }

    public Collection getObjectsById (Collection oids, boolean validate)
    {
        checkStatus();
        return pm.getObjectsById(oids, validate);
    }

    public Object[] getObjectsById (Object[] oids, boolean validate)
    {
        checkStatus();
        return pm.getObjectsById(validate, oids);
    }

    public Object[] getObjectsById (boolean validate, Object... oids)
    {
        checkStatus();
        return pm.getObjectsById(validate, oids);
    }

    public Object getObjectById (Class cls, Object key)
    {
        checkStatus();
        return pm.getObjectById(cls, key);
    }

    public Object newObjectIdInstance (Class pcClass, Object key)
    {
        checkStatus();
        return pm.newObjectIdInstance(pcClass, key);
    }

    /**
     * Accessor for the id of an object.
     * @param pc The object
     * @return The objects id
     */
    public Object getObjectId(Object pc)
    {
        checkStatus();
        return pm.getObjectId(pc);
    }

    /**
     *
     * @param pc <description>
     * @return <description>
     */
    public Object getTransactionalObjectId(Object pc)
    {
        checkStatus();
        return pm.getTransactionalObjectId(pc);
    }

    /**
     *
     * @param clazz <description>
     * @param str <description>
     * @return <description>
     */
    public Object newObjectIdInstance(Class clazz, String str)
    {
        checkStatus();
        return pm.newObjectIdInstance(clazz, str);
    }

    /**
     * Method to create an instance of an interface or abstract class
     * @param persistenceCapable interface/abstract class declared in metadata
     * @return Instance of the interface / abstract class
     */
    public Object newInstance(Class persistenceCapable)
    {
        checkStatus();
        return pm.newInstance(persistenceCapable);
    }

    /**
     * Method to make an object persistent.
     * @param pc The object to persist
     */
    public Object makePersistent(Object pc)
    {
        checkStatus();
        return pm.makePersistent(pc);
    }

    /**
     * Make an array of Persistent Capable objects persistent 
     * @param pcs Array of Persistent Capable objects
     */
    public Object[] makePersistentAll(Object... pcs)
    {
        checkStatus();
        return pm.makePersistentAll(pcs);
    }

    /**
     * Make a Collection of Persistence Capable objects persistent 
     * @param pcs Collection of Persistence Capable objects
     */
    public Collection makePersistentAll(Collection pcs)
    {
        checkStatus();
        return pm.makePersistentAll(pcs);
    }

    /**
     * JDO method to delete a Persistence Capable object 
     * @param pc Persistence Capable object
     */
    public void deletePersistent(Object pc)
    {
        checkStatus();
        pm.deletePersistent(pc);
    }

    /**
     * JDO method to delete an array of Persistence Capable objects 
     * @param pcs Array of Persistence Capable objects
     */
    public void deletePersistentAll(Object... pcs)
    {
        checkStatus();
        pm.deletePersistentAll(pcs);
    }

    /**
     * JDO method to delete a Collection of Persistence Capable objects 
     * @param pcs Collection of Persistence Capable objects
     */
    public void deletePersistentAll(Collection pcs)
    {
        checkStatus();
        pm.deletePersistentAll(pcs);
    }

    /**
     * Make a Persistence-Capable object transient , optionally using the fetch plan.
     * @param pc Persistence-Capable object
     * @param useFetchPlan Whether to use the fetch plan
     */
    public void makeTransient(Object pc, boolean useFetchPlan)
    {
        checkStatus();
        pm.makeTransient(pc, useFetchPlan);
    }

    /**
     * Make an array of Persistence-Capable objects transient 
     * @param pcs Array of Persistence-Capable objects
     * @param useFetchPlan Whether to use the fetch plan
     */
    public void makeTransientAll(Object[] pcs, boolean useFetchPlan)
    {
        checkStatus();
        pm.makeTransientAll(useFetchPlan, pcs);
    }

    /**
     * Make an array of Persistence-Capable objects transient 
     * @param useFetchPlan Whether to use the fetch plan
     * @param pcs Array of Persistence-Capable objects
     * @since JDO 2.1
     */
    public void makeTransientAll(boolean useFetchPlan, Object... pcs)
    {
        checkStatus();
        pm.makeTransientAll(useFetchPlan, pcs);
    }

    /**
     * Make a Collection of Persistence-Capable objects transient 
     * @param pcs Collection of Persistence-Capable objects
     * @param useFetchPlan Whether to use the fetch plan
     */
    public void makeTransientAll(Collection pcs, boolean useFetchPlan)
    {
        checkStatus();
        pm.makeTransientAll(pcs, useFetchPlan);
    }

    /**
     * Make a Persistence-Capable object transient 
     * @param pc Persistence-Capable object
     */
    public void makeTransient(Object pc)
    {
        checkStatus();
        pm.makeTransient(pc);
    }

    /**
     * Make an array of Persistence-Capable objects transient 
     * @param pcs Array of Persistence-Capable objects
     */
    public void makeTransientAll(Object... pcs)
    {
        checkStatus();
        pm.makeTransientAll(pcs);
    }

    /**
     * Make a Collection of Persistence-Capable objects transient 
     * @param pcs Collection of Persistence-Capable objects
     */
    public void makeTransientAll(Collection pcs)
    {
        checkStatus();
        pm.makeTransientAll(pcs);
    }

    /**
     * Make a Persistence-Capable object transient 
     * @param pc Persistence-Capable object
     */
    public void makeTransactional(Object pc)
    {
        checkStatus();
        pm.makeTransactional(pc);
    }

    /**
     * Make an array of Persistence-Capable objects transactional 
     * @param pcs Array of Persistence-Capable objects
     */
    public void makeTransactionalAll(Object... pcs)
    {
        checkStatus();
        pm.makeTransactionalAll(pcs);
    }

    /**
     * Make a collection of PersistenceCapable objects transactional
     * @param pcs Collection of Persistence-Capable objects
     */
    public void makeTransactionalAll(Collection pcs)
    {
        checkStatus();
        pm.makeTransactionalAll(pcs);
    }

    /**
     * Make a Persistence-Capable object non-transactional 
     * @param pc Persistence-Capable object
     */
    public void makeNontransactional(Object pc)
    {
        checkStatus();
        pm.makeNontransactional(pc);
    }

    /**
     * Make an array of Persistence-Capable objects non-transactional 
     * @param pcs Array of Persistence-Capable objects
     */
    public void makeNontransactionalAll(Object... pcs)
    {
        checkStatus();
        pm.makeNontransactionalAll(pcs);
    }

    /**
     * Make a Collection of Persistence-Capable objects non-transactional 
     * @param pcs Collection of Persistence-Capable objects
     */
    public void makeNontransactionalAll(Collection pcs)
    {
        checkStatus();
        pm.makeNontransactionalAll(pcs);
    }

    /**
     * Detach the specified object from the <code>PersistenceManager</code>.
     * @param pc the instance to detach
     * @return the detached instance
     * @see #detachCopyAll(Object[])
     * @since JDO 2.0
     */
    public synchronized Object detachCopy(Object pc)
    {
        checkStatus();
        return pm.detachCopy(pc);
    }

    /**
     * Detach the specified objects from the
     * <code>PersistenceManager</code>. The objects returned can be
     * manipulated and re-attached with 
     * {@link #makePersistentAll(Object[])}. 
     * The detached instances will be
     * unmanaged copies of the specified parameters, and are suitable
     * for serialization and manipulation outside of a JDO
     * environment. When detaching instances, only fields in the
     * current {@link FetchPlan} will be traversed. Thus, to detach a
     * graph of objects, relations to other persistent instances must
     * either be in the <code>default-fetch-group</code>, or in the
     * current custom {@link FetchPlan}.
     * @param pcs the instances to detach
     * @return the detached instances
     * @throws javax.jdo.JDOUserException if any of the instances do not
     * @see #makePersistentAll(Object[])
     * @see #getFetchPlan
     */ 
    public synchronized Object[] detachCopyAll(Object... pcs)
    {
        checkStatus();
        return pm.detachCopyAll(pcs);
    }

    /**
     * Detach the specified objects from the <code>PersistenceManager</code>.
     * @param pcs the instances to detach
     * @return the detached instances
     * @see #detachCopyAll(Object[])
     */
    public synchronized Collection detachCopyAll(Collection pcs)
    {
        checkStatus();
        return pm.detachCopyAll(pcs);
    }

    /**
     * Method to put a user object into the PersistenceManager. This is so that
     * multiple users can each have a user object for example. <I>The parameter
     * is not inspected or used in any way by the JDO implementation. </I>
     * @param key The key to store the user object under
     * @param value The object to store
     * @return The previous value for this key
     * @since 1.1
     */
    public synchronized Object putUserObject(Object key, Object value)
    {
        checkStatus();
        return pm.putUserObject(key, value);
    }

    /**
     * Method to get a user object from the PersistenceManager. This is for user
     * objects which are stored under a key. <I>The parameter is not inspected
     * or used in any way by the JDO implementation. </I>
     * @param key The key to store the user object under
     * @return The user object for that key
     * @since 1.1
     */
    public synchronized Object getUserObject(Object key)
    {
        checkStatus();
        return pm.getUserObject(key);
    }

    /**
     * Method to remove a user object from the PersistenceManager. This is for
     * user objects which are stored under a key. <I>The parameter is not
     * inspected or used in any way by the JDO implementation. </I>
     * @param key The key whose user object is to be removed.
     * @return The user object that was removed
     * @since 1.1
     */
    public synchronized Object removeUserObject(Object key)
    {
        checkStatus();
        return pm.removeUserObject(key);
    }

    /**
     * The application might manage PersistenceManager instances by using an
     * associated object for bookkeeping purposes. These methods allow the user
     * to manage the associated object.
     * The parameter is not inspected or used in any way by the JDO
     * implementation.
     *
     * @param obj User Object
     */
    public void setUserObject(Object obj)
    {
        checkStatus();
        pm.setUserObject(obj);
    }

    /**
     * The application might manage PersistenceManager instances by using an
     * associated object for bookkeeping purposes. These methods allow the user
     * to manage the associated object.
     * The parameter is not inspected or used in any way by the JDO
     * implementation.*
     * 
     * @return User object
     */
    public Object getUserObject()
    {
        checkStatus();
        return pm.getUserObject();
    }

    /**
     * Retrieve the PersistenceManagerFactory for this manager 
     * @return The PersistenceManagerFactory
     */
    public PersistenceManagerFactory getPersistenceManagerFactory()
    {
        return this.pm.getPersistenceManagerFactory();
    }

    /**
     * Retrieve the class for the objectid 
     * @param clazz The class to retrieve
     * @return The Class of the ObjectId
     */
    public Class getObjectIdClass(Class clazz)
    {
        checkStatus();
        return pm.getObjectIdClass(clazz);
    }

    /**
     * Mutator for the multithreaded capability of the manager 
     * @param multithreaded Whether to run multithreaded or not
     */
    public void setMultithreaded(boolean multithreaded)
    {
        checkStatus();
        pm.setMultithreaded(multithreaded);
    }

    /**
     * Accessor for the multithreaded capability of the manager 
     * @return Whether the PersistenceManager is multithreaded or not
     */
    public boolean getMultithreaded()
    {
        return false;
    }

    /**
     * Mutator for whether to ignore the cache or not 
     * @param ignore Whether to ignore the cache or not
     */
    public void setIgnoreCache(boolean ignore)
    {
        checkStatus();
        pm.setIgnoreCache(ignore);
    }

    /**
     * Accessor for whether to ignore the cache or not 
     * @return Whether to ignore the cache or not
     */
    public boolean getIgnoreCache()
    {
        return false;
    }

    /**
     * This method flushes all dirty, new, and deleted instances to the datastore. It has no effect
     * if a transaction is not active.
     * If a datastore transaction is active, this method synchronizes the cache with the datastore
     * and reports any exceptions.
     * If an optimistic transaction is active, this method obtains a datastore connection and synchronizes
     * the cache with the datastore using this connection. The connection obtained by
     * this method is held until the end of the transaction.
     */
    public void flush()
    {
        checkStatus();
        pm.flush();
    }

    /**
     * Method to check the consistency of the cache.
     */
    public void checkConsistency()
    {
        checkStatus();
        pm.checkConsistency();
    }

    /**
     * (non-Javadoc)
     * @see javax.jdo.PersistenceManager#getDataStoreConnection()
     * @since 1.1
     */
    public JDOConnection getDataStoreConnection()
    {
        checkStatus();
        return pm.getDataStoreConnection();
    }

    /**
     * Accessor for a Sequence.
     * @param sequenceName Name of the sequence
     * @return The sequence
     */
    public Sequence getSequence(String sequenceName)
    {
        checkStatus();
        return pm.getSequence(sequenceName);
    }

    /**
     * JDO 2.0 spec 12.15 "LifecycleListeners".
     * @param listener The instance lifecycle listener to sends events to
     * @param classes The classes that it is interested in
     * @since 1.1
     */
    public void addInstanceLifecycleListener(InstanceLifecycleListener listener, Class... classes)
    {
        checkStatus();
        pm.addInstanceLifecycleListener(listener, classes);
    }
    
    /**
     * JDO 2.0 spec 12.15 "LifecycleListeners".
     * @param listener The instance lifecycle listener to remove.
     * @since 1.1
     */
    public void removeInstanceLifecycleListener(InstanceLifecycleListener listener)
    {
        checkStatus();
        pm.removeInstanceLifecycleListener(listener);
    }

    /**
     * Convenience method to check the status of the adapter.
     */
    private void checkStatus()
    {
        if (closed || pm == null) 
        {
            throw new JDOException("Invalid state, closed or no mc");
        }
    }

    /**
     * Accessor for the server date/time.
     * @return The server date/time
     */
    public Date getServerDate()
    {
        return pm.getServerDate();
    }

    public boolean getCopyOnAttach()
    {
        return pm.getCopyOnAttach();
    }

    public void setCopyOnAttach(boolean flag)
    {
        pm.setCopyOnAttach(flag);
    }

    public Set getManagedObjects()
    {
        return pm.getManagedObjects();
    }

    public Set getManagedObjects(Class... classes)
    {
        return pm.getManagedObjects(classes);
    }

    public Set getManagedObjects(EnumSet states)
    {
        return pm.getManagedObjects(states);
    }

    public Set getManagedObjects(EnumSet states, Class... classes)
    {
        return pm.getManagedObjects(states, classes);
    }

    /* (non-Javadoc)
     * @see javax.jdo.PersistenceManager#getFetchGroup(java.lang.Class, java.lang.String)
     */
    public FetchGroup getFetchGroup(Class cls, String name)
    {
        return pm.getFetchGroup(cls, name);
    }

    /* (non-Javadoc)
     * @see javax.jdo.PersistenceManager#getDatastoreReadTimeoutMillis()
     */
    public Integer getDatastoreReadTimeoutMillis()
    {
        return pm.getDatastoreReadTimeoutMillis();
    }

    /* (non-Javadoc)
     * @see javax.jdo.PersistenceManager#setDatastoreReadTimeoutMillis(java.lang.Integer)
     */
    public void setDatastoreReadTimeoutMillis(Integer intvl)
    {
        pm.setDatastoreReadTimeoutMillis(intvl);
    }

    /* (non-Javadoc)
     * @see javax.jdo.PersistenceManager#getDatastoreWriteTimeoutMillis()
     */
    public Integer getDatastoreWriteTimeoutMillis()
    {
        return pm.getDatastoreWriteTimeoutMillis();
    }

    /* (non-Javadoc)
     * @see javax.jdo.PersistenceManager#setDatastoreWriteTimeoutMillis(java.lang.Integer)
     */
    public void setDatastoreWriteTimeoutMillis(Integer intvl)
    {
        pm.setDatastoreWriteTimeoutMillis(intvl);
    }

    /* (non-Javadoc)
     * @see javax.jdo.PersistenceManager#getProperties()
     */
    public Map<String, Object> getProperties()
    {
        return pm.getProperties();
    }

    /* (non-Javadoc)
     * @see javax.jdo.PersistenceManager#getSupportedProperties()
     */
    public Set<String> getSupportedProperties()
    {
        return pm.getSupportedProperties();
    }

    /* (non-Javadoc)
     * @see javax.jdo.PersistenceManager#setProperty(java.lang.String, java.lang.Object)
     */
    public void setProperty(String propertyName, Object value)
    {
        pm.setProperty(propertyName, value);
    }
}