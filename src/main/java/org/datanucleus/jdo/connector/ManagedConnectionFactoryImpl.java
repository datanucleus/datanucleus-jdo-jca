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
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import javax.jdo.Constants;
import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;
import javax.resource.spi.security.PasswordCredential;
import javax.security.auth.Subject;

import org.datanucleus.PropertyNames;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.TransactionType;

/**
 * Implementation of the JCA adapter for use in J2EE environments.
 * Provides an implementation of the methods for ManagedConnectionFactory as
 * well as setters/getters for properties required in the J2EE environment.
 */
public class ManagedConnectionFactoryImpl implements ManagedConnectionFactory
{
    private static final long serialVersionUID = 318962833424682326L;
    JDOPersistenceManagerFactory pmf;
    Properties properties = new Properties();
    boolean configurable = true;

    public ManagedConnectionFactoryImpl() 
    {
    }

    /**
     * Freezes the current configuration. Executed only once
     * @throws NucleusException if the configuration was invalid or inconsistent in some way
     */
    protected void freezeConfiguration()
    {
        if (!configurable)
        {
        	return;
        }

        pmf = new JDOPersistenceManagerFactory(properties);
    	pmf.setTransactionType(TransactionType.JTA.toString()); // J2EE : default to JTA transactions
        pmf.getNucleusContext().setJcaMode(true); // J2EE : set that using JCA connector

        configurable = false;
    }

    // ----------------------- Setters/Getters for JCA -------------------------

    /**
     * Mutator for the name of the persistence unit.
     * @param name Name of the persistence unit
     */
    public synchronized void setPersistenceUnitName(String name)
    {
        properties.put(Constants.PROPERTY_PERSISTENCE_UNIT_NAME, name);
    }

    /**
     * Mutator for the filename of the persistence.xml file.
     * This is for the case where an application has placed the persistence.xml somewhere else maybe
     * outside the CLASSPATH.
     * @param name Filename of the persistence unit
     */
    public synchronized void setPersistenceXmlFilename(String name)
    {
    	properties.put(PropertyNames.PROPERTY_PERSISTENCE_XML_FILENAME, name);
    }

    /**
     * Setter for the primaryClassLoader
     * @param name Name of the class to use to set the primary class loader. Otherwise uses this class
     */
    public synchronized void setPrimaryClassLoader(String name)
    {
        ClassLoader thisClazzLoader = this.getClass().getClassLoader();
        try
        {
            ClassLoader primaryClassLoader = thisClazzLoader.loadClass(name).getClassLoader();
            properties.put(PropertyNames.PROPERTY_CLASSLOADER_PRIMARY, primaryClassLoader);
        }
        catch (Exception e)
        {
            // ignore this error
            if (PersistenceManagerImpl.LOGGER.isInfoEnabled())
            {
                PersistenceManagerImpl.LOGGER.info("Exception while creating PrimaryClassLoader: ", e);
            }
        }
    }

    // --------------- Implementation of ManagedConnectionFactory --------------
 

    public JDOPersistenceManagerFactory getPersistenceManagerFactory()
    {
		return pmf;
	}
    
    /**
     * Equality operator.
     * @param other The object to compare with
     * @return Whether the objects are equal
     */
    public boolean equals(Object other)
    {
        if (this == other) 
        {
            return true;
        }
      
        if (!(other instanceof ManagedConnectionFactoryImpl)) 
        {
             return false;
        }
        return super.equals(other);
    }
    
    public int hashCode() 
    {
    	return super.hashCode();
    }

    /**
     * Creator for the connection factory
     * @return The connection factory
     * @exception javax.resource.ResourceException Thrown if an error occurs
     */
    public Object createConnectionFactory()
    throws ResourceException
    {
        throw new ResourceException("Not Yet Implemented");
    }

    /**
     * Creator for the connection factory.
     * @param cm ConnectionManager
     * @return The connection factory.
     * @exception javax.resource.ResourceException Thrown if an error occurs
     */
    public Object createConnectionFactory(ConnectionManager cm) 
    throws ResourceException
    {
        freezeConfiguration();
        return new PersistenceManagerFactoryImpl(this, cm);
    }

    /**
     * Creator for a managed connection.
     * @param subject The subject (what ?)
     * @param cri Connection request info.
     * @return The managed connection.
     * @exception javax.resource.ResourceException Thrown if an error occurs
     */
    public ManagedConnection createManagedConnection(Subject subject, ConnectionRequestInfo cri)
    throws ResourceException
    {
        freezeConfiguration();
        PasswordCredential pc = getPasswordCredential(subject);
        ManagedConnectionImpl mc = new ManagedConnectionImpl(this,pc);
        return mc;
    }

    /**
     * Method to match managed connections.
     * The application server invokes it when the user asks for a PersistenceManager
     * In application servers such as WebLogic 9.x, 10.x, you can use the setting match-connections-supported=true
     * to return the same ManagedConnection instance if the user asks multiple times for a PersistenceManager (PMF.getPersistenceManager()) 
     * inside a single transaction. For JBoss 4.x the setting is track-connection-by-tx.
     * 
     * @param mcs managed connections
     * @param subject The subject
     * @param cri request info
     * @return The managed connection that matches
     * @exception javax.resource.ResourceException Thrown if an error occurs
     * 
     */
    public ManagedConnection matchManagedConnections(Set mcs, Subject subject, ConnectionRequestInfo cri) 
    throws ResourceException
    {
        PasswordCredential pc = getPasswordCredential(subject);
        for (Iterator i = mcs.iterator(); i.hasNext();)
        {
            Object o = i.next();
            if (!(o instanceof ManagedConnectionImpl))
            {
                continue;
            }

            ManagedConnectionImpl mc = (ManagedConnectionImpl)o;
            if (!(mc.getManagedConnectionFactory().equals(this)))
            {
                continue;
            }
            if (pc == null && mc.getPasswordCredential() == null)
            {
                return mc;
            }
            if( pc != null && mc.getPasswordCredential() != null && pc.equals(mc.getPasswordCredential())) 
            {
                return mc;
            }
        }
        return null;
    }

    /**
     * Accessor for the Log writer
     * @return The Log Writer
     * @exception javax.resource.ResourceException Thrown if an error occurs.
     */
    public PrintWriter getLogWriter()
    throws ResourceException
    {
        return null;
    }

    /**
     * Mutator for the Log Writer
     * @param writer The log writer
     * @exception javax.resource.ResourceException Thrown if an error occurs
     */
    public void setLogWriter(PrintWriter writer)
    throws ResourceException
    {
    }

    /**
     * Accessor for the Password credentials
     * @param subject The subject.
     * @return The password credential
     * @throws javax.resource.ResourceException Thrown if an error occurs.
     */
    PasswordCredential getPasswordCredential(Subject subject) 
    throws ResourceException
    {
        if (subject == null) 
        {
            if (!properties.containsKey(Constants.PROPERTY_CONNECTION_USER_NAME) ||
            	!properties.containsKey(Constants.PROPERTY_CONNECTION_PASSWORD))
            {
                return null;
            }
            PasswordCredential pc=new PasswordCredential(properties.getProperty("javax.jdo.option.ConnectionUserName"), properties.getProperty("javax.jdo.option.ConnectionPassword").toCharArray());
            pc.setManagedConnectionFactory(this);
            return pc;
        }

        for (Iterator i=subject.getPrivateCredentials().iterator();i.hasNext();)
        {
            Object o = i.next();
            if (o instanceof PasswordCredential) 
            {
                PasswordCredential pc = (PasswordCredential)o;
                if (this.equals(pc.getManagedConnectionFactory())) 
                {
                    return pc;
                }
            }
        }
        throw new ResourceException("No credentials found for ManagedConnectionFactory: " + this);
    }
}