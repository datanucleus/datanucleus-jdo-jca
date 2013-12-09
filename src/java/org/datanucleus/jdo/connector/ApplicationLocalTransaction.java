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
2004 Andy Jefferson - addition of rollbackOnly
    ...
**********************************************************************/
package org.datanucleus.jdo.connector;

import javax.resource.ResourceException;
import javax.resource.cci.LocalTransaction;

/**
 * Application demarcated local transaction
 * This interface is used for application level local transaction demarcation.
 * One can use the javax.jdo or the javax.resource.cci.LocalTransaction APIs,
 */
public class ApplicationLocalTransaction implements LocalTransaction
{
    private final PersistenceManagerImpl pm;

    /**
     * Constructor
     * @param pm the PersistenceManager
     */
    public ApplicationLocalTransaction(PersistenceManagerImpl pm) 
    {
        this.pm = pm;
    }

	public void begin() throws ResourceException
	{
		pm.currentTransaction().begin();
	}

	public void commit() throws ResourceException
	{
		pm.currentTransaction().commit();
	}

	public void rollback() throws ResourceException
	{
		pm.currentTransaction().rollback();
	}


}