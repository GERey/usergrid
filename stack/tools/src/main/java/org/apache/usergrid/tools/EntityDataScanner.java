/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.usergrid.tools;


import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import org.apache.usergrid.corepersistence.CpEntityManagerFactory;
import org.apache.usergrid.persistence.EntityManager;
import org.apache.usergrid.persistence.EntityRef;
import org.apache.usergrid.persistence.Query;
import org.apache.usergrid.persistence.collection.serialization.impl.EntityVersion;
import org.apache.usergrid.persistence.collection.serialization.impl.EntityVersionSerializer;
import org.apache.usergrid.persistence.collection.serialization.impl.TypeField;
import org.apache.usergrid.persistence.collection.serialization.impl.UniqueTypeFieldRowKeySerializer;
import org.apache.usergrid.persistence.core.astyanax.MultiTenantColumnFamily;
import org.apache.usergrid.persistence.core.astyanax.ScopedRowKey;
import org.apache.usergrid.persistence.core.astyanax.ScopedRowKeySerializer;


public class EntityDataScanner extends ToolBase {

    /**
     *
     */
    private static final int PAGE_SIZE = 100;


    private static final Logger logger = LoggerFactory.getLogger( UniqueValueScanner.class );

    private static final String ORG_ARG = "org";

    private static final String APPLICATION_ARG = "app";

    private static final String COLLECTION_ARG = "col";

    private static final String COLUMN_ARG = "cf";


    private static final String ENTITY_UNIQUE_PROPERTY_NAME = "property";

    private static final String ENTITY_UNIQUE_PROPERTY_VALUE = "value";

    //copied shamelessly from unique value serialization strat.
    private static final ScopedRowKeySerializer<TypeField> ROW_KEY_SER =
        new ScopedRowKeySerializer<>( UniqueTypeFieldRowKeySerializer.get() );


    final EntityVersionSerializer ENTITY_VERSION_SER = new EntityVersionSerializer();

    final MultiTenantColumnFamily<ScopedRowKey<TypeField>, EntityVersion> CF_UNIQUE_VALUES =
        new MultiTenantColumnFamily<>( "Unique_Values_V2", ROW_KEY_SER, ENTITY_VERSION_SER );

    @Override
    @SuppressWarnings( "static-access" )
    public Options createOptions() {


        Options options = super.createOptions();


        Option orgOption = OptionBuilder.withArgName( ORG_ARG ).hasArg().isRequired( true )
                                        .withDescription( "org_app_name" ).create( ORG_ARG );

        options.addOption( orgOption );


        Option appOption = OptionBuilder.withArgName( APPLICATION_ARG ).hasArg().isRequired( true )
                                        .withDescription( "application id" ).create( APPLICATION_ARG );


        options.addOption( appOption );

        Option collectionOption =
            OptionBuilder.withArgName( COLLECTION_ARG ).hasArg().isRequired( true ).withDescription( "collection name" )
                         .create( COLLECTION_ARG );

        options.addOption( collectionOption );


        return options;
    }


    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.usergrid.tools.ToolBase#runTool(org.apache.commons.cli.CommandLine)
     */
    @Override
    public void runTool( CommandLine line ) throws Exception {
        startSpring();

        logger.info( "Starting entity unique index cleanup" );


        UUID appUuid = UUID.fromString( line.getOptionValue(APPLICATION_ARG ));
        CpEntityManagerFactory cpEntityManagerFactory = (CpEntityManagerFactory)emf;
        EntityManager cpEntityManager = cpEntityManagerFactory.getEntityManager( appUuid );

        if(cpEntityManager!=null){
            logger.trace("CpEntityManager is not null contains."  );
        }
        Set<String> keysetAppNames = emf.getApplications().keySet();
        for ( String appName: keysetAppNames ) {
            logger.trace( "Contains this app: "+appName );
        }

        EntityRef entityRefApp = cpEntityManager.getAlias( "application",line.getOptionValue( ORG_ARG ) );


        Iterator<org.apache.usergrid.persistence.Entity>
            iterator = cpEntityManager.getCollection( entityRefApp,line.getOptionValue( COLLECTION_ARG ),null,1000,
            Query.Level.ALL_PROPERTIES,false ).iterator();

        while(iterator.hasNext()){
            org.apache.usergrid.persistence.Entity entity = iterator.next();
            EntityRef entityRef = cpEntityManager.getAlias( entity.getType(),entity.getName() );
            if(entityRef == null){
                logger.error( "Unique Value: " + entity.getName() +". Doesn't exist under: "+entity.getUuid() );
            }
            else{
                logger.info( "Unique Value: " + entity.getName() +". Exists under: "+entity.getUuid() );
            }

        }

        logger.info( "Completed repair successfully" );
    }
}
