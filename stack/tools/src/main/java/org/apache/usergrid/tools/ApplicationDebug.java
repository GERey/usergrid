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


import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import org.apache.usergrid.corepersistence.CpEntityManagerFactory;
import org.apache.usergrid.management.OrganizationInfo;
import org.apache.usergrid.persistence.Entity;
import org.apache.usergrid.persistence.EntityManager;
import org.apache.usergrid.persistence.EntityRef;
import org.apache.usergrid.persistence.SimpleEntityRef;
import org.apache.usergrid.persistence.collection.serialization.impl.EntityVersion;
import org.apache.usergrid.persistence.collection.serialization.impl.EntityVersionSerializer;
import org.apache.usergrid.persistence.collection.serialization.impl.TypeField;
import org.apache.usergrid.persistence.collection.serialization.impl.UniqueTypeFieldRowKeySerializer;
import org.apache.usergrid.persistence.core.astyanax.MultiTenantColumnFamily;
import org.apache.usergrid.persistence.core.astyanax.ScopedRowKey;
import org.apache.usergrid.persistence.core.astyanax.ScopedRowKeySerializer;

import com.google.common.collect.BiMap;


public class ApplicationDebug extends ToolBase {

    /**
     *
     */
    private static final int PAGE_SIZE = 100;


    private static final Logger logger = LoggerFactory.getLogger( UniqueValueScanner.class );

    private static final String ORG_ARG = "org";

    private static final String APPLICATION_ARG = "app";

    private static final String APPLICATIONID_ARG = "appID";

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
                                        .withDescription( "org name" ).create( ORG_ARG );

        options.addOption( orgOption );


        Option appOption = OptionBuilder.withArgName( APPLICATION_ARG ).hasArg().isRequired( true )
                                        .withDescription( "application" ).create( APPLICATION_ARG );


        options.addOption( appOption );

        Option appIdOption = OptionBuilder.withArgName( APPLICATIONID_ARG ).hasArg().isRequired( true )
                                        .withDescription( "application id" ).create( APPLICATIONID_ARG );


        options.addOption( appIdOption );

        Option entityName = OptionBuilder.withArgName( ENTITY_UNIQUE_PROPERTY_NAME ).hasArg().isRequired( false )
                                          .withDescription( "entity type" ).create( ENTITY_UNIQUE_PROPERTY_NAME );


        options.addOption( entityName );

        Option entityVal = OptionBuilder.withArgName( ENTITY_UNIQUE_PROPERTY_VALUE ).hasArg().isRequired( false )
                                          .withDescription( "entity uuid" ).create( ENTITY_UNIQUE_PROPERTY_VALUE );


        options.addOption( entityVal );

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


        UUID appUuid = UUID.fromString( line.getOptionValue(APPLICATIONID_ARG ));
        CpEntityManagerFactory cpEntityManagerFactory = (CpEntityManagerFactory)emf;
        EntityManager cpEntityManager = cpEntityManagerFactory.getEntityManager( appUuid );

        if(cpEntityManager!=null){
            logger.trace("CpEntityManager is not null contains: "+cpEntityManager.toString()  );
        }
        try {
            Set<String> keysetAppNames = emf.getApplications().keySet();
            for ( String appName: keysetAppNames ) {
                logger.trace( "Contains this app: " + appName );
            }

        }catch(Exception e){
            logger.error( e.getMessage() );
            logger.trace( "the app cannot be retrieved due to above issue. " );
        }

        OrganizationInfo organizationInfo = managementService.getOrganizationByName( line.getOptionValue( ORG_ARG ) );

        BiMap<UUID,String> biMap = managementService.getApplicationsForOrganization( organizationInfo.getUuid() );

        Set<String> values  = biMap.values();

        for(String applicationNamesForOrg : values){
            logger.trace( "Contains this app: " + applicationNamesForOrg );
        }

        if(line.hasOption( ENTITY_UNIQUE_PROPERTY_NAME ) && line.hasOption( ENTITY_UNIQUE_PROPERTY_VALUE )) {
            EntityRef entityRef = new SimpleEntityRef( line.getOptionValue( ENTITY_UNIQUE_PROPERTY_NAME ), UUID.fromString(line
                .getOptionValue( ENTITY_UNIQUE_PROPERTY_VALUE ) ) );

            Entity entity = cpEntityManager.get( entityRef );
            if(entity == null){
                logger.trace("the entity is still null");
            }
            else{
                logger.trace("it worked?");
            }
        }

    }
}
