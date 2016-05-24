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


import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.thrift.TBaseHelper;

import org.apache.usergrid.corepersistence.CpEntityManagerFactory;
import org.apache.usergrid.persistence.Entity;
import org.apache.usergrid.persistence.EntityManager;
import org.apache.usergrid.persistence.collection.serialization.impl.EntityVersion;
import org.apache.usergrid.persistence.collection.serialization.impl.EntityVersionSerializer;
import org.apache.usergrid.persistence.collection.serialization.impl.TypeField;
import org.apache.usergrid.persistence.collection.serialization.impl.UniqueTypeFieldRowKeySerializer;
import org.apache.usergrid.persistence.core.astyanax.MultiTenantColumnFamily;
import org.apache.usergrid.persistence.core.astyanax.ScopedRowKey;
import org.apache.usergrid.persistence.core.astyanax.ScopedRowKeySerializer;
import org.apache.usergrid.utils.UUIDUtils;

import me.prettyprint.cassandra.service.RangeSlicesIterator;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

import static org.apache.usergrid.persistence.cassandra.Serializers.be;


public class UniqueValueScanner extends ToolBase {

    /**
     *
     */
    private static final int PAGE_SIZE = 100;


    private static final Logger logger = LoggerFactory.getLogger( UniqueValueScanner.class );

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


        Option appOption = OptionBuilder.withArgName( APPLICATION_ARG ).hasArg().isRequired( false )
                                        .withDescription( "application id" ).create( APPLICATION_ARG );


        options.addOption( appOption );

        Option collectionOption =
            OptionBuilder.withArgName( COLLECTION_ARG ).hasArg().isRequired( true ).withDescription( "collection name" )
                         .create( COLLECTION_ARG );

        options.addOption( collectionOption );

        Option columnFamilyOption =
            OptionBuilder.withArgName( COLUMN_ARG ).hasArg().isRequired( true ).withDescription( "column family" )
                         .create( COLUMN_ARG );

        options.addOption( columnFamilyOption );

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


        // go through each collection and audit the values
        Keyspace ko = cass.getUsergridApplicationKeyspace();
        //find a better solution to thisColu
        ArrayList<Row> rows = new ArrayList<>(  );

        String applicationKeyspace = line.getOptionValue( COLUMN_ARG );

        //maybe put a byte buffer infront.
        RangeSlicesQuery<ByteBuffer, ByteBuffer, ByteBuffer> rangeSlicesQuery =
            HFactory.createRangeSlicesQuery( ko, be, be, be ).setColumnFamily( applicationKeyspace )
                    //not sure if I trust the lower two settings as it might iterfere with paging or set
                    // arbitrary limits and what I want to retrieve.
                    //That needs to be verified.
                    .setKeys( null, null ).setRange( null, null, false, PAGE_SIZE );


        RangeSlicesIterator rangeSlicesIterator = new RangeSlicesIterator( rangeSlicesQuery, null, null );

        int i = 0;
        //can't be in the same loop as the serializer or it'll infinite loop.
        while(rangeSlicesIterator.hasNext()) {
            Row verificationRow = rangeSlicesIterator.next();


           ByteBuffer buf = ( TBaseHelper.rightSize( ( ByteBuffer ) verificationRow.getKey() ) );

            String returnedRowKey = new String( buf.array(), buf.arrayOffset() + buf.position(), buf.remaining(),
                       Charset.defaultCharset() ).trim();

            logger.trace( returnedRowKey );

            if( returnedRowKey.contains( line.getOptionValue( COLLECTION_ARG )) &&
                returnedRowKey.contains( "name" )){
                rows.add( verificationRow );
                logger.trace( "Found a value "+(i++) );
               // rows[i++] = verificationRow;
            }
        }



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
        //logger.trace( emf.getApplications().keySet() );
//        logger.trace(  );
//        logger.trace(  );
//        logger.trace(  );
//        logger.trace(  );
//        logger.trace(  );




        for(int j = 0; j< rows.size(); j++) {
            ScopedRowKey scopedRowKey = ROW_KEY_SER.fromByteBuffer( ( ByteBuffer ) rows.get( j ).getKey() );
            if(!scopedRowKey.getScope().getUuid().equals( appUuid )){
                continue;
            }
            String name = ( String ) ( ( TypeField ) scopedRowKey.getKey() ).getField().getValue();
            //now here you can go check to see what the logic is for checking entity_version_data

            EntityVersion entityVersion = ENTITY_VERSION_SER.fromByteBuffer( ((HColumn)rows.get(j).getColumnSlice().getColumns().get( 0 )).getNameBytes() );

            UUID entityUUID = entityVersion.getEntityId().getUuid();
            Entity entity = cpEntityManager.get( entityUUID);
            if(entity == null){
                logger.error( "Unique Value: " + name +". Doesn't exist under: "+entityUUID );
            }
            else{
                logger.info("Entity looks fine: "+name);
            }

        }

        logger.info( "Completed repair successfully" );
    }

    //Returns a functioning rowkey if it can otherwise returns null
    public String[] garbageRowKeyParser( String[] parsedRowKey ) {
        String[] modifiedRowKey = parsedRowKey.clone();
        while ( modifiedRowKey != null ) {
            if ( modifiedRowKey.length < 4 ) {
                return null;
            }

            String recreatedRowKey = uuidStringVerifier( modifiedRowKey[0] );
            if ( recreatedRowKey == null ) {
                recreatedRowKey = "";
                modifiedRowKey = getStrings( modifiedRowKey, recreatedRowKey );
            }
            else {
                recreatedRowKey = recreatedRowKey.concat( ":" );
                modifiedRowKey = getStrings( modifiedRowKey, recreatedRowKey );
                break;
            }
        }
        return modifiedRowKey;
    }


    private String[] getStrings( String[] modifiedRowKey, String recreatedRowKey ) {
        for ( int i = 1; i < modifiedRowKey.length; i++ ) {

            recreatedRowKey = recreatedRowKey.concat( modifiedRowKey[i] );
            if ( i + 1 != modifiedRowKey.length ) {
                recreatedRowKey = recreatedRowKey.concat( ":" );
            }
        }
        modifiedRowKey = recreatedRowKey.split( ":" );
        return modifiedRowKey;
    }


    //    private void deleteRow( final Mutator<ByteBuffer> m, final UUID applicationId, final String collectionName,
    //                            final String uniqueValueKey, final String uniqueValue ) throws Exception {
    //        logger.debug( "Found 0 uuid's associated with {} Deleting row.", uniqueValue );
    //        UUID timestampUuid = newTimeUUID();
    //        long timestamp = getTimestampInMicros( timestampUuid );
    //
    //        Keyspace ko = cass.getApplicationKeyspace( applicationId );
    //        Mutator<ByteBuffer> mutator = createMutator( ko, be );
    //
    //        Object key = key( applicationId, collectionName, uniqueValueKey, uniqueValue );
    //        addDeleteToMutator( mutator, ENTITY_UNIQUE, key, timestamp );
    //        mutator.execute();
    //        return;
    //    }

    //
    //    private void entityUUIDDelete( final Mutator<ByteBuffer> m, final UUID applicationId, final String
    // collectionName,
    //                                   final String uniqueValueKey, final String uniqueValue,
    //                                   final List<HColumn<ByteBuffer, ByteBuffer>> cols, String rowKey ) throws
    // Exception {
    //        Boolean cleanup = false;
    //        EntityManagerImpl em = ( EntityManagerImpl ) emf.getEntityManager( applicationId );
    //        int numberOfColumnsDeleted = 0;
    //        //these columns all come from the same row key, which means they each belong to the same row key
    // identifier
    //        //thus mixing and matching them in the below if cases won't matter.
    //        Entity[] entities = new Entity[cols.size()];
    //        int numberOfRetrys = 8;
    //        int numberOfTimesRetrying = 0;
    //
    //        int index = 0;
    //
    //
    //        for ( int i = 0; i < numberOfRetrys; i++ ) {
    //            try {
    //                Map<String, EntityRef> results =
    //                    em.getAlias( applicationId, collectionName, Collections.singletonList( uniqueValue ) );
    //                if ( results.size() > 1 ) {
    //                    logger.error("failed to clean up {} from application {}. Please clean manually.",
    // uniqueValue,applicationId);
    //                    break;
    //                }
    //                else {
    //                    continue;
    //                }
    //            }
    //            catch ( Exception toe ) {
    //                logger.error( "timeout doing em getAlias repair. This is the {} number of repairs attempted", i );
    //                toe.printStackTrace();
    //                Thread.sleep( 1000 * i );
    //            }
    //        }
    //    }


    private Entity verifyModifiedTimestamp( final Entity unverifiedEntity ) {
        Entity entity = unverifiedEntity;
        if ( entity != null && entity.getModified() == null ) {
            if ( entity.getCreated() != null ) {
                logger.debug( "{} has no modified. Subsituting created timestamp for their modified timestamp.Manually "
                    + "adding one for comparison purposes", entity.getUuid() );
                entity.setModified( entity.getCreated() );
                return entity;
            }
            else {
                logger.error( "Found no created or modified timestamp. Please remake the following entity: {}."
                    + " Setting both created and modified to 1", entity.getUuid().toString() );
                entity.setCreated( 1L );
                entity.setModified( 1L );
                return entity;
            }
        }
        return entity;
    }


    //really only deletes ones that aren't existant for a specific value
    //    private void deleteInvalidValuesForUniqueProperty( Mutator<ByteBuffer> m, CommandLine line ) throws
    // Exception {
    //        UUID applicationId = UUID.fromString( line.getOptionValue( APPLICATION_ARG ) );
    //        String collectionName = line.getOptionValue( COLLECTION_ARG );
    //        String uniqueValueKey = line.getOptionValue( ENTITY_UNIQUE_PROPERTY_NAME );
    //        String uniqueValue = line.getOptionValue( ENTITY_UNIQUE_PROPERTY_VALUE );
    //
    //        //PLEASE ADD VERIFICATION.
    //
    //        Object key = key( applicationId, collectionName, uniqueValueKey, uniqueValue );
    //
    //
    //        List<HColumn<ByteBuffer, ByteBuffer>> cols =
    //            cass.getColumns( cass.getApplicationKeyspace( applicationId ), CF_UNIQUE_VALUES, key, null, null,
    // 1000,
    //                false );
    //
    //
    //        if ( cols.size() == 0 ) {
    //            logger.error( "This row key: {} has zero columns. Deleting...", key.toString() );
    //        }
    //
    //        entityUUIDDelete( m, applicationId, collectionName, uniqueValueKey, uniqueValue, cols, key.toString() );
    //    }


    private String uuidGarbageParser( final String garbageString ) {
        int index = 1;
        String stringToBeTruncated = garbageString;
        while ( !UUIDUtils.isUUID( stringToBeTruncated ) ) {
            if ( stringToBeTruncated.length() > 36 ) {
                stringToBeTruncated = stringToBeTruncated.substring( index );
            }
            else {
                logger.error( "{} is unparsable", garbageString );
                break;
            }
        }
        return stringToBeTruncated;
    }


    private String uuidStringVerifier( final String garbageString ) {
        int index = 1;
        String stringToBeTruncated = garbageString;
        while ( !UUIDUtils.isUUID( stringToBeTruncated ) ) {
            if ( stringToBeTruncated.length() > 36 ) {
                stringToBeTruncated = stringToBeTruncated.substring( index );
            }
            else {
                return null;
            }
        }
        return stringToBeTruncated;
    }
    //
    //
    //    private void deleteUniqueValue( final UUID applicationId, final String collectionName, final String
    // uniqueValueKey,
    //                                    final String uniqueValue, final UUID entityId ) throws Exception {
    //        logger.warn( "Entity with id {} did not exist in app {} Deleting", entityId, applicationId );
    //        UUID timestampUuid = newTimeUUID();
    //        long timestamp = getTimestampInMicros( timestampUuid );
    //        Keyspace ko = cass.getApplicationKeyspace( applicationId );
    //        Mutator<ByteBuffer> mutator = createMutator( ko, be );
    //
    //
    //        Object key = key( applicationId, collectionName, uniqueValueKey, uniqueValue );
    //        addDeleteToMutator( mutator, CF_UNIQUE_VALUES, key, entityId, timestamp );
    //        mutator.execute();
    //        return;
    //    }
}
