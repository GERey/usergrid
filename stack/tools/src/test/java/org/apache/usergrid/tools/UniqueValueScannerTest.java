package org.apache.usergrid.tools;/*
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


import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.RandomStringUtils;

import org.apache.usergrid.ServiceITSetup;
import org.apache.usergrid.ServiceITSetupImpl;
import org.apache.usergrid.services.AbstractServiceIT;


public class UniqueValueScannerTest extends AbstractServiceIT {
    static final Logger logger = LoggerFactory.getLogger( UniqueValueScannerTest.class );

    @ClassRule
    public static ServiceITSetup setup = new ServiceITSetupImpl();

    @org.junit.Test
    public void testBasicOperation() throws Exception {

        String rand = RandomStringUtils.randomAlphanumeric( 10 );

        // create app with some data
//
//        String orgName = "org_" + rand;
//        String appName = "app_" + rand;
//        String userName = "user_" + rand;
//
//
//        ExportDataCreator creator = new ExportDataCreator();
//        creator.startTool( new String[] {
//            "-organization", orgName,
//            "-application", appName,
//            "-username", userName,
//            "-host", "localhost:9160",
//            "-eshost", "localhost:9200",
//            "-escluster", "elasticsearch",
//            "-ugcluster","testcluster"
//
//        }, false);
//
//        long start = System.currentTimeMillis();

        // export app to a directory

        //String directoryName = "target/export" + rand;'

        //org-NuxYzVmnPo

        EntityDataScanner uniqueValueScanner = new EntityDataScanner();
        uniqueValueScanner.startTool( new String[] {
            "-host", "localhost:9160",
//            "-eshost", "localhost:9200",
//            "-escluster", "elasticsearch",
//            "-ugcluster","testcluster",
            "-app","9b36bea0-1ec7-11e6-8ada-4a347760de5b",
            "-col","company"
//            "-cf","Entity_Dictionaries"
//            System.setProperty( "usergrid.cluster_name", line.getOptionValue( "ugcluster" )  );

        },false );

//        logger.info( "100 read and 100 write threads = " + (System.currentTimeMillis() - start) / 1000 + "s" );
//
//        // check that we got the expected number of export files
//
//        File exportDir1 = new File(directoryName + "1");
//        exportApp.startTool( new String[] {
//            "-application", orgName + "/" + appName,
//            "-writeThreads", "1",
//            "-host", "localhost:9160",
//            "-eshost", "localhost:9200",
//            "-escluster", "elasticsearch",
//            "-outputDir", directoryName + "1"
//        }, false );
//
//        logger.info( "1 thread time = " + (System.currentTimeMillis() - start) / 1000 + "s" );
    }

}
