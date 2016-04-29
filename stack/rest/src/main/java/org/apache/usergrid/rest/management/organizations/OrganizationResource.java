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
package org.apache.usergrid.rest.management.organizations;


import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import org.glassfish.jersey.server.mvc.Viewable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import org.apache.usergrid.exception.NotImplementedException;
import org.apache.usergrid.management.ActivationState;
import org.apache.usergrid.management.OrganizationConfig;
import org.apache.usergrid.management.OrganizationInfo;
import org.apache.usergrid.rest.AbstractContextResource;
import org.apache.usergrid.rest.ApiResponse;
import org.apache.usergrid.rest.exceptions.RedirectionException;
import org.apache.usergrid.rest.management.organizations.applications.ApplicationsResource;
import org.apache.usergrid.rest.management.organizations.users.UsersResource;
import org.apache.usergrid.rest.security.annotations.RequireOrganizationAccess;
import org.apache.usergrid.rest.security.annotations.RequireSystemAccess;
import org.apache.usergrid.security.oauth.ClientCredentialsInfo;
import org.apache.usergrid.security.tokens.exceptions.TokenException;
import org.apache.usergrid.services.ServiceResults;

import com.fasterxml.jackson.jaxrs.json.annotation.JSONP;


@Component("org.apache.usergrid.rest.management.organizations.OrganizationResource")
@Scope("prototype")
@Produces({
        MediaType.APPLICATION_JSON, "application/javascript", "application/x-javascript", "text/ecmascript",
        "application/ecmascript", "text/jscript"
})
public class OrganizationResource extends AbstractContextResource {

    private static final Logger logger = LoggerFactory.getLogger( OrganizationsResource.class );

    OrganizationInfo organization;


    public OrganizationResource() {
        if (logger.isTraceEnabled()) {
            logger.trace("OrganizationResource created");
        }
    }


    public OrganizationResource init( OrganizationInfo organization ) {
        this.organization = organization;
        if (logger.isTraceEnabled()) {
            logger.trace("OrganizationResource initialized for org {}", organization.getName());
        }
        return this;
    }


    @Path("users")
    public UsersResource getOrganizationUsers( @Context UriInfo ui ) throws Exception {
        return getSubResource( UsersResource.class ).init( organization );
    }


    @Path("applications")
    public ApplicationsResource getOrganizationApplications( @Context UriInfo ui ) throws Exception {
        return getSubResource( ApplicationsResource.class ).init( organization );
    }


    @Path("apps")
    public ApplicationsResource getOrganizationApplications2( @Context UriInfo ui ) throws Exception {
        return getSubResource( ApplicationsResource.class ).init( organization );
    }


    @GET
    @JSONP
    @RequireOrganizationAccess
    @Produces({MediaType.APPLICATION_JSON, "application/javascript"})
    public ApiResponse getOrganizationDetails( @Context UriInfo ui,
                                                   @QueryParam("callback") @DefaultValue("callback") String callback )
            throws Exception {

        if (logger.isTraceEnabled()) {
            logger.trace("Get details for organization: {}", organization.getUuid());
        }

        ApiResponse response = createApiResponse();
        response.setProperty( "organization", management.getOrganizationData( organization ) );

        return response;
    }


    @GET
    @Path("activate")
    @Produces(MediaType.TEXT_HTML)
    public Viewable activate( @Context UriInfo ui, @QueryParam("token") String token ) {

        try {
            management.handleActivationTokenForOrganization( organization.getUuid(), token );
            return handleViewable( "activate", this, organization.getName() );
        }
        catch ( TokenException e ) {
            return handleViewable( "bad_activation_token", this, organization.getName() );
        }
        catch ( RedirectionException e ) {
            throw e;
        }
        catch ( Exception e ) {
            return handleViewable( "error", e, organization.getName() );
        }
    }


    @GET
    @Path("confirm")
    @Produces(MediaType.TEXT_HTML)
    public Viewable confirm( @Context UriInfo ui, @QueryParam("token") String token ) {

        try {
            ActivationState state = management.handleActivationTokenForOrganization( organization.getUuid(), token );
            if ( state == ActivationState.CONFIRMED_AWAITING_ACTIVATION ) {
                return handleViewable( "confirm", this, organization.getName() );
            }
            return handleViewable( "activate", this, organization.getName() );
        }
        catch ( TokenException e ) {
            return handleViewable( "bad_activation_token", this, organization.getName() );
        }
        catch ( RedirectionException e ) {
            throw e;
        }
        catch ( Exception e ) {
            return handleViewable( "error", e, organization.getName() );
        }
    }


    @GET
    @Path("reactivate")
    @JSONP
    @Produces({MediaType.APPLICATION_JSON, "application/javascript"})
    public ApiResponse reactivate( @Context UriInfo ui,
                                       @QueryParam("callback") @DefaultValue("callback") String callback )
            throws Exception {

        logger.info("Send activation email for organization: {}", organization.getUuid());

        ApiResponse response = createApiResponse();

        management.startOrganizationActivationFlow( organization );

        response.setAction( "reactivate organization" );
        return response;
    }


    @RequireOrganizationAccess
    @GET
    @Path("feed")
    @JSONP
    @Produces({MediaType.APPLICATION_JSON, "application/javascript"})
    public ApiResponse getFeed( @Context UriInfo ui,
                                    @QueryParam("callback") @DefaultValue("callback") String callback )
            throws Exception {

        ApiResponse response = createApiResponse();
        response.setAction( "get organization feed" );

        ServiceResults results = management.getOrganizationActivity( organization );
        response.setEntities( results.getEntities() );
        response.setSuccess();

        return response;
    }


    @RequireOrganizationAccess
    @GET
    @Path("credentials")
    @JSONP
    @Produces({MediaType.APPLICATION_JSON, "application/javascript"})
    public ApiResponse getCredentials( @Context UriInfo ui,
                                           @QueryParam("callback") @DefaultValue("callback") String callback )
            throws Exception {

        ApiResponse response = createApiResponse();
        response.setAction( "get organization client credentials" );

        ClientCredentialsInfo keys =
                new ClientCredentialsInfo( management.getClientIdForOrganization( organization.getUuid() ),
                        management.getClientSecretForOrganization( organization.getUuid() ) );

        response.setCredentials( keys );
        return response;
    }


    @RequireOrganizationAccess
    @POST
    @Path("credentials")
    @JSONP
    @Produces({MediaType.APPLICATION_JSON, "application/javascript"})
    public ApiResponse generateCredentials( @Context UriInfo ui,
                                                @QueryParam("callback") @DefaultValue("callback") String callback )
            throws Exception {

        ApiResponse response = createApiResponse();
        response.setAction( "generate organization client credentials" );

        ClientCredentialsInfo credentials =
                new ClientCredentialsInfo( management.getClientIdForOrganization( organization.getUuid() ),
                        management.newClientSecretForOrganization( organization.getUuid() ) );

        response.setCredentials( credentials );
        return response;
    }


    public OrganizationInfo getOrganization() {
        return organization;
    }


    @RequireOrganizationAccess
    @Consumes(MediaType.APPLICATION_JSON)
    @PUT
    @JSONP
    @Produces({MediaType.APPLICATION_JSON, "application/javascript"})
    public ApiResponse executePut( @Context UriInfo ui, Map<String, Object> json,
                                       @QueryParam("callback") @DefaultValue("callback") String callback )
            throws Exception {

        if (logger.isTraceEnabled()) {
            logger.trace("executePut");
        }

        ApiResponse response = createApiResponse();
        response.setAction( "put" );

        response.setParams( ui.getQueryParameters() );

        Map customProperties = ( Map ) json.get( OrganizationsResource.ORGANIZATION_PROPERTIES );
        organization.setProperties( customProperties );
        management.updateOrganization( organization );

        return response;
    }

    @JSONP
    @RequireSystemAccess
    @GET
    @Path("config")
    public ApiResponse getConfig( @Context UriInfo ui,
                                  @QueryParam("items") @DefaultValue("") String itemsParam,
                                  @QueryParam("separate_defaults") @DefaultValue("false") boolean separateDefaults,
                                  @QueryParam("callback") @DefaultValue("callback") String callback )
            throws Exception {

        if (logger.isTraceEnabled()) {
            logger.trace("Get configuration for organization: {}", organization.getUuid());
        }

        ApiResponse response = createApiResponse();
        response.setAction( "get organization configuration" );
        //response.setParams(ui.getQueryParameters());

        OrganizationConfig orgConfig =
                management.getOrganizationConfigByUuid( organization.getUuid() );

        if (separateDefaults) {
            response.setProperty("orgConfiguration", getConfigData(orgConfig, itemsParam, false, true));
            response.setProperty("defaults", getConfigData(orgConfig, itemsParam, true, false));
        } else {
            response.setProperty("configuration", getConfigData(orgConfig, itemsParam, true, true));
        }

        return response;
    }


    @RequireSystemAccess
    @Consumes(MediaType.APPLICATION_JSON)
    @JSONP
    @PUT
    @Path("config")
    public ApiResponse putConfig( @Context UriInfo ui,
                                  Map<String, Object> json,
                                  @QueryParam("separate_defaults") @DefaultValue("false") boolean separateDefaults,
                                  @QueryParam("only_changed") @DefaultValue("false") boolean onlyChanged,
                                  @QueryParam("callback") @DefaultValue("callback") String callback )
            throws Exception {

        if (logger.isTraceEnabled()) {
            logger.trace("Put configuration for organization: {}", organization.getUuid());
        }

        ApiResponse response = createApiResponse();
        response.setAction("put organization configuration");
        //response.setParams(ui.getQueryParameters());

        OrganizationConfig orgConfig =
                management.getOrganizationConfigByUuid( organization.getUuid() );

        // validates JSON and throws IllegalArgumentException if invalid
        // exception will be handled up the chain
        orgConfig.addProperties(json, true);

        management.updateOrganizationConfig(orgConfig);

        // refresh orgConfig -- to pick up removed entries and defaults
        orgConfig = management.getOrganizationConfigByUuid( organization.getUuid() );

        String itemsToReturn = "";
        if (onlyChanged) {
            itemsToReturn = String.join(",", json.keySet());
        }

        if (separateDefaults) {
            response.setProperty("orgConfiguration", getConfigData(orgConfig, itemsToReturn, false, true));
            response.setProperty("defaults", getConfigData(orgConfig, itemsToReturn, true, false));
        } else {
            response.setProperty( "configuration", getConfigData(orgConfig, itemsToReturn, true, true));
        }

        return response;
    }


    /** Delete organization is not yet supported */
    //@RequireSystemAccess
    @DELETE
    public ApiResponse deleteOrganization() throws Exception {
        throw new NotImplementedException();
    }

}
