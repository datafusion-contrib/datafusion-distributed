// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use arrow_flight::{
    flight_service_server::FlightService, sql::server::FlightSqlService, Action, ActionType,
    Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse,
    PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::stream::BoxStream;
use tonic::{Request, Response, Status, Streaming};

use crate::logging::error;

pub type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
pub type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;

#[tonic::async_trait]
pub trait FlightHandler: Send + Sync {
    async fn do_get(&self, request: Request<Ticket>) -> Result<Response<DoGetStream>, Status>;
    async fn do_action(&self, request: Request<Action>)
        -> Result<Response<DoActionStream>, Status>;
}

pub struct FlightServ {
    pub handler: Arc<dyn FlightHandler>,
}

#[tonic::async_trait]
impl FlightService for FlightServ {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        self.handler.do_get(request).await.inspect_err(|e| {
            error!("Error in do_get: {:#?}", e);
            log::error!("LOG Error in do_get: {:#?}", e);
        })
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Unimplemented: do put"))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Unimplemented: handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Unimplemented: list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Unimplemented: get_flight_info"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Unimplemented: poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Unimplemented: get_schema"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        self.handler.do_action(request).await.inspect_err(|e| {
            error!("Error in do_action: {:#?}", e);
        })
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Unimplemented: list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Unimplemented: do_exchange"))
    }
}

#[tonic::async_trait]
pub trait FlightSqlHandler: Send + Sync {
    async fn get_flight_info_statement(
        &self,
        query: arrow_flight::sql::CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status>;

    async fn get_flight_info_substrait_plan(
        &self,
        query: arrow_flight::sql::CommandStatementSubstraitPlan,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status>;

    async fn do_get_statement(
        &self,
        ticket: arrow_flight::sql::TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status>;
}

pub struct FlightSqlServ {
    pub handler: Arc<dyn FlightSqlHandler>,
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServ {
    type FlightService = Self;

    async fn register_sql_info(&self, _id: i32, _result: &arrow_flight::sql::SqlInfo) {}

    async fn get_flight_info_statement(
        &self,
        query: arrow_flight::sql::CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.handler
            .get_flight_info_statement(query, request)
            .await
            .inspect_err(|e| {
                error!("Error in do_flight_info_statement: {:#?}", e);
            })
    }

    async fn get_flight_info_substrait_plan(
        &self,
        query: arrow_flight::sql::CommandStatementSubstraitPlan,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.handler
            .get_flight_info_substrait_plan(query, request)
            .await
            .inspect_err(|e| {
                error!("Error in get_flight_info_substrait_plan: {:?}", e);
            })
    }

    async fn do_get_statement(
        &self,
        ticket: arrow_flight::sql::TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.handler
            .do_get_statement(ticket, request)
            .await
            .inspect_err(|e| {
                error!("Error in do_get_statement: {:#?}", e);
            })
    }
}
