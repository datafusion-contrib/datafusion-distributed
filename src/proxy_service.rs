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

use anyhow::{Context, anyhow};
use arrow_flight::{
    FlightDescriptor,
    FlightInfo,
    Ticket,
    flight_service_server::FlightServiceServer,
};
use parking_lot::Mutex;
use prost::Message;
use tokio::{
    net::TcpListener,
    sync::mpsc::{Receiver, Sender, channel},
};
use tonic::{Request, Response, Status, async_trait, transport::Server};

use crate::{
    explain::is_explain_query,
    flight::{FlightSqlHandler, FlightSqlServ},
    flight_handlers::FlightRequestHandler,
    k8s::get_worker_addresses,
    logging::{debug, info, trace},
    protobuf::TicketStatementData,
    query_planner::QueryPlanner,
    result::Result,
};

pub struct DfRayProxyHandler {
    pub flight_handler: FlightRequestHandler,
}

impl DfRayProxyHandler {
    pub fn new() -> Self {
        // call this function to bootstrap the worker discovery mechanism
        get_worker_addresses().expect("Could not get worker addresses upon startup");
        Self {
            flight_handler: FlightRequestHandler::new(QueryPlanner::new()),
        }
    }
}

#[async_trait]
impl FlightSqlHandler for DfRayProxyHandler {
    async fn get_flight_info_statement(
        &self,
        query: arrow_flight::sql::CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let is_explain = is_explain_query(&query.query);

        if is_explain {
            self.flight_handler.handle_explain_request(&query.query).await
        } else {
            self.flight_handler.handle_query_request(&query.query).await
        }
    }

    async fn do_get_statement(
        &self,
        ticket: arrow_flight::sql::TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<crate::flight::DoGetStream>, Status> {
        trace!("do_get_statement");
        let remote_addr = request
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or("unknown".to_string());

        let tsd = TicketStatementData::decode(ticket.statement_handle)
            .map_err(|e| Status::internal(format!("Cannot parse statement handle {e:?}")))?;

        debug!("request for ticket: {:?} from {}", tsd, remote_addr);

        if tsd.explain_data.is_some() {
            self.flight_handler.handle_explain_statement_execution(tsd, &remote_addr).await
        } else {
            self.flight_handler.handle_regular_statement_execution(tsd, &remote_addr).await
        }
    }
}

/// DFRayProcessorService is a Arrow Flight service that serves streams of
/// partitions from a hosted Physical Plan
///
/// It only responds to the DoGet Arrow Flight method
pub struct DFRayProxyService {
    listener: Option<TcpListener>,
    handler: Arc<DfRayProxyHandler>,
    addr: Option<String>,
    all_done_tx: Arc<Mutex<Sender<()>>>,
    all_done_rx: Option<Receiver<()>>,
    port: usize,
}

impl DFRayProxyService {
    pub fn new(port: usize) -> Self {
        debug!("Creating DFRayProxyService!");
        let listener = None;
        let addr = None;

        let (all_done_tx, all_done_rx) = channel(1);
        let all_done_tx = Arc::new(Mutex::new(all_done_tx));

        let handler = Arc::new(DfRayProxyHandler::new());

        Self {
            listener,
            handler,
            addr,
            all_done_tx,
            all_done_rx: Some(all_done_rx),
            port,
        }
    }

    pub async fn start_up(&mut self) -> Result<()> {
        let my_host_str = format!("0.0.0.0:{}", self.port);

        self.listener = TcpListener::bind(&my_host_str)
            .await
            .map(Some)
            .context("Could not bind socket to {my_host_str}")?;

        self.addr = Some(format!(
            "{}",
            self.listener.as_ref().unwrap().local_addr().unwrap()
        ));

        info!("DFRayProxyService bound to {}", self.addr.as_ref().unwrap());

        Ok(())
    }

    /// get the address of the listing socket for this service
    pub fn addr(&self) -> Result<String> {
        let addr = self.addr.clone().ok_or(anyhow!(
            "DFRayProxyService not started yet, no address available"
        ))?;
        Ok(addr)
    }

    pub async fn all_done(&self) -> Result<()> {
        let sender = self.all_done_tx.lock().clone();

        sender
            .send(())
            .await
            .context("Could not send shutdown signal")?;
        Ok(())
    }

    /// start the service
    pub async fn serve(&mut self) -> Result<()> {
        let mut all_done_rx = self.all_done_rx.take().unwrap();

        let signal = async move {
            all_done_rx
                .recv()
                .await
                .expect("problem receiving shutdown signal");
            info!("received shutdown signal");
        };

        let service = FlightSqlServ {
            handler: self.handler.clone(),
        };

        fn intercept(req: Request<()>) -> Result<Request<()>, Status> {
            println!("Intercepting request: {:?}", req);
            debug!("Intercepting request: {:?}", req);
            Ok(req)
        }
        //let svc = FlightServiceServer::new(service);
        let svc = FlightServiceServer::with_interceptor(service, intercept);

        let listener = self.listener.take().unwrap();

        Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                signal,
            )
            .await
            .context("error running service")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // Test-specific imports
    use arrow_flight::{
        FlightDescriptor,
        Ticket,
        sql::{CommandStatementQuery, TicketStatementQuery},
    };
    use prost::Message;
    use tonic::Request;
    
    use crate::{
        test_utils::explain_test_helpers::{
            create_explain_ticket_statement_data, 
            create_test_proxy_handler, 
            verify_explain_stream_results
        },
    };

    #[tokio::test]
    async fn test_get_flight_info_statement_explain() {
        
        let handler = create_test_proxy_handler();
        
        // Test EXPLAIN query
        let command = CommandStatementQuery {
            query: "EXPLAIN SELECT 1 as test_col".to_string(),
            transaction_id: None,
        };
        
        let request = Request::new(FlightDescriptor::new_cmd(vec![]));
        let result = handler.get_flight_info_statement(command, request).await;
        
        assert!(result.is_ok());
        let response = result.unwrap();
        let flight_info = response.into_inner();
        
        // Verify FlightInfo structure
        assert!(!flight_info.schema.is_empty());
        assert_eq!(flight_info.endpoint.len(), 1);
        assert!(flight_info.endpoint[0].ticket.is_some());
        
        // Verify that ticket has content (encoded TicketStatementData)
        let ticket = flight_info.endpoint[0].ticket.as_ref().unwrap();
        assert!(!ticket.ticket.is_empty());
        
        println!("✓ FlightInfo created successfully with {} schema bytes and ticket with {} bytes", 
                 flight_info.schema.len(), ticket.ticket.len());
    }

    #[tokio::test]
    async fn test_do_get_statement_explain() {
        
        let handler = create_test_proxy_handler();
        
        // First prepare an EXPLAIN query to get proper ticket data
        let query = "EXPLAIN SELECT 1 as test_col";
        let plans = handler.flight_handler.planner.prepare_explain(query).await.unwrap();
        
        let tsd = create_explain_ticket_statement_data(plans);
        
        // Create the ticket
        let ticket_query = TicketStatementQuery {
            statement_handle: tsd.encode_to_vec().into(),
        };
        
        let request = Request::new(Ticket::new(vec![]));
        let result = handler.do_get_statement(ticket_query, request).await;
        
        assert!(result.is_ok());
        let response = result.unwrap();
        let stream = response.into_inner();
        
        // Use shared verification function
        verify_explain_stream_results(stream).await;
    }

    #[tokio::test]
    async fn test_compare_explain_flight_info_responses() {
        let handler = create_test_proxy_handler();
        let query = "EXPLAIN SELECT 1 as test_col";
        
        // Get FlightInfo from handle_explain_request
        let result1 = handler.flight_handler.handle_explain_request(query).await.unwrap();
        let flight_info1 = result1.into_inner();
        
        // Get FlightInfo from get_flight_info_statement
        let command = CommandStatementQuery {
            query: query.to_string(),
            transaction_id: None,
        };
        let request = Request::new(FlightDescriptor::new_cmd(vec![]));
        let result2 = handler.get_flight_info_statement(command, request).await.unwrap();
        let flight_info2 = result2.into_inner();
        
        // Compare FlightInfo responses (structure should be identical)
        assert_eq!(flight_info1.schema.len(), flight_info2.schema.len()); // Same schema size
        assert_eq!(flight_info1.endpoint.len(), flight_info2.endpoint.len()); // Same number of endpoints
        assert_eq!(flight_info1.endpoint.len(), 1); // Both should have exactly one endpoint
        
        // Both should have tickets with content
        let ticket1 = flight_info1.endpoint[0].ticket.as_ref().unwrap();
        let ticket2 = flight_info2.endpoint[0].ticket.as_ref().unwrap();
        assert!(!ticket1.ticket.is_empty());
        assert!(!ticket2.ticket.is_empty());
        
        println!("✓ Both tests produce FlightInfo with identical structure:");
        println!("  - Schema bytes: {} vs {}", flight_info1.schema.len(), flight_info2.schema.len());
        println!("  - Endpoints: {} vs {}", flight_info1.endpoint.len(), flight_info2.endpoint.len());
        println!("  - Ticket bytes: {} vs {}", ticket1.ticket.len(), ticket2.ticket.len());
    }

    // TODO: Add tests for regular (non-explain) queries
    // We might need to create integration or end-to-end test infrastructure for this because
    // they need workers
}
