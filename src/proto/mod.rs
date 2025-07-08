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

use crate::{
    protobuf::{Hosts, StageAddrs},
    vocab::Addrs,
};

#[rustfmt::skip]
pub mod generated;

impl From<Addrs> for StageAddrs {
    fn from(value: Addrs) -> Self {
        let mut stage_addrs = StageAddrs::default();

        for (stage_id, partition_addrs) in value {
            for (part, part_addrs) in partition_addrs {
                let host_addrs = part_addrs.into_iter().collect();
                stage_addrs
                    .stage_addrs
                    .entry(stage_id)
                    .or_default()
                    .partition_addrs
                    .insert(part, Hosts { hosts: host_addrs });
            }
        }

        stage_addrs
    }
}
