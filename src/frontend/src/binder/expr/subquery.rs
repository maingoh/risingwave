// Copyright 2022 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::types::MapType;
use risingwave_sqlparser::ast::Query;

use crate::binder::Binder;
use crate::error::{ErrorCode, Result, bail_bind_error};
use crate::expr::{ExprImpl, Subquery, SubqueryKind};

impl Binder {
    pub fn bind_subquery_expr(&mut self, query: &Query, kind: SubqueryKind) -> Result<ExprImpl> {
        let query = self.bind_query(query)?;
        let expected_columns = match kind {
            SubqueryKind::Existential | SubqueryKind::UpdateSet => None,
            SubqueryKind::Map => Some(2),
            _ => Some(1),
        };
        if let Some(n) = expected_columns
            && query.data_types().len() != n
        {
            if n == 1 {
                bail_bind_error!("Subquery must return only one column");
            } else {
                bail_bind_error!("Subquery must return exactly {} columns", n);
            }
        }
        if matches!(kind, SubqueryKind::Map) {
            let types = query.data_types();
            MapType::try_from_kv(types[0].clone(), types[1].clone()).map_err(ErrorCode::BindError)?;
        }
        Ok(Subquery::new(query, kind).into())
    }
}
