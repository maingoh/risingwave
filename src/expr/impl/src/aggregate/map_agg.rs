// Copyright 2026 RisingWave Labs
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

use std::collections::HashSet;

use risingwave_common::array::ArrayBuilderImpl;
use risingwave_common::types::{Datum, ListValue, MapType, MapValue, ScalarImpl, ScalarRefImpl};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::aggregate::AggStateDyn;
use risingwave_expr::expr::Context;
use risingwave_expr::{ExprError, Result, aggregate};

/// Aggregates `(key, value)` pairs into a map. NULL keys and duplicate keys
/// are rejected at update time so that finalization is infallible. This is
/// functionally equivalent to `map_from_entries(array_agg(row(key, value)))`,
/// but avoids materializing the intermediate `list<struct<key, value>>`.
#[aggregate(
    "map_agg(any, any) -> anymap",
    type_infer = "|args| Ok(MapType::from_kv(args[0].clone(), args[1].clone()).into())"
)]
fn map_agg(
    state: &mut MapAggState,
    key: Option<ScalarRefImpl<'_>>,
    value: Option<ScalarRefImpl<'_>>,
    ctx: &Context,
) -> Result<()> {
    let Some(key) = key else {
        return Err(ExprError::Custom("map keys must not be NULL".into()));
    };
    state.ensure_initialized(ctx);
    if !state.seen.insert(key.into_scalar_impl()) {
        return Err(ExprError::Custom("map keys must be unique".into()));
    }
    state.keys.as_mut().unwrap().append(Some(key));
    state.values.as_mut().unwrap().append(value);
    Ok(())
}

#[derive(Debug, Default)]
struct MapAggState {
    keys: Option<ArrayBuilderImpl>,
    values: Option<ArrayBuilderImpl>,
    seen: HashSet<ScalarImpl>,
}

impl MapAggState {
    fn ensure_initialized(&mut self, ctx: &Context) {
        if self.keys.is_none() {
            self.keys = Some(ctx.arg_types[0].create_array_builder(1));
            self.values = Some(ctx.arg_types[1].create_array_builder(1));
        }
    }
}

impl EstimateSize for MapAggState {
    fn estimated_heap_size(&self) -> usize {
        self.keys.estimated_heap_size()
            + self.values.estimated_heap_size()
            // rough approximation: 32 bytes per slot in the HashSet.
            + self.seen.capacity() * 32
    }
}

impl AggStateDyn for MapAggState {}

impl From<&MapAggState> for Datum {
    fn from(state: &MapAggState) -> Self {
        let (keys, values) = state.keys.as_ref().zip(state.values.as_ref())?;
        let keys = ListValue::new(keys.clone().finish());
        let values = ListValue::new(values.clone().finish());
        // Update-time checks guarantee no NULL keys and no duplicates, so this
        // conversion is infallible. If it ever fails (framework bug or manual
        // misuse), unwrap surfaces the invariant violation loudly.
        Some(MapValue::try_from_kv(keys, values).unwrap().into())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::StreamChunk;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_expr::Result;
    use risingwave_expr::aggregate::{AggCall, build_append_only};

    #[tokio::test]
    async fn test_map_agg_basic() -> Result<()> {
        let chunk = StreamChunk::from_pretty(
            " T i
            + a 1
            + b 2
            + c 3",
        );
        let map_agg = build_append_only(&AggCall::from_pretty(
            "(map_agg:map<varchar,int4> $0:varchar $1:int4)",
        ))?;
        let mut state = map_agg.create_state()?;
        map_agg.update(&mut state, &chunk).await?;
        assert!(map_agg.get_result(&state).await?.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn test_map_agg_empty() -> Result<()> {
        let map_agg = build_append_only(&AggCall::from_pretty(
            "(map_agg:map<varchar,int4> $0:varchar $1:int4)",
        ))?;
        let state = map_agg.create_state()?;
        assert_eq!(map_agg.get_result(&state).await?, None);
        Ok(())
    }
}
