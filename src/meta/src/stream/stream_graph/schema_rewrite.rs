use std::collections::HashMap;
use std::sync::LazyLock;

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::catalog::{ColumnCatalog, Field};
use risingwave_connector::sink::catalog::SinkType;
use risingwave_pb::catalog::PbTable;
use risingwave_pb::plan_common::{PbColumnCatalog, PbField};
use risingwave_pb::stream_plan::stream_node::{NodeBody as PbNodeBody, NodeBodyDiscriminants};
use risingwave_pb::stream_plan::{PbStreamScanType, StreamNode};
use strum::IntoDiscriminant;

use crate::MetaResult;
use crate::model::FragmentId;

pub enum SinkSchemaChangeSet {
    AddColumns(Vec<ColumnCatalog>),
    #[allow(dead_code)]
    DropColumns(Vec<String>),
    // Placeholder for future extensibility.
    // AlterColumnTypes,
}

impl SinkSchemaChangeSet {
    pub fn for_add_columns(newly_added_columns: &[ColumnCatalog]) -> Self {
        Self::AddColumns(newly_added_columns.to_vec())
    }
}

pub struct RewriteContext<'a> {
    pub sink: &'a risingwave_pb::catalog::PbSink,
    pub upstream_table: &'a PbTable,
    pub upstream_table_fragment_id: FragmentId,
}

pub trait OperatorRule: Send + Sync {
    /// Validates whether this operator node is supported by the current rewrite framework.
    fn validate(&self, _node: &StreamNode) -> MetaResult<()> {
        Ok(())
    }

    fn rewrite(
        &self,
        node: &mut StreamNode,
        change_set: &SinkSchemaChangeSet,
        ctx: &RewriteContext<'_>,
    ) -> MetaResult<()>;
}

pub struct PlanRewriter {
    rules: HashMap<NodeBodyDiscriminants, Box<dyn OperatorRule>>,
}

impl PlanRewriter {
    pub fn rewrite_root(
        &self,
        root: &mut StreamNode,
        change_set: &SinkSchemaChangeSet,
        ctx: &RewriteContext<'_>,
    ) -> MetaResult<()> {
        self.rewrite_node(root, change_set, ctx)
    }

    pub fn validate_root(&self, root: &StreamNode) -> MetaResult<()> {
        self.validate_node(root)
    }

    fn rewrite_node(
        &self,
        node: &mut StreamNode,
        change_set: &SinkSchemaChangeSet,
        ctx: &RewriteContext<'_>,
    ) -> MetaResult<()> {
        for child in &mut node.input {
            self.rewrite_node(child, change_set, ctx)?;
        }

        let kind = node
            .node_body
            .as_ref()
            .expect("stream node should have a node_body")
            .discriminant();
        let Some(rule) = self.rules.get(&kind) else {
            return Err(
                anyhow!("unsupported operator for sink auto schema change: kind={kind}").into(),
            );
        };
        rule.rewrite(node, change_set, ctx)
    }

    fn validate_node(&self, node: &StreamNode) -> MetaResult<()> {
        for child in &node.input {
            self.validate_node(child)?;
        }

        let kind = node
            .node_body
            .as_ref()
            .expect("stream node should have a node_body")
            .discriminant();
        let Some(rule) = self.rules.get(&kind) else {
            return Err(
                anyhow!("unsupported operator for sink auto schema change: kind={kind}").into(),
            );
        };
        rule.validate(node)
    }
}

/// Default rule set for sink auto schema change rewriting.
pub static DEFAULT_SINK_SCHEMA_REWRITER: LazyLock<PlanRewriter> = LazyLock::new(|| {
    let entries: [(NodeBodyDiscriminants, Box<dyn OperatorRule>); 4] = [
        (NodeBodyDiscriminants::Sink, Box::new(SinkRule)),
        (NodeBodyDiscriminants::StreamScan, Box::new(StreamScanRule)),
        (NodeBodyDiscriminants::Merge, Box::new(MergeRule)),
        (NodeBodyDiscriminants::BatchPlan, Box::new(BatchPlanRule)),
    ];
    let rules = HashMap::from(entries);

    PlanRewriter { rules }
});

fn extend_pb_fields(
    fields: &mut Vec<PbField>,
    new_columns: &[ColumnCatalog],
    table_name: &str,
) -> MetaResult<()> {
    fields.extend(new_columns.iter().map(|col| {
        Field::new(
            format!("{}.{}", table_name, col.column_desc.name),
            col.data_type().clone(),
        )
        .to_prost()
    }));

    Ok(())
}

pub struct SinkRule;

impl OperatorRule for SinkRule {
    fn rewrite(
        &self,
        node: &mut StreamNode,
        change_set: &SinkSchemaChangeSet,
        ctx: &RewriteContext<'_>,
    ) -> MetaResult<()> {
        fn extend_sink_columns(
            sink_columns: &mut Vec<PbColumnCatalog>,
            new_columns: &[ColumnCatalog],
            get_column_name: impl Fn(&String) -> String,
        ) -> MetaResult<()> {
            let next_column_id = sink_columns
                .iter()
                .map(|col| col.column_desc.as_ref().unwrap().column_id + 1)
                .max()
                .unwrap_or(1);

            sink_columns.extend(new_columns.iter().enumerate().map(|(i, col)| {
                let mut col = col.to_protobuf();
                let column_desc = col.column_desc.as_mut().unwrap();
                column_desc.column_id = next_column_id + (i as i32);
                column_desc.name = get_column_name(&column_desc.name);
                col
            }));

            Ok(())
        }

        let PbNodeBody::Sink(sink_node_body) = node.node_body.as_mut().unwrap() else {
            return Err(anyhow!(
                "expected Sink node_body, got {}",
                node.node_body.as_ref().unwrap().discriminant()
            )
            .into());
        };

        let SinkSchemaChangeSet::AddColumns(added_columns) = change_set else {
            return Err(anyhow!("only support AddColumns in sink auto schema change").into());
        };

        // Update sink desc columns and identity.
        let sink_desc = sink_node_body
            .sink_desc
            .as_mut()
            .ok_or_else(|| anyhow!("sink node must have sink_desc"))?;
        extend_sink_columns(&mut sink_desc.column_catalogs, added_columns, |name| {
            name.clone()
        })?;

        // following logic in <StreamSink as Explain>::distill
        node.identity = {
            let sink_type = SinkType::from_proto(ctx.sink.sink_type());
            let sink_type_str = sink_type.type_str();
            let column_names = sink_desc
                .column_catalogs
                .iter()
                .map(|col| {
                    ColumnCatalog::from(col.clone())
                        .name_with_hidden()
                        .to_string()
                })
                .join(", ");
            let downstream_pk = if !sink_type.is_append_only() {
                let downstream_pk = ctx
                    .sink
                    .downstream_pk
                    .iter()
                    .map(|i| {
                        &ctx.sink.columns[*i as usize]
                            .column_desc
                            .as_ref()
                            .unwrap()
                            .name
                    })
                    .collect_vec();
                format!(", downstream_pk: {downstream_pk:?}")
            } else {
                "".to_owned()
            };
            format!(
                "StreamSink {{ type: {sink_type_str}, columns: [{column_names}]{downstream_pk} }}"
            )
        };

        // Update plan fields.
        extend_pb_fields(&mut node.fields, added_columns, &ctx.upstream_table.name)?;

        // Update log store table columns if present.
        if let Some(log_store_table) = &mut sink_node_body.table {
            extend_sink_columns(&mut log_store_table.columns, added_columns, |name| {
                format!("{}_{}", ctx.upstream_table.name, name)
            })?;
        }

        Ok(())
    }
}

pub struct StreamScanRule;

impl OperatorRule for StreamScanRule {
    fn rewrite(
        &self,
        node: &mut StreamNode,
        change_set: &SinkSchemaChangeSet,
        ctx: &RewriteContext<'_>,
    ) -> MetaResult<()> {
        let PbNodeBody::StreamScan(scan) = node.node_body.as_mut().unwrap() else {
            return Err(anyhow!(
                "expected StreamScan node_body, got {}",
                node.node_body.as_ref().unwrap().discriminant()
            )
            .into());
        };

        let SinkSchemaChangeSet::AddColumns(added_columns) = change_set else {
            return Err(anyhow!("only support AddColumns in sink auto schema change").into());
        };

        // Update scan node fields + identity.
        extend_pb_fields(&mut node.fields, added_columns, &ctx.upstream_table.name)?;
        node.identity = {
            let columns = node.fields.iter().map(|col| &col.name).join(", ");
            format!("StreamTableScan {{ table: t, columns: [{columns}] }}")
        };

        // Update scan internals.
        scan.arrangement_table = Some(ctx.upstream_table.clone());
        scan.output_indices
            .extend((0..added_columns.len()).map(|i| (i + scan.upstream_column_ids.len()) as u32));
        scan.upstream_column_ids
            .extend(added_columns.iter().map(|col| col.column_id().get_id()));

        let table_desc = scan
            .table_desc
            .as_mut()
            .ok_or_else(|| anyhow!("stream scan must have table_desc"))?;
        table_desc
            .value_indices
            .extend((0..added_columns.len()).map(|i| (i + table_desc.columns.len()) as u32));
        table_desc.columns.extend(
            added_columns
                .iter()
                .map(|col| col.column_desc.to_protobuf()),
        );
        Ok(())
    }

    fn validate(&self, node: &StreamNode) -> MetaResult<()> {
        let Some(PbNodeBody::StreamScan(scan)) = node.node_body.as_ref() else {
            return Err(anyhow!("expected StreamScan node_body").into());
        };

        let stream_scan_type = PbStreamScanType::try_from(scan.stream_scan_type).unwrap();
        if stream_scan_type != PbStreamScanType::ArrangementBackfill {
            return Err(anyhow!(
                "unsupported stream_scan_type for auto refresh schema: {:?}",
                stream_scan_type
            )
            .into());
        }

        let [merge_node, batch_plan_node] = node.input.as_slice() else {
            return Err(anyhow!(
                "unsupported StreamScan shape for auto refresh schema: expected 2 inputs, got {}",
                node.input.len()
            )
            .into());
        };

        let Some(PbNodeBody::Merge(_)) = merge_node.node_body.as_ref() else {
            return Err(anyhow!(
                "unsupported StreamScan child[0]: expected Merge, got {}",
                merge_node.node_body.as_ref().unwrap().discriminant()
            )
            .into());
        };
        let Some(PbNodeBody::BatchPlan(_)) = batch_plan_node.node_body.as_ref() else {
            return Err(anyhow!(
                "unsupported StreamScan child[1]: expected BatchPlan, got {}",
                batch_plan_node.node_body.as_ref().unwrap().discriminant()
            )
            .into());
        };
        Ok(())
    }
}

pub struct MergeRule;

impl OperatorRule for MergeRule {
    fn rewrite(
        &self,
        node: &mut StreamNode,
        change_set: &SinkSchemaChangeSet,
        ctx: &RewriteContext<'_>,
    ) -> MetaResult<()> {
        let PbNodeBody::Merge(merge) = node.node_body.as_mut().unwrap() else {
            return Err(anyhow!(
                "expected Merge node_body, got {}",
                node.node_body.as_ref().unwrap().discriminant()
            )
            .into());
        };

        let SinkSchemaChangeSet::AddColumns(added_columns) = change_set else {
            return Err(anyhow!("only support AddColumns in sink auto schema change").into());
        };

        // The downstream fragment ID might change during schema change.
        merge.upstream_fragment_id = ctx.upstream_table_fragment_id;

        // Keep the output schema in sync for ADD COLUMN.
        node.fields.extend(added_columns.iter().map(|col| {
            Field::new(col.column_desc.name.clone(), col.data_type().clone()).to_prost()
        }));

        Ok(())
    }
}

pub struct BatchPlanRule;

impl OperatorRule for BatchPlanRule {
    fn rewrite(
        &self,
        _node: &mut StreamNode,
        _change_set: &SinkSchemaChangeSet,
        _ctx: &RewriteContext<'_>,
    ) -> MetaResult<()> {
        // Intentionally left as a no-op for now.
        // For backfill variants other than ArrangementBackfill, `BatchPlan` may become
        // schema-relevant and should be handled here.
        Ok(())
    }
}
