use pyo3::prelude::*;

use datafusion::prelude::ExecutionConfig;

#[pyclass(name = "ExecutionConfig", module = "datafusion", subclass, unsendable)]
#[derive(Clone)]
pub(crate) struct PyExecutionConfig {
    cfg: ExecutionConfig,
}

#[pymethods]
impl PyExecutionConfig{
    #[new]
    fn new() -> Self {
        Self {
            cfg: ExecutionConfig::new(),
        }
    }

    /// Customize target_partitions
    fn with_target_partitions(&self, n: usize) -> PyExecutionConfig {
        let mut result = self.to_owned();
        result.cfg = result.cfg.with_target_partitions(n);
        result
    }

    /// Customize batch size
    fn with_batch_size(&self, n: usize) -> PyExecutionConfig {
        let mut result = self.to_owned();
        result.cfg = result.cfg.with_batch_size(n);
        result
    }

    /// Selects a name for the default catalog and schema
    pub fn with_default_catalog_and_schema(
        &self,
        catalog: &str,
        schema: &str,
    ) -> PyExecutionConfig {
        let mut result = self.to_owned();
        result.cfg = result.cfg.with_default_catalog_and_schema(catalog, schema);
        result
    }

    /// Controls whether the default catalog and schema will be automatically created
    pub fn create_default_catalog_and_schema(&self, create: bool) -> PyExecutionConfig {
        let mut result = self.to_owned();
        result.cfg = result.cfg.create_default_catalog_and_schema(create);
        result
    }

    /// Enables or disables the inclusion of `information_schema` virtual tables
    pub fn with_information_schema(&self, enabled: bool) -> PyExecutionConfig {
        let mut result = self.to_owned();
        result.cfg = result.cfg.with_information_schema(enabled);
        result
    }
    
    /// Enables or disables the use of repartitioning for joins to improve parallelism
    pub fn with_repartition_joins(&self, enabled: bool) -> PyExecutionConfig {
        let mut result = self.to_owned();
        result.cfg = result.cfg.with_repartition_joins(enabled);
        result
    }

    /// Enables or disables the use of repartitioning for aggregations to improve parallelism
    pub fn with_repartition_aggregations(&self, enabled: bool) -> PyExecutionConfig {
        let mut result = self.to_owned();
        result.cfg = result.cfg.with_repartition_aggregations(enabled);
        result
    }

    /// Enables or disables the use of pruning predicate for parquet readers to skip row groups
    pub fn with_parquet_pruning(&self, enabled: bool) -> PyExecutionConfig {
        let mut result = self.to_owned();
        result.cfg = result.cfg.with_parquet_pruning(enabled);
        result
    }
    
}