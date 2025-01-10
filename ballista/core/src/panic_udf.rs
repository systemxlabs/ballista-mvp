use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion::logical_expr::{create_udf, ScalarUDF, Volatility};
use datafusion::physical_expr::functions::make_scalar_function;
use std::sync::Arc;

pub fn panic_udf() -> ScalarUDF {
    let panic = |args: &[ArrayRef]| panic!("Panic UDF invoked");

    let panic = make_scalar_function(panic);

    let panic = create_udf(
        "panic",
        // expects two f64
        vec![DataType::Int64],
        // returns f64
        Arc::new(DataType::Int64),
        Volatility::Immutable,
        panic,
    );
    panic
}
