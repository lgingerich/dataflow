use datafusion::logical_expr::LogicalPlan;

/// Translates DataFusion LogicalPlan to differential dataflow operators
/// 
/// This module provides translation from SQL logical plans to
/// differential dataflow computation graphs.

/// Operator type labels for describing the translation
#[derive(Debug, Clone)]
pub enum OperatorType {
    Input,
    Map,
    Filter,
    Reduce,
    Join,
    Sort,
}

/// Describe what operators would be created from a LogicalPlan
/// 
/// This walks the plan tree and returns a human-readable description
/// of the operator chain that would be built.
pub fn describe_translation(plan: &LogicalPlan) -> Result<String, String> {
    let chain = describe_operator_chain(plan, 0)?;
    Ok(format!("Operator chain:\n{}", chain.join("\n")))
}

fn describe_operator_chain(plan: &LogicalPlan, depth: usize) -> Result<Vec<String>, String> {
    let mut chain = Vec::new();
    let indent = "  ".repeat(depth);
    
    let op_type = match plan {
        LogicalPlan::TableScan(_) => OperatorType::Input,
        LogicalPlan::Projection(_) => OperatorType::Map,
        LogicalPlan::Filter(_) => OperatorType::Filter,
        LogicalPlan::Aggregate(_) => OperatorType::Reduce,
        LogicalPlan::Join(_) => OperatorType::Join,
        LogicalPlan::Sort(_) => OperatorType::Sort,
        _ => return Err(format!("Unsupported logical plan operator: {:?}", plan)),
    };
    
    chain.push(format!("{}{:?}", indent, op_type));
    
    // Recursively describe inputs
    for input in plan.inputs() {
        let mut input_chain = describe_operator_chain(input, depth + 1)?;
        chain.append(&mut input_chain);
    }
    
    Ok(chain)
}

