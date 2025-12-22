//! SPARQL Parser NIF wrapper for TripleStore
//!
//! This module provides the Rust NIF interface for parsing SPARQL queries
//! using the spargebra crate from the Oxigraph project. The parser converts
//! SPARQL query strings into an Elixir-native AST representation.

use rustler::{Encoder, Env, NifResult, Term};
use spargebra::{GraphUpdateOperation, Query, Update};
use spargebra::algebra::{
    AggregateExpression, AggregateFunction, Expression, Function, GraphPattern,
    GraphTarget, OrderExpression, PropertyPathExpression,
};
use spargebra::term::{
    BlankNode, GraphName, GraphNamePattern, GroundQuad, GroundQuadPattern,
    GroundSubject, GroundTerm, GroundTermPattern, Literal, NamedNode, NamedNodePattern,
    Quad, QuadPattern, Subject, TermPattern, TriplePattern, Variable,
};
use oxiri::Iri;

/// Atoms for Elixir interop
mod atoms {
    rustler::atoms! {
        ok,
        error,

        // Query types
        select,
        construct,
        ask,
        describe,

        // Pattern types
        bgp,
        join,
        left_join,
        union,
        minus,
        filter,
        graph,
        extend,
        service,
        group,
        values,
        order_by,
        project,
        distinct,
        reduced,
        slice,
        path,

        // Term types
        variable,
        named_node,
        blank_node,
        literal,
        triple,

        // Literal types
        simple,
        language_tagged,
        typed,

        // Expression types
        or,
        and,
        equal,
        same_term,
        greater,
        greater_or_equal,
        less,
        less_or_equal,
        add,
        subtract,
        multiply,
        divide,
        unary_plus,
        unary_minus,
        not,
        bound,
        if_expr,
        coalesce,
        function_call,
        exists,
        not_exists,
        in_expr,

        // Aggregate functions
        count,
        count_solutions,
        sum,
        min,
        max,
        avg,
        sample,
        group_concat,
        custom,

        // Order direction
        asc,
        desc,

        // Parse error types
        parse_error,

        // Update operation types
        update,
        insert_data,
        delete_data,
        delete_insert,
        load,
        clear,
        create,
        drop,

        // Graph targets
        default_graph,
        named_graph,
        all_graphs,
        all_named,

        // Quad/graph-related
        quad,
    }
}

/// Placeholder function to verify NIF loads correctly.
#[rustler::nif]
fn nif_loaded() -> &'static str {
    "sparql_parser_nif"
}

/// Parses a SPARQL query string into an Elixir AST.
///
/// # Arguments
/// * `sparql` - The SPARQL query string to parse
///
/// # Returns
/// * `{:ok, ast}` on success where ast is the Elixir representation
/// * `{:error, {:parse_error, message}}` on parse failure
#[rustler::nif]
fn parse_query<'a>(env: Env<'a>, sparql: &str) -> NifResult<Term<'a>> {
    match Query::parse(sparql, None) {
        Ok(query) => {
            let ast = query_to_term(env, &query);
            Ok((atoms::ok(), ast).encode(env))
        }
        Err(e) => {
            let error_msg = e.to_string();
            Ok((atoms::error(), (atoms::parse_error(), error_msg)).encode(env))
        }
    }
}

/// Parses a SPARQL UPDATE string into an Elixir AST.
///
/// # Arguments
/// * `sparql` - The SPARQL UPDATE string to parse
///
/// # Returns
/// * `{:ok, ast}` on success where ast is the Elixir representation
/// * `{:error, {:parse_error, message}}` on parse failure
#[rustler::nif]
fn parse_update<'a>(env: Env<'a>, sparql: &str) -> NifResult<Term<'a>> {
    match Update::parse(sparql, None) {
        Ok(update) => {
            let ast = update_to_term(env, &update);
            Ok((atoms::ok(), ast).encode(env))
        }
        Err(e) => {
            let error_msg = e.to_string();
            Ok((atoms::error(), (atoms::parse_error(), error_msg)).encode(env))
        }
    }
}

/// Converts a spargebra Query to an Elixir term.
fn query_to_term<'a>(env: Env<'a>, query: &Query) -> Term<'a> {
    match query {
        Query::Select {
            dataset,
            pattern,
            base_iri,
        } => {
            let pattern_term = graph_pattern_to_term(env, pattern);
            let dataset_term = option_to_term(env, dataset, |e, d| query_dataset_to_term(e, d));
            let base_term = option_iri_to_term(env, base_iri);

            (
                atoms::select(),
                vec![
                    ("pattern", pattern_term),
                    ("dataset", dataset_term),
                    ("base_iri", base_term),
                ],
            ).encode(env)
        }
        Query::Construct {
            template,
            dataset,
            pattern,
            base_iri,
        } => {
            let template_term = construct_template_to_term(env, template);
            let pattern_term = graph_pattern_to_term(env, pattern);
            let dataset_term = option_to_term(env, dataset, |e, d| query_dataset_to_term(e, d));
            let base_term = option_iri_to_term(env, base_iri);

            (
                atoms::construct(),
                vec![
                    ("template", template_term),
                    ("pattern", pattern_term),
                    ("dataset", dataset_term),
                    ("base_iri", base_term),
                ],
            ).encode(env)
        }
        Query::Ask {
            dataset,
            pattern,
            base_iri,
        } => {
            let pattern_term = graph_pattern_to_term(env, pattern);
            let dataset_term = option_to_term(env, dataset, |e, d| query_dataset_to_term(e, d));
            let base_term = option_iri_to_term(env, base_iri);

            (
                atoms::ask(),
                vec![
                    ("pattern", pattern_term),
                    ("dataset", dataset_term),
                    ("base_iri", base_term),
                ],
            ).encode(env)
        }
        Query::Describe {
            dataset,
            pattern,
            base_iri,
        } => {
            let pattern_term = graph_pattern_to_term(env, pattern);
            let dataset_term = option_to_term(env, dataset, |e, d| query_dataset_to_term(e, d));
            let base_term = option_iri_to_term(env, base_iri);

            (
                atoms::describe(),
                vec![
                    ("pattern", pattern_term),
                    ("dataset", dataset_term),
                    ("base_iri", base_term),
                ],
            ).encode(env)
        }
    }
}

/// Converts a GraphPattern to an Elixir term.
fn graph_pattern_to_term<'a>(env: Env<'a>, pattern: &GraphPattern) -> Term<'a> {
    match pattern {
        GraphPattern::Bgp { patterns } => {
            let triple_terms: Vec<Term<'a>> = patterns
                .iter()
                .map(|tp| triple_pattern_to_term(env, tp))
                .collect();
            (atoms::bgp(), triple_terms).encode(env)
        }
        GraphPattern::Path {
            subject,
            path,
            object,
        } => {
            let subject_term = term_pattern_to_term(env, subject);
            let path_term = property_path_to_term(env, path);
            let object_term = term_pattern_to_term(env, object);
            (atoms::path(), subject_term, path_term, object_term).encode(env)
        }
        GraphPattern::Join { left, right } => {
            let left_term = graph_pattern_to_term(env, left);
            let right_term = graph_pattern_to_term(env, right);
            (atoms::join(), left_term, right_term).encode(env)
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            let left_term = graph_pattern_to_term(env, left);
            let right_term = graph_pattern_to_term(env, right);
            let expr_term = option_expression_to_term(env, expression);
            (atoms::left_join(), left_term, right_term, expr_term).encode(env)
        }
        GraphPattern::Filter { expr, inner } => {
            let expr_term = expression_to_term(env, expr);
            let inner_term = graph_pattern_to_term(env, inner);
            (atoms::filter(), expr_term, inner_term).encode(env)
        }
        GraphPattern::Union { left, right } => {
            let left_term = graph_pattern_to_term(env, left);
            let right_term = graph_pattern_to_term(env, right);
            (atoms::union(), left_term, right_term).encode(env)
        }
        GraphPattern::Minus { left, right } => {
            let left_term = graph_pattern_to_term(env, left);
            let right_term = graph_pattern_to_term(env, right);
            (atoms::minus(), left_term, right_term).encode(env)
        }
        GraphPattern::Graph { name, inner } => {
            let name_term = named_node_pattern_to_term(env, name);
            let inner_term = graph_pattern_to_term(env, inner);
            (atoms::graph(), name_term, inner_term).encode(env)
        }
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => {
            let inner_term = graph_pattern_to_term(env, inner);
            let var_term = variable_to_term(env, variable);
            let expr_term = expression_to_term(env, expression);
            (atoms::extend(), inner_term, var_term, expr_term).encode(env)
        }
        GraphPattern::Service {
            name,
            inner,
            silent,
        } => {
            let name_term = named_node_pattern_to_term(env, name);
            let inner_term = graph_pattern_to_term(env, inner);
            (atoms::service(), name_term, inner_term, *silent).encode(env)
        }
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => {
            let inner_term = graph_pattern_to_term(env, inner);
            let vars_term: Vec<Term<'a>> = variables
                .iter()
                .map(|v| variable_to_term(env, v))
                .collect();
            let aggs_term: Vec<Term<'a>> = aggregates
                .iter()
                .map(|(var, agg)| {
                    let var_term = variable_to_term(env, var);
                    let agg_term = aggregate_expression_to_term(env, agg);
                    (var_term, agg_term).encode(env)
                })
                .collect();
            (atoms::group(), inner_term, vars_term, aggs_term).encode(env)
        }
        GraphPattern::Values {
            variables,
            bindings,
        } => {
            let vars_term: Vec<Term<'a>> = variables
                .iter()
                .map(|v| variable_to_term(env, v))
                .collect();
            let bindings_term: Vec<Term<'a>> = bindings
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|opt| match opt {
                            Some(t) => ground_term_to_term(env, t),
                            None => rustler::types::atom::nil().encode(env),
                        })
                        .collect::<Vec<_>>()
                        .encode(env)
                })
                .collect();
            (atoms::values(), vars_term, bindings_term).encode(env)
        }
        GraphPattern::OrderBy { inner, expression } => {
            let inner_term = graph_pattern_to_term(env, inner);
            let order_terms: Vec<Term<'a>> = expression
                .iter()
                .map(|oc| order_expression_to_term(env, oc))
                .collect();
            (atoms::order_by(), inner_term, order_terms).encode(env)
        }
        GraphPattern::Project { inner, variables } => {
            let inner_term = graph_pattern_to_term(env, inner);
            let vars_term: Vec<Term<'a>> = variables
                .iter()
                .map(|v| variable_to_term(env, v))
                .collect();
            (atoms::project(), inner_term, vars_term).encode(env)
        }
        GraphPattern::Distinct { inner } => {
            let inner_term = graph_pattern_to_term(env, inner);
            (atoms::distinct(), inner_term).encode(env)
        }
        GraphPattern::Reduced { inner } => {
            let inner_term = graph_pattern_to_term(env, inner);
            (atoms::reduced(), inner_term).encode(env)
        }
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => {
            let inner_term = graph_pattern_to_term(env, inner);
            let length_term = match length {
                Some(l) => (*l as i64).encode(env),
                None => rustler::types::atom::nil().encode(env),
            };
            (atoms::slice(), inner_term, *start as i64, length_term).encode(env)
        }
    }
}

/// Converts a TriplePattern to an Elixir term.
fn triple_pattern_to_term<'a>(env: Env<'a>, tp: &TriplePattern) -> Term<'a> {
    let subject = term_pattern_to_term(env, &tp.subject);
    let predicate = named_node_pattern_to_term(env, &tp.predicate);
    let object = term_pattern_to_term(env, &tp.object);
    (atoms::triple(), subject, predicate, object).encode(env)
}

/// Converts a TermPattern (subject/object position) to an Elixir term.
fn term_pattern_to_term<'a>(env: Env<'a>, tp: &TermPattern) -> Term<'a> {
    match tp {
        TermPattern::NamedNode(nn) => named_node_to_term(env, nn),
        TermPattern::BlankNode(bn) => blank_node_to_term(env, bn),
        TermPattern::Literal(lit) => literal_to_term(env, lit),
        TermPattern::Variable(var) => variable_to_term(env, var),
    }
}

/// Converts a NamedNodePattern (predicate position) to an Elixir term.
fn named_node_pattern_to_term<'a>(env: Env<'a>, nnp: &NamedNodePattern) -> Term<'a> {
    match nnp {
        NamedNodePattern::NamedNode(nn) => named_node_to_term(env, nn),
        NamedNodePattern::Variable(var) => variable_to_term(env, var),
    }
}

/// Converts a GroundTerm to an Elixir term.
fn ground_term_to_term<'a>(env: Env<'a>, gt: &GroundTerm) -> Term<'a> {
    match gt {
        GroundTerm::NamedNode(nn) => named_node_to_term(env, nn),
        GroundTerm::Literal(lit) => literal_to_term(env, lit),
    }
}

/// Converts a NamedNode (IRI) to an Elixir term.
fn named_node_to_term<'a>(env: Env<'a>, nn: &NamedNode) -> Term<'a> {
    (atoms::named_node(), nn.as_str()).encode(env)
}

/// Converts a BlankNode to an Elixir term.
fn blank_node_to_term<'a>(env: Env<'a>, bn: &BlankNode) -> Term<'a> {
    (atoms::blank_node(), bn.as_str()).encode(env)
}

/// Converts a Literal to an Elixir term.
fn literal_to_term<'a>(env: Env<'a>, lit: &Literal) -> Term<'a> {
    let value = lit.value();

    if let Some(lang) = lit.language() {
        // Language-tagged literal
        (atoms::literal(), atoms::language_tagged(), value, lang).encode(env)
    } else {
        let datatype = lit.datatype();
        let datatype_str = datatype.as_str();

        // Check if it's a simple literal (xsd:string)
        if datatype_str == "http://www.w3.org/2001/XMLSchema#string" {
            (atoms::literal(), atoms::simple(), value).encode(env)
        } else {
            // Typed literal
            (atoms::literal(), atoms::typed(), value, datatype_str).encode(env)
        }
    }
}

/// Converts a Variable to an Elixir term.
fn variable_to_term<'a>(env: Env<'a>, var: &Variable) -> Term<'a> {
    (atoms::variable(), var.as_str()).encode(env)
}

/// Converts an Expression to an Elixir term.
fn expression_to_term<'a>(env: Env<'a>, expr: &Expression) -> Term<'a> {
    match expr {
        Expression::NamedNode(nn) => named_node_to_term(env, nn),
        Expression::Literal(lit) => literal_to_term(env, lit),
        Expression::Variable(var) => variable_to_term(env, var),
        Expression::Or(left, right) => {
            let left_term = expression_to_term(env, left);
            let right_term = expression_to_term(env, right);
            (atoms::or(), left_term, right_term).encode(env)
        }
        Expression::And(left, right) => {
            let left_term = expression_to_term(env, left);
            let right_term = expression_to_term(env, right);
            (atoms::and(), left_term, right_term).encode(env)
        }
        Expression::Equal(left, right) => {
            let left_term = expression_to_term(env, left);
            let right_term = expression_to_term(env, right);
            (atoms::equal(), left_term, right_term).encode(env)
        }
        Expression::SameTerm(left, right) => {
            let left_term = expression_to_term(env, left);
            let right_term = expression_to_term(env, right);
            (atoms::same_term(), left_term, right_term).encode(env)
        }
        Expression::Greater(left, right) => {
            let left_term = expression_to_term(env, left);
            let right_term = expression_to_term(env, right);
            (atoms::greater(), left_term, right_term).encode(env)
        }
        Expression::GreaterOrEqual(left, right) => {
            let left_term = expression_to_term(env, left);
            let right_term = expression_to_term(env, right);
            (atoms::greater_or_equal(), left_term, right_term).encode(env)
        }
        Expression::Less(left, right) => {
            let left_term = expression_to_term(env, left);
            let right_term = expression_to_term(env, right);
            (atoms::less(), left_term, right_term).encode(env)
        }
        Expression::LessOrEqual(left, right) => {
            let left_term = expression_to_term(env, left);
            let right_term = expression_to_term(env, right);
            (atoms::less_or_equal(), left_term, right_term).encode(env)
        }
        Expression::Add(left, right) => {
            let left_term = expression_to_term(env, left);
            let right_term = expression_to_term(env, right);
            (atoms::add(), left_term, right_term).encode(env)
        }
        Expression::Subtract(left, right) => {
            let left_term = expression_to_term(env, left);
            let right_term = expression_to_term(env, right);
            (atoms::subtract(), left_term, right_term).encode(env)
        }
        Expression::Multiply(left, right) => {
            let left_term = expression_to_term(env, left);
            let right_term = expression_to_term(env, right);
            (atoms::multiply(), left_term, right_term).encode(env)
        }
        Expression::Divide(left, right) => {
            let left_term = expression_to_term(env, left);
            let right_term = expression_to_term(env, right);
            (atoms::divide(), left_term, right_term).encode(env)
        }
        Expression::UnaryPlus(inner) => {
            let inner_term = expression_to_term(env, inner);
            (atoms::unary_plus(), inner_term).encode(env)
        }
        Expression::UnaryMinus(inner) => {
            let inner_term = expression_to_term(env, inner);
            (atoms::unary_minus(), inner_term).encode(env)
        }
        Expression::Not(inner) => {
            let inner_term = expression_to_term(env, inner);
            (atoms::not(), inner_term).encode(env)
        }
        Expression::Bound(var) => {
            let var_term = variable_to_term(env, var);
            (atoms::bound(), var_term).encode(env)
        }
        Expression::If(cond, then_expr, else_expr) => {
            let cond_term = expression_to_term(env, cond);
            let then_term = expression_to_term(env, then_expr);
            let else_term = expression_to_term(env, else_expr);
            (atoms::if_expr(), cond_term, then_term, else_term).encode(env)
        }
        Expression::Coalesce(exprs) => {
            let expr_terms: Vec<Term<'a>> = exprs
                .iter()
                .map(|e| expression_to_term(env, e))
                .collect();
            (atoms::coalesce(), expr_terms).encode(env)
        }
        Expression::FunctionCall(func, args) => {
            let func_term = function_to_term(env, func);
            let arg_terms: Vec<Term<'a>> = args
                .iter()
                .map(|a| expression_to_term(env, a))
                .collect();
            (atoms::function_call(), func_term, arg_terms).encode(env)
        }
        Expression::Exists(pattern) => {
            let pattern_term = graph_pattern_to_term(env, pattern);
            (atoms::exists(), pattern_term).encode(env)
        }
        Expression::In(expr, list) => {
            let expr_term = expression_to_term(env, expr);
            let list_terms: Vec<Term<'a>> = list
                .iter()
                .map(|e| expression_to_term(env, e))
                .collect();
            (atoms::in_expr(), expr_term, list_terms).encode(env)
        }
    }
}

/// Converts a Function to an Elixir term.
fn function_to_term<'a>(env: Env<'a>, func: &Function) -> Term<'a> {
    let func_name = match func {
        Function::Str => "STR",
        Function::Lang => "LANG",
        Function::LangMatches => "LANGMATCHES",
        Function::Datatype => "DATATYPE",
        Function::Iri => "IRI",
        Function::BNode => "BNODE",
        Function::Rand => "RAND",
        Function::Abs => "ABS",
        Function::Ceil => "CEIL",
        Function::Floor => "FLOOR",
        Function::Round => "ROUND",
        Function::Concat => "CONCAT",
        Function::SubStr => "SUBSTR",
        Function::StrLen => "STRLEN",
        Function::Replace => "REPLACE",
        Function::UCase => "UCASE",
        Function::LCase => "LCASE",
        Function::EncodeForUri => "ENCODE_FOR_URI",
        Function::Contains => "CONTAINS",
        Function::StrStarts => "STRSTARTS",
        Function::StrEnds => "STRENDS",
        Function::StrBefore => "STRBEFORE",
        Function::StrAfter => "STRAFTER",
        Function::Year => "YEAR",
        Function::Month => "MONTH",
        Function::Day => "DAY",
        Function::Hours => "HOURS",
        Function::Minutes => "MINUTES",
        Function::Seconds => "SECONDS",
        Function::Timezone => "TIMEZONE",
        Function::Tz => "TZ",
        Function::Now => "NOW",
        Function::Uuid => "UUID",
        Function::StrUuid => "STRUUID",
        Function::Md5 => "MD5",
        Function::Sha1 => "SHA1",
        Function::Sha256 => "SHA256",
        Function::Sha384 => "SHA384",
        Function::Sha512 => "SHA512",
        Function::StrLang => "STRLANG",
        Function::StrDt => "STRDT",
        Function::IsIri => "ISIRI",
        Function::IsBlank => "ISBLANK",
        Function::IsLiteral => "ISLITERAL",
        Function::IsNumeric => "ISNUMERIC",
        Function::Regex => "REGEX",
        Function::Custom(iri) => {
            return (atoms::custom(), iri.as_str()).encode(env);
        }
    };
    func_name.encode(env)
}

/// Converts an AggregateExpression to an Elixir term.
fn aggregate_expression_to_term<'a>(env: Env<'a>, agg: &AggregateExpression) -> Term<'a> {
    match agg {
        AggregateExpression::CountSolutions { distinct } => {
            // COUNT(*) or COUNT(DISTINCT *)
            (atoms::count_solutions(), *distinct).encode(env)
        }
        AggregateExpression::FunctionCall { name, expr, distinct } => {
            let func_term = aggregate_function_to_term(env, name);
            let expr_term = expression_to_term(env, expr);
            (func_term, expr_term, *distinct).encode(env)
        }
    }
}

/// Converts an AggregateFunction to an Elixir term.
fn aggregate_function_to_term<'a>(env: Env<'a>, func: &AggregateFunction) -> Term<'a> {
    match func {
        AggregateFunction::Count => atoms::count().encode(env),
        AggregateFunction::Sum => atoms::sum().encode(env),
        AggregateFunction::Min => atoms::min().encode(env),
        AggregateFunction::Max => atoms::max().encode(env),
        AggregateFunction::Avg => atoms::avg().encode(env),
        AggregateFunction::Sample => atoms::sample().encode(env),
        AggregateFunction::GroupConcat { separator } => {
            let sep_term = match separator {
                Some(s) => s.as_str().encode(env),
                None => rustler::types::atom::nil().encode(env),
            };
            (atoms::group_concat(), sep_term).encode(env)
        }
        AggregateFunction::Custom(iri) => {
            (atoms::custom(), iri.as_str()).encode(env)
        }
    }
}

/// Converts an OrderExpression to an Elixir term.
fn order_expression_to_term<'a>(env: Env<'a>, oe: &OrderExpression) -> Term<'a> {
    match oe {
        OrderExpression::Asc(expr) => {
            let expr_term = expression_to_term(env, expr);
            (atoms::asc(), expr_term).encode(env)
        }
        OrderExpression::Desc(expr) => {
            let expr_term = expression_to_term(env, expr);
            (atoms::desc(), expr_term).encode(env)
        }
    }
}

/// Converts a PropertyPath to an Elixir term.
fn property_path_to_term<'a>(env: Env<'a>, path: &PropertyPathExpression) -> Term<'a> {
    match path {
        PropertyPathExpression::NamedNode(nn) => named_node_to_term(env, nn),
        PropertyPathExpression::Reverse(inner) => {
            let inner_term = property_path_to_term(env, inner);
            ("reverse", inner_term).encode(env)
        }
        PropertyPathExpression::Sequence(left, right) => {
            let left_term = property_path_to_term(env, left);
            let right_term = property_path_to_term(env, right);
            ("sequence", left_term, right_term).encode(env)
        }
        PropertyPathExpression::Alternative(left, right) => {
            let left_term = property_path_to_term(env, left);
            let right_term = property_path_to_term(env, right);
            ("alternative", left_term, right_term).encode(env)
        }
        PropertyPathExpression::ZeroOrMore(inner) => {
            let inner_term = property_path_to_term(env, inner);
            ("zero_or_more", inner_term).encode(env)
        }
        PropertyPathExpression::OneOrMore(inner) => {
            let inner_term = property_path_to_term(env, inner);
            ("one_or_more", inner_term).encode(env)
        }
        PropertyPathExpression::ZeroOrOne(inner) => {
            let inner_term = property_path_to_term(env, inner);
            ("zero_or_one", inner_term).encode(env)
        }
        PropertyPathExpression::NegatedPropertySet(nodes) => {
            let node_terms: Vec<Term<'a>> = nodes
                .iter()
                .map(|nn| named_node_to_term(env, nn))
                .collect();
            ("negated_property_set", node_terms).encode(env)
        }
    }
}

/// Converts a QueryDataset to an Elixir term.
fn query_dataset_to_term<'a>(env: Env<'a>, dataset: &spargebra::algebra::QueryDataset) -> Term<'a> {
    let default_graphs: Vec<Term<'a>> = dataset
        .default
        .iter()
        .map(|nn| named_node_to_term(env, nn))
        .collect();
    let named_graphs: Vec<Term<'a>> = dataset
        .named
        .as_ref()
        .map(|graphs| graphs.iter().map(|nn| named_node_to_term(env, nn)).collect())
        .unwrap_or_default();

    vec![
        ("default", default_graphs.encode(env)),
        ("named", named_graphs.encode(env)),
    ].encode(env)
}

/// Helper for optional values.
fn option_to_term<'a, T, F>(env: Env<'a>, opt: &Option<T>, f: F) -> Term<'a>
where
    F: FnOnce(Env<'a>, &T) -> Term<'a>,
{
    match opt {
        Some(v) => f(env, v),
        None => rustler::types::atom::nil().encode(env),
    }
}

/// Converts an optional base IRI (oxiri::Iri) to an Elixir term.
fn option_iri_to_term<'a>(env: Env<'a>, iri: &Option<Iri<String>>) -> Term<'a> {
    match iri {
        Some(i) => (atoms::named_node(), i.as_str()).encode(env),
        None => rustler::types::atom::nil().encode(env),
    }
}

/// Converts an optional Expression to an Elixir term.
fn option_expression_to_term<'a>(env: Env<'a>, expr: &Option<Expression>) -> Term<'a> {
    match expr {
        Some(e) => expression_to_term(env, e),
        None => rustler::types::atom::nil().encode(env),
    }
}

/// Converts a CONSTRUCT template to an Elixir term.
fn construct_template_to_term<'a>(env: Env<'a>, template: &[TriplePattern]) -> Term<'a> {
    let triple_terms: Vec<Term<'a>> = template
        .iter()
        .map(|tp| triple_pattern_to_term(env, tp))
        .collect();
    triple_terms.encode(env)
}

// ===========================================================================
// UPDATE Conversion Functions
// ===========================================================================

/// Converts a spargebra Update to an Elixir term.
fn update_to_term<'a>(env: Env<'a>, update: &Update) -> Term<'a> {
    let operations: Vec<Term<'a>> = update
        .operations
        .iter()
        .map(|op| graph_update_operation_to_term(env, op))
        .collect();

    let base_term = option_iri_to_term(env, &update.base_iri);

    (
        atoms::update(),
        vec![
            ("operations", operations.encode(env)),
            ("base_iri", base_term),
        ],
    ).encode(env)
}

/// Converts a GraphUpdateOperation to an Elixir term.
fn graph_update_operation_to_term<'a>(env: Env<'a>, op: &GraphUpdateOperation) -> Term<'a> {
    match op {
        GraphUpdateOperation::InsertData { data } => {
            let quads: Vec<Term<'a>> = data
                .iter()
                .map(|q| quad_to_term(env, q))
                .collect();
            (atoms::insert_data(), quads).encode(env)
        }
        GraphUpdateOperation::DeleteData { data } => {
            let quads: Vec<Term<'a>> = data
                .iter()
                .map(|q| ground_quad_to_term(env, q))
                .collect();
            (atoms::delete_data(), quads).encode(env)
        }
        GraphUpdateOperation::DeleteInsert {
            delete,
            insert,
            using,
            pattern,
        } => {
            let delete_terms: Vec<Term<'a>> = delete
                .iter()
                .map(|q| ground_quad_pattern_to_term(env, q))
                .collect();
            let insert_terms: Vec<Term<'a>> = insert
                .iter()
                .map(|q| quad_pattern_to_term(env, q))
                .collect();
            let using_term = option_to_term(env, using, |e, d| query_dataset_to_term(e, d));
            let pattern_term = graph_pattern_to_term(env, pattern);

            (
                atoms::delete_insert(),
                vec![
                    ("delete", delete_terms.encode(env)),
                    ("insert", insert_terms.encode(env)),
                    ("using", using_term),
                    ("pattern", pattern_term),
                ],
            ).encode(env)
        }
        GraphUpdateOperation::Load {
            silent,
            source,
            destination,
        } => {
            let source_term = named_node_to_term(env, source);
            let dest_term = graph_name_to_term(env, destination);
            (
                atoms::load(),
                vec![
                    ("silent", silent.encode(env)),
                    ("source", source_term),
                    ("destination", dest_term),
                ],
            ).encode(env)
        }
        GraphUpdateOperation::Clear { silent, graph } => {
            let graph_term = graph_target_to_term(env, graph);
            (
                atoms::clear(),
                vec![
                    ("silent", silent.encode(env)),
                    ("graph", graph_term),
                ],
            ).encode(env)
        }
        GraphUpdateOperation::Create { silent, graph } => {
            let graph_term = named_node_to_term(env, graph);
            (
                atoms::create(),
                vec![
                    ("silent", silent.encode(env)),
                    ("graph", graph_term),
                ],
            ).encode(env)
        }
        GraphUpdateOperation::Drop { silent, graph } => {
            let graph_term = graph_target_to_term(env, graph);
            (
                atoms::drop(),
                vec![
                    ("silent", silent.encode(env)),
                    ("graph", graph_term),
                ],
            ).encode(env)
        }
    }
}

/// Converts a Quad to an Elixir term.
fn quad_to_term<'a>(env: Env<'a>, quad: &Quad) -> Term<'a> {
    let subject = subject_to_term(env, &quad.subject);
    let predicate = named_node_to_term(env, &quad.predicate);
    let object = spargebra_term_to_elixir_term(env, &quad.object);
    let graph = graph_name_to_term(env, &quad.graph_name);
    (atoms::quad(), subject, predicate, object, graph).encode(env)
}

/// Converts a GroundQuad to an Elixir term.
fn ground_quad_to_term<'a>(env: Env<'a>, quad: &GroundQuad) -> Term<'a> {
    let subject = ground_subject_to_term(env, &quad.subject);
    let predicate = named_node_to_term(env, &quad.predicate);
    let object = ground_term_to_term(env, &quad.object);
    let graph = graph_name_to_term(env, &quad.graph_name);
    (atoms::quad(), subject, predicate, object, graph).encode(env)
}

/// Converts a QuadPattern to an Elixir term.
fn quad_pattern_to_term<'a>(env: Env<'a>, quad: &QuadPattern) -> Term<'a> {
    let subject = term_pattern_to_term(env, &quad.subject);
    let predicate = named_node_pattern_to_term(env, &quad.predicate);
    let object = term_pattern_to_term(env, &quad.object);
    let graph = graph_name_pattern_to_term(env, &quad.graph_name);
    (atoms::quad(), subject, predicate, object, graph).encode(env)
}

/// Converts a GroundQuadPattern to an Elixir term.
fn ground_quad_pattern_to_term<'a>(env: Env<'a>, quad: &GroundQuadPattern) -> Term<'a> {
    let subject = ground_term_pattern_to_term(env, &quad.subject);
    let predicate = named_node_pattern_to_term(env, &quad.predicate);
    let object = ground_term_pattern_to_term(env, &quad.object);
    let graph = graph_name_pattern_to_term(env, &quad.graph_name);
    (atoms::quad(), subject, predicate, object, graph).encode(env)
}

/// Converts a Subject to an Elixir term.
fn subject_to_term<'a>(env: Env<'a>, subject: &Subject) -> Term<'a> {
    match subject {
        Subject::NamedNode(nn) => named_node_to_term(env, nn),
        Subject::BlankNode(bn) => blank_node_to_term(env, bn),
    }
}

/// Converts a GroundSubject to an Elixir term.
fn ground_subject_to_term<'a>(env: Env<'a>, subject: &GroundSubject) -> Term<'a> {
    match subject {
        GroundSubject::NamedNode(nn) => named_node_to_term(env, nn),
    }
}

/// Converts a spargebra Term to an Elixir term.
fn spargebra_term_to_elixir_term<'a>(env: Env<'a>, term: &spargebra::term::Term) -> Term<'a> {
    match term {
        spargebra::term::Term::NamedNode(nn) => named_node_to_term(env, nn),
        spargebra::term::Term::BlankNode(bn) => blank_node_to_term(env, bn),
        spargebra::term::Term::Literal(lit) => literal_to_term(env, lit),
    }
}

/// Converts a GroundTermPattern to an Elixir term.
fn ground_term_pattern_to_term<'a>(env: Env<'a>, term: &GroundTermPattern) -> Term<'a> {
    match term {
        GroundTermPattern::NamedNode(nn) => named_node_to_term(env, nn),
        GroundTermPattern::Literal(lit) => literal_to_term(env, lit),
        GroundTermPattern::Variable(var) => variable_to_term(env, var),
    }
}

/// Converts a GraphName to an Elixir term.
fn graph_name_to_term<'a>(env: Env<'a>, graph: &GraphName) -> Term<'a> {
    match graph {
        GraphName::NamedNode(nn) => (atoms::named_graph(), nn.as_str()).encode(env),
        GraphName::DefaultGraph => atoms::default_graph().encode(env),
    }
}

/// Converts a GraphNamePattern to an Elixir term.
fn graph_name_pattern_to_term<'a>(env: Env<'a>, graph: &GraphNamePattern) -> Term<'a> {
    match graph {
        GraphNamePattern::NamedNode(nn) => (atoms::named_graph(), nn.as_str()).encode(env),
        GraphNamePattern::DefaultGraph => atoms::default_graph().encode(env),
        GraphNamePattern::Variable(var) => variable_to_term(env, var),
    }
}

/// Converts a GraphTarget to an Elixir term.
fn graph_target_to_term<'a>(env: Env<'a>, target: &GraphTarget) -> Term<'a> {
    match target {
        GraphTarget::NamedNode(nn) => (atoms::named_graph(), nn.as_str()).encode(env),
        GraphTarget::DefaultGraph => atoms::default_graph().encode(env),
        GraphTarget::NamedGraphs => atoms::all_named().encode(env),
        GraphTarget::AllGraphs => atoms::all_graphs().encode(env),
    }
}

rustler::init!("Elixir.TripleStore.SPARQL.Parser.NIF", [nif_loaded, parse_query, parse_update]);
