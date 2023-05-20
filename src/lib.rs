use regex::Regex;
use once_cell::sync::OnceCell;
use eyre::ContextCompat;
use serde::{Serialize, Deserialize};
use serde_json::Value;

use fluvio_smartmodule::{
    smartmodule, Result, Record, RecordData,
    dataplane::smartmodule::{
        SmartModuleExtraParams, SmartModuleInitError
    },
    eyre
};

static REGEX: OnceCell<Vec<RegexMatch>> = OnceCell::new();
const PARAM_NAME: &str = "spec";

#[derive(Debug, Serialize, Deserialize)]
struct RegexParams {
    source: String,
    destination: String,
    regex: String
}

#[derive(Debug)]
struct RegexMatch {
    source: String,
    destination: String,
    re: Regex,  
}

/// Parse input paramters
fn get_params(params: SmartModuleExtraParams) -> Result<Vec<RegexParams>> {
    if let Some(raw_spec) = params.get(PARAM_NAME) {
        match serde_json::from_str(raw_spec) {
            Ok(regex_params) => {
                Ok(regex_params)
            }
            Err(err) => {
                eprintln!("unable to parse spec from params: {err:?}");
                Err(eyre!("cannot parse `spec` param: {:#?}", err))
            }
        }
    } else {
        Err(SmartModuleInitError::MissingParam(PARAM_NAME.to_string()).into())
    }
}

/// Use input parameters to compile a list of regular expressions
fn compile_regex_list(regex_params: Vec<RegexParams>) -> Result<Vec<RegexMatch>> {
    let mut result: Vec<RegexMatch> = vec![];

    for r in regex_params {
        let re = Regex::new(&r.regex.as_str())?;
        let res = RegexMatch {source: r.source, destination: r.destination, re};
        result.push(res);
    }
    Ok(result)
}

/// Extract json value based on JSON pointer notations:
///     [ "/top/one", "/top/two"]
fn extract_json_field(data: &str, lookup: &String) -> Result<String> {
    let json:Value = serde_json::from_str(data)?;

    // Extract value and convert to string, return empty string if none.
    let result = if let Some(val) = json.pointer(lookup.as_str()) {
        match val.as_str() {
            Some(s) => s.to_owned(),
            None => val.to_string()
        }
    } else {
        "".to_owned()
    };

    Ok(result)
}

/// Run regex to capture the value, and returne in a (target, value) pair
fn capture_regex_value(text: &String, regex: &Regex) -> Result<String> {
    let capture = match regex.captures(text.as_str()) {
        Some(caps) => caps.get(1).map_or("", |m| m.as_str()),
        None => ""
    };

    Ok(capture.to_string())
}

/// Merge json trees
fn merge_json(a: &mut Value, b: &Value) {
    match (a, b) {
        (&mut Value::Object(ref mut a), &Value::Object(ref b)) => {
            for (k, v) in b {
                merge_json(a.entry(k.clone()).or_insert(Value::Null), v);
            }
        }
        (a, b) => {
            *a = b.clone();
        }
    }
}

/// Recursive function that traverses the json tree to insert the value at proper hierarchy
///     "/root/one" -> "test" - inserts {"root": {"one": "text"}}
///     "/root" -> "test" - inserts {"root": "text"}
/// Note, if the path matches an existing value exists, that value is replaced.
fn add_json_key_value_recursive(json: &mut Value, key_path:String, new_value: Value ) {
    // Check json path
    // Found a match, merge json objects at this hiearchy
    let some_found_json = json.pointer_mut(key_path.as_str());
    if some_found_json.is_some() {
        return merge_json(some_found_json.unwrap(), &new_value);
    }
    
    // Peal off the leaf 
    // Use as key
    let mut path_array:Vec<_> = key_path.split('/').skip(1).collect();
    let some_key = path_array.pop();

    // No key (no neaf)
    // Merge with the top of the tree
    if some_key.is_none() {
        return merge_json(json, &new_value);
    }

    // Have key
    // Create new value storing previous key/val, and go again
    let mut v_map = serde_json::Map::new();
    v_map.insert(some_key.unwrap().to_owned(), new_value);
    let r_val = Value::Object(v_map);

    // No more path elements
    // Merge new value with the top of the tree
    if path_array.is_empty()  {
        return merge_json(json, &r_val);
    }

    // Build parent key-path and try again
    let new_key_path = format!("/{}", path_array.join("/"));
    add_json_key_value(json, new_key_path, r_val);

}

/// Project badly formatted key_path (without /) from wipping out the existing json
/// Improperly formatted destinations, leave the original json untouched
fn add_json_key_value(json: &mut Value, key_path:String, new_value: Value ) {
    if key_path.contains("/") {
        add_json_key_value_recursive(json, key_path, new_value)
    }
}

/// Traverse the regex list, extract JSON values, compute regex, and save output
fn compute_regex_append_values(record: &Record, regex_list: &Vec<RegexMatch>) -> Result<Value> {
    let data: &str = std::str::from_utf8(record.value.as_ref())?;
    let mut json:Value = serde_json::from_str(data)?;

    for regex in regex_list {

        // Skip if source doesn't exist
        let value = extract_json_field(data, &regex.source)?;
        if value.is_empty() {
            continue;
        }

        // Skip if regex does not compute
        let result = capture_regex_value(&value, &regex.re)?;
        if result.is_empty() {
            continue;
        }

        // update json record with the new values
        add_json_key_value(
            &mut json, 
            regex.destination.clone(), 
            Value::from(result)
        );
    }
    
    Ok(json)
}

#[smartmodule(map)]
pub fn map(record: &Record) -> Result<(Option<RecordData>, RecordData)> {
    let key = record.key.clone();
    let regex_list = REGEX.get().wrap_err("regex is not initialized")?;

    let result = compute_regex_append_values(record, regex_list)?;

    Ok((key, serde_json::to_string(&result)?.into()))
}

#[smartmodule(init)]
fn init(params: SmartModuleExtraParams) -> Result<()> {
    let regex_params = get_params(params)?;
    let regex_list = compile_regex_list(regex_params)?;

    REGEX.set(regex_list).expect("regex is already initialized");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    static INPUT: &str = r#"{
        "dedup_key": "6fcb9fe530c24613ed1df3e51c0e86addd794251f49ec6cd77fd4381cc0e0ac2",
        "description": "First: bk Second: 4 Third: 13 Fourth: Jack, tr Sec  [Encased string - (data)] (<a href='https://example.com/doc1/182031340621?pdf_header=&de_seq_num=44&caseid=456177'>9</a>)",
        "last_build_date": "Tue, 18 Apr 2023 15:00:01 GMT",
        "link": "https://www.example.comv/cgi-bin/DktRpt.pl?456177",
        "pub_date": "Mon, 17 Apr 2023 15:54:45 GMT",
        "title": "23-20670 Abby Lynn Hardy",
        "name": {
            "first": "Abby", 
            "last": "Hardy"
        }
    }"#;

    #[test]
    fn test_compile_regex_list() {
        let params = vec![RegexParams {
            regex: r#"(?i)Third:\s+(\w+)\b"#.to_owned(), 
            source: "src".to_owned(), 
            destination: "dst".to_owned()
        }];

        let result = compile_regex_list(params);
        assert_eq!(result.is_ok(), true);
    }

    #[test]
    fn capture_regex_value_test() {
        let input: &str = r#"First: bk Second: 4 Third: 13 Fourth: Jack, tr Sec  [Encased string - (data)] (<a href='https://example.com/doc1/182031340621?pdf_header=&de_seq_num=44&caseid=456177'>9</a>)"#;

        // First
        let re = Regex::new(r"(?i)First:\s+(\w+)\b").unwrap();
        let expected = "bk".to_owned();

        let result = capture_regex_value(&input.to_owned(), &re);
        assert_eq!(result.unwrap(), expected);

        // Second
        let re = Regex::new(r"(?i)Second:\s+(\w+)\b").unwrap();
        let expected = "4".to_owned();

        let result = capture_regex_value(&input.to_owned(), &re);
        assert_eq!(result.unwrap(), expected);

        // Third
        let re = Regex::new(r"(?i)Third:\s+(\w+)\b").unwrap();
        let expected = "13".to_owned();

        let result = capture_regex_value(&input.to_owned(), &re);
        assert_eq!(result.unwrap(), expected);

        // Fourth
        let re = Regex::new(r"(?i)Fourth:\s+([\w,\s\.\']*\S)\s*\[").unwrap();
        let expected = "Jack, tr Sec".to_owned();

        let result = capture_regex_value(&input.to_owned(), &re);
        assert_eq!(result.unwrap(), expected);

        // doc-link
        let re = Regex::new(r"href='([^']+)'").unwrap();
        let expected = "https://example.com/doc1/182031340621?pdf_header=&de_seq_num=44&caseid=456177".to_owned();

        let result = capture_regex_value(&input.to_owned(), &re);
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn extract_json_field_tests() {
        // string
        let lookup = "/description".to_owned();
        let result: &str = r#"First: bk Second: 4 Third: 13 Fourth: Jack, tr Sec  [Encased string - (data)] (<a href='https://example.com/doc1/182031340621?pdf_header=&de_seq_num=44&caseid=456177'>9</a>)"#;
        assert_eq!(result.to_owned(), extract_json_field(INPUT, &lookup).unwrap());

        // nested node
        let lookup = "/name/last".to_owned();
        let result = "Hardy";
        assert_eq!(result.to_owned(), extract_json_field(INPUT, &lookup).unwrap());

        // nested tree
        let lookup = "/name".to_owned();
        let result = r#"{"first": "Abby", "last": "Hardy"}"#;
        let expected: Value = serde_json::from_str(result).unwrap();
        assert_eq!(expected.to_string(), extract_json_field(INPUT, &lookup).unwrap());

        // invalid 
        let lookup = "/invalid".to_owned();
        let result = "";
        assert_eq!(result.to_owned(), extract_json_field(INPUT, &lookup).unwrap());
    }

    #[test]
    fn add_json_key_value_test() {

        // Test: Empty tree
        let mut json:Value = serde_json::from_str(r#"{}"#).unwrap();
        let key_path = "/root".to_owned();
        let new_v:Value = serde_json::json!("xyz");
        let expected:Value = serde_json::from_str(r#"{"root": "xyz"}"#).unwrap();

        add_json_key_value(&mut json, key_path, new_v);
        assert_eq!(json, expected);

        // Test: Empty tree
        let mut json:Value = serde_json::json!("");
        let key_path = "/root".to_owned();
        let new_v:Value = serde_json::json!("xyz");
        let expected:Value = serde_json::from_str(r#"{"root": "xyz"}"#).unwrap();

        add_json_key_value(&mut json, key_path, new_v);
        assert_eq!(json, expected);

        // Test: Invalid Node
        let mut json:Value = serde_json::json!("");
        let key_path = "root".to_owned();
        let new_v:Value = serde_json::json!("xyz");
        let expected:Value = serde_json::json!("");

        add_json_key_value(&mut json, key_path, new_v);
        assert_eq!(json, expected);

        // Test: Add peer leaf
        let mut json:Value = serde_json::from_str(r#"{"root": {"aaa" : 1 , "bbb": 2}}"#).unwrap();
        let key_path = "/root/ccc".to_owned();
        let new_v:Value = serde_json::json!(3);
        let expected :Value = serde_json::from_str(r#"{"root": {"aaa" : 1 , "bbb": 2, "ccc": 3}}"#).unwrap();

        add_json_key_value(&mut json, key_path, new_v);
        assert_eq!(json, expected);

        // Test: Add peer middle leave
        let mut json:Value = serde_json::from_str(r#"{"root": {"aaa" : {"bbb": 2}}}"#).unwrap();
        let key_path = "/root/ccc".to_owned();
        let new_v:Value = serde_json::json!(3);
        let expected :Value = serde_json::from_str(r#"{"root": {"aaa" : {"bbb": 2}, "ccc": 3}}"#).unwrap();

        add_json_key_value(&mut json, key_path, new_v);
        assert_eq!(json, expected);

        // Test: Add deep nested leave
        let mut json:Value = serde_json::from_str(r#"{"root": {"aaa" : {"bbb": 2}}}"#).unwrap();
        let key_path = "/root/aaa/ccc".to_owned();
        let new_v:Value = serde_json::json!(3);
        let expected :Value = serde_json::from_str(r#"{"root": {"aaa" : {"bbb": 2, "ccc": 3}}}"#).unwrap();

        add_json_key_value(&mut json, key_path, new_v);
        assert_eq!(json, expected);

        // Test: Swap content
        let mut json:Value = serde_json::from_str(r#"{"root": [{"aaa" : 1} , {"bbb": 2}]}"#).unwrap();
        let key_path = "/root/ccc".to_owned();
        let new_v:Value = serde_json::json!(3);
        let expected :Value = serde_json::from_str(r#"{"root": {"ccc": 3}}"#).unwrap();

        add_json_key_value(&mut json, key_path, new_v);
        assert_eq!(json, expected);

    }

    #[test]
    fn compute_regex_append_values_tests() {
        static EXPECTED: &str = r#"{
            "dedup_key": "6fcb9fe530c24613ed1df3e51c0e86addd794251f49ec6cd77fd4381cc0e0ac2",
            "description": "First: bk Second: 4 Third: 13 Fourth: Jack, tr Sec  [Encased string - (data)] (<a href='https://example.com/doc1/182031340621?pdf_header=&de_seq_num=44&caseid=456177'>9</a>)",
            "last_build_date": "Tue, 18 Apr 2023 15:00:01 GMT",
            "link": "https://www.example.comv/cgi-bin/DktRpt.pl?456177",
            "pub_date": "Mon, 17 Apr 2023 15:54:45 GMT",
            "title": "23-20670 Abby Lynn Hardy",
            "name": {
                "first": "Abby", 
                "last": "Hardy"
            },
            "parsed": {
                "first": "bk",
                "second": "4",
                "third": "13",
                "fourth": "Jack, tr Sec",
                "doc-link": "https://example.com/doc1/182031340621?pdf_header=&de_seq_num=44&caseid=456177"
            }
        }"#;
        let spec: Vec<RegexMatch> = 
            vec![ 
                RegexMatch {
                    source: "/description".to_owned(), 
                    destination: "/parsed/first".to_owned(),
                    re: Regex::new(r"(?i)First:\s+(\w+)\b").unwrap()
                },
                RegexMatch {
                    source: "/description".to_owned(), 
                    destination: "/parsed/second".to_owned(),
                    re: Regex::new(r"(?i)Second:\s+(\w+)\b").unwrap()
                },
                RegexMatch {
                    source: "/description".to_owned(), 
                    destination: "/parsed/third".to_owned(),
                    re: Regex::new(r"(?i)Third:\s+(\w+)\b").unwrap()
                },
                RegexMatch {
                    source: "/description".to_owned(), 
                    destination: "/parsed/fourth".to_owned(),
                    re: Regex::new(r"(?i)Fourth:\s+([\w,\s\.\']*\S)\s*\[").unwrap()
                },
                RegexMatch {
                    source: "/description".to_owned(), 
                    destination: "/parsed/doc-link".to_owned(),
                    re: Regex::new(r"href='([^']+)'").unwrap()
                }                              
            ];

        let record = Record::new(INPUT);
        let result = compute_regex_append_values(&record, &spec).unwrap();
        let expected_value:Value = serde_json::from_str(EXPECTED).unwrap();
        assert_eq!(result, expected_value);
    }

}