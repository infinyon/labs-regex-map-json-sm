## Regex-Map JSON Smartmodule

SmartModule to read a JSON record, look-up values, run regex, and write the result back into the record. This SmartModule is [map] type, where each record-in generates a new records-out.

### Input Record

A JSON object:

```json
{
  "description": "First: bk Second: 4 Third: 13 Fourth: Jack, tr Sec  [Encased string - (data)] (<a href='https://example.com/doc1/182031340621?pdf_header=&de_seq_num=44&caseid=456177'>9</a>)",
  "name": {
      "first": "Abby",
      "last": "Hardy",
      "ssn": "123-45-6789"
  }
}
```

### Transformation spec

The transformation spec takes two types of `regex` operations: `capture` and `replace`. 

Regex `captures` retrieves a substring from a json value, and it requires the following parameters:

* `regex`: perl style regular expressions (as used by Rust Regex)
* `target`: the path notation of the `json` value we operate on
    * i.e top level: `/description`; nested: `/name/last` or `/names/1/last`
* `output`: the path of the `json` key for the output:
    * if the key exists, it is overwritten; otherwise it is created.
    * the path will inject into the json at various hierarcies

Regex `replace` replaces substrings in a json value, and it requires the following parameters:

* `regex`: perl style regular expressions (as used by Rust Regex)
* `target`: the path notation of the `json` value
* `with`: the string to replace the value matched by regex

In this example, we'll use the following transformation spec:

```yaml
transforms:
  - uses: <group>/regex-map-json@0.1.1
    with:
      spec:
        - capture:
            regex: "(?i)First:\\s+(\\w+)\\b"
            target: "/description"
            output: "/parsed/first"      
        - capture:
            regex: "(?i)Second:\\s+(\\w+)\\b"
            target: "/description"
            output: "/parsed/second"
        - capture: 
            regex: "(?i)Third:\\s+(\\w+)\\b"
            target: "/description"
            output: "/parsed/third"     
        - capture: 
            regex: "(?i)Fourth:\\s+([\\w,\\s\\.\\']*\\S)\\s*\\["
            target: "/description"
            output: "/parsed/fourth"
        - capture:
            regex: "href='([^']+)'"
            target: "/description"
            output: "/parsed/doc-link"
        - replace:
            regex: "\\d{3}-\\d{2}-\\d{4}"
            target: "/name/ssn"
            with: "***-**-****"
```

### Outpot Record

A JSON object with a new `parsed` tree, and masked `ssn` value:

```json
{
  "description": "First: bk Second: 4 Third: 13 Fourth: Jack, tr Sec  [Encased string - (data)] (<a href='https://example.com/doc1/182031340621?pdf_header=&de_seq_num=44&caseid=456177'>9</a>)",
  "name": {
    "first": "Abby",
    "last": "Hardy",
    "ssn": "***-**-****"
  },  
  "parsed": {
    "doc-link": "https://example.com/doc1/182031340621?pdf_header=&de_seq_num=44&caseid=456177",
    "first": "bk",
    "fourth": "Jack, tr Sec",
    "second": "4",
    "third": "13"
  }
}
```

Note, no result is generated if the `target` key cannot be found, or the `regex` capture operation returns no matches.


### Build binary

Use `smdk` command tools to build:

```bash
smdk build
```

### Inline Test 

Use `smdk` to test:

```bash
smdk test --file ./test-data/input.json --raw -e spec='[{"capture": {"regex": "(?i)First:\\s+(\\w+)\\b", "target": "/description", "output": "/parsed/first"}}, {"replace": {"regex": "\\d{3}-\\d{2}-\\d{4}", "target": "/name/ssn", "with": "***-**-****" }}]'
```

### Cluster Test

Use `smdk` to load to cluster:

```bash
smdk load 
```

Test using `transform.yaml` file:

```bash
smdk test --file ./test-data/input.json --raw  --transforms-file ./test-data/transform.yaml
```

### Cargo Compatible

Build & Test

```
cargo build
```

```
cargo test
```


[map]: https://www.fluvio.io/smartmodules/transform/map/