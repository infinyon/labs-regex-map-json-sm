## Regex-Map JSON Smartmodule

SmartModule to read JSON Records, look-up values, run regex to generate a result, and append the result in a new key. This SmartModule is [map] type, where each record-in generates a new records-out.

### Input Record

A JSON object:

```json
{
  "description": "First: bk Second: 4 Third: 13 Fourth: Jack, tr Sec  [Encased string - (data)] (<a href='https://example.com/doc1/182031340621?pdf_header=&de_seq_num=44&caseid=456177'>9</a>)",
  "title": "23-20670 Abby Lynn Hardy"
}
```

### Transformation spec

The transformation spec takes an array of `regex` expression with the following params:
* `regex`: perl style regular expressions (as used by Rust Regex)
* `source`: the `json` key in path notation:
    * To level: `/description`, or nested: `/name/last` or `/names/1/last`
* `destination`: the `json` key in path notation:
    * `name/new` if the key already exists, it is overwritten, if it does not it is created.
    * the path will inject into the json at various hierarcies

In this example, we'll use the following transformation spec:

```yaml
transforms:
  - uses: <group>/regex-map-json@0.1.0
    with:
      spec:
        - regex: "(?i)First:\\s+(\\w+)\\b"
          source: "/description"
          destination: "/parsed/first"      
        - regex: "(?i)Second:\\s+(\\w+)\\b"
          source: "/description"
          destination: "/parsed/second"
        - regex: "(?i)Third:\\s+(\\w+)\\b"
          source: "/description"
          destination: "/parsed/third"     
        - regex: "(?i)Fourth:\\s+([\\w,\\s\\.\\']*\\S)\\s*\\["
          source: "/description"
          destination: "/parsed/fourth"
        - regex: "href='([^']+)'"
          source: "/description"
          destination: "/parsed/doc-link"
```

### Outpot Record

A JSON object augmented with `dedup_key`, and a digest:

```json
{
  "description": "First: bk Second: 4 Third: 13 Fourth: Jack, tr Sec  [Encased string - (data)] (<a href='https://example.com/doc1/182031340621?pdf_header=&de_seq_num=44&caseid=456177'>9</a>)",
  "parsed": {
    "doc-link": "https://example.com/doc1/182031340621?pdf_header=&de_seq_num=44&caseid=456177",
    "first": "bk",
    "fourth": "Jack, tr Sec",
    "second": "4",
    "third": "13"
  },
  "title": "23-20670 Abby Lynn Hardy"
}
```

Note, no result is generated if the `source` key cannot be found, or the `regex` return null.


### Build binary

Use `smdk` command tools to build:

```bash
smdk build
```

### Inline Test 

Use `smdk` to test:

```bash
smdk test --file ./test-data/input.json --raw -e spec='[{"regex": "?i)First:\\s+(\\w+)\\b", "source": "/description", "destination": "/parsed/first"}]'
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