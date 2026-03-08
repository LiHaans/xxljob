# xxljob

Java 8 tool: scan `*.jsonl.gz`, extract MinIO object names from a configured array field, download from source MinIO, and upload to target MinIO.

## Features

- Recursively scans a directory for `*.jsonl.gz`
- Reads one JSON object per line
- Extracts object names from a configurable array field path
- Ignores missing source objects
- Streams data from source MinIO to target MinIO
- Supports optional target key prefix
- Java 8 compatible

## Build

```bash
mvn clean package
```

## Run

```bash
java -jar target/minio-transfer-tool-1.0.0.jar --config config.properties
```

## Config example

See `config-example.properties`.

### Important field-path rules

`json.arrayFieldPath` supports dot notation, for example:

- `files`
- `data.files`
- `payload.items`

The target field must resolve to a JSON array. Array items can be:

- string: `"path/to/object.jpg"`
- object: extract with `json.objectNameField`, e.g. `{"objectName":"path/a"}`

## Example

If one line is:

```json
{"records":[{"objectName":"a/b/c.png"},{"objectName":"x/y/z.pdf"}]}
```

Use:

```properties
json.arrayFieldPath=records
json.objectNameField=objectName
```

## Notes

- Source object not found -> skipped
- Other failures -> logged and continue
- Existing target object behavior depends on your MinIO setup; this tool uploads directly to target key
