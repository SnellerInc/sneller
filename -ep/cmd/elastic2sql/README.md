# elastic2sql

## Usage

Stream in the JSON query to translate and provide the desired table name as argument:

```
$ ./elastic2sql
usage: elastic2sql <table>
$ ./elastic2sql table < query.json
SELECT * FROM table WHERE foo.bar = 'aap'
```

## Pipe to sneller cloud

```
$ cat query.json | ./elastic2sql table | curl -X POST -H 'Authorization: Bearer <<token>>' --data-binary @- 'https://aws-master-us-east-1.sneller-dev.io/executeQuery?database=db&json'
```

## Pretty printing

Install [SQL formatter](https://github.com/zeroturnaround/sql-formatter):
```
$ npm install sql-formatter
```

```
$ echo '{"aggs":{}}' | ./elastic2sql table | npx sql-formatter
SELECT
  COUNT(*)
FROM
  table
```
