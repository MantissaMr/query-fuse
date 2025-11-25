# Query Fuse

An interactive SQL query engine for local columnar files (Parquet, Arrow, Feather)

## Installation

### Docker

```bash
docker run -it -v $PWD:/data alamiinsi/query-fuse -i /data/your-file.parquet
```

### Pre-compiled Binaries

Download the latest release for your OS from the Releases Page, unzip, and add to your PATH.

### Cargo

```bash
cargo install query-fuse
```

## Usage

```bash
query-fuse -i data.parquet
```

```sql
query-fuse > SELECT count(*) FROM data
+----------+
| count(*) |
+----------+
| 50000    |
+----------+

query-fuse > SELECT city, AVG(temp) FROM data GROUP BY city
+-------+--------------------+
| city  | AVG(data.temp)     |
+-------+--------------------+
| Lagos | 30.5               |
| Kano  | 35.2               |
+-------+--------------------+
```

### Commands

- `.tables` - List all registered tables
- `.help` - Show available commands
- `.exit` or `.quit` - Exit the shell

## License

MIT License