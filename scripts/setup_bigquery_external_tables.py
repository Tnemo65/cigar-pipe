"""External wildcard serving is retired; native tables are created by export."""

def setup_external_tables():
    raise RuntimeError("External publication is retired. Use export_bigquery with an isolated native dataset; see docs/implementation.md.")

if __name__ == "__main__":
    setup_external_tables()
