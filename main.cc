#include <duckdb.hpp>

int main() {
  duckdb::DBConfig conf;
  conf.maximum_threads = 32;
  duckdb::DuckDB db(nullptr, &conf);
  duckdb::Connection con(db);
  con.Query(
      "CREATE VIEW particles AS SELECT * FROM "
      "'/Users/qingzheng/CLionProjects/c2/c2-vpic-sample-dataset-parquet/"
      "particles/*.parquet'");
  std::unique_ptr<duckdb::QueryResult> r =
      con.SendQuery("SELECT * FROM particles");
  int total = 0;
  int j = 0;
  for (duckdb::QueryResult* a = r.get(); a; a = a->next.get()) {
    fprintf(stderr, "==Query result %d:\n", j);
    std::unique_ptr<duckdb::DataChunk> d = r->FetchRaw();
    int i = 0;
    while (d) {
      fprintf(stderr, "> Chunk: %d-%d (%d)\n", j, i++, d->size());
      // d->Print();
      total += d->size();
      d = r->FetchRaw();
    }
    j++;
  }
  fprintf(stderr, "total rows: %d\n", total);
  return 0;
}
