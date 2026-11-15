// Command vgi-prometheus-worker serves the prometheus catalog to DuckDB.
//
//	vgi-prometheus-worker                                  # stdio (what DuckDB launches)
//	vgi-prometheus-worker --http --host 0.0.0.0 --port 8000  # HTTP, GET /health
//	vgi-prometheus-worker --unix /tmp/prometheus.sock         # AF_UNIX (launch: transport)
//
// Attach it from DuckDB:
//
//	ATTACH 'prometheus' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-prometheus-worker');
package main

import (
	"os"

	"github.com/Query-farm/vgi-go/vgi"

	"github.com/Query-farm/vgi-prometheus/internal/prometheus"
	"github.com/Query-farm/vgi-prometheus/internal/serve"
)

func main() {
	serve.Main("vgi-prometheus-worker", os.Args[1:], func() *vgi.Worker { return prometheus.NewWorker() })
}
