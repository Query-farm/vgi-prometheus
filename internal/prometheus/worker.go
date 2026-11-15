// Package prometheus exposes the Prometheus HTTP API (and PromQL tools) as a VGI catalog.
//
// To add a function: write it (see query.go, samples.go, catalog_funcs.go or
// compute.go), then register it in NewWorker below.
//
// Run it:
//
//	vgi-prometheus-worker                      # stdio: what DuckDB launches by default
//	vgi-prometheus-worker --http --port 8000   # HTTP: for Docker / hosted deploys
//	vgi-prometheus-worker --help
//
// Attach it from DuckDB:
//
//	ATTACH 'prometheus' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-prometheus-worker');
package prometheus

import (
	"github.com/Query-farm/vgi-go/vgi"
)

// allExamples lists every function's examples for the schema-level tag.
func allExamples() []exampleQuery {
	lists := [][]vgi.CatalogExample{queryExamples, queryRangeExamples, samplesExamples, seriesExamples,
		labelNamesExamples, labelValuesExamples, metadataExamples, targetsExamples, rulesExamples,
		alertsExamples, alertmanagersExamples, tsdbExamples, buildInfoExamples}
	return exampleQueries(append(lists, toolsExamples()...)...)
}

// NewWorker builds the worker: one catalog, one main schema. Extra options
// (logging, storage) are appended after the catalog options.
func NewWorker(opts ...vgi.WorkerOption) *vgi.Worker {
	sourceURL, version := SourceURL, Version
	base := []vgi.WorkerOption{
		vgi.WithCatalogName(CatalogName),
		vgi.WithCatalogComment(CatalogDocMD),
		vgi.WithCatalogTags(CatalogTags),
		vgi.WithCatalogInfo(vgi.CatalogInfo{SourceURL: &sourceURL, ImplementationVersion: &version}),
		vgi.WithSchemaComments(map[string]string{"main": SchemaDocMD}),
		vgi.WithSchemaTags(map[string]map[string]string{"main": schemaTags(allExamples())}),
		vgi.WithSecretTypes(APISecretType),
	}
	w := vgi.NewWorker(append(base, opts...)...)
	registerEvaluator()
	w.RegisterTable(vgi.AsTableFunction[QueryState](&QueryFunc{}))
	w.RegisterTable(vgi.AsTableFunction[RangeState](&QueryRangeFunc{}))
	w.RegisterTable(vgi.AsTableFunction[SamplesState](&SamplesFunc{}))
	for _, f := range []vgi.TableFunction{newSeries(), newLabelNames(), newLabelValues(), newMetadata(),
		newTargets(), newRules(), newAlerts(), newAlertmanagers(), newTSDBStatus(), newBuildInfo()} {
		w.RegisterTable(f)
	}
	registerTools(w)
	return w
}
