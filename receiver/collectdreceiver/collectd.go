// Copyright 2019, OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collectdreceiver

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes"

	"github.com/golang/protobuf/ptypes/timestamp"

	metricspb "github.com/census-instrumentation/opencensus-proto/gen-go/metrics/v1"
)

const (
	defaultMetricType string = "guage"
)

var metricTypeMapInt = map[string]metricspb.MetricDescriptor_Type{
	"gauge":    metricspb.MetricDescriptor_GAUGE_INT64,
	"derive":   metricspb.MetricDescriptor_CUMULATIVE_INT64,
	"counter":  metricspb.MetricDescriptor_CUMULATIVE_INT64,
	"absolute": metricspb.MetricDescriptor_GAUGE_INT64,
}

var metricTypeMapDouble = map[string]metricspb.MetricDescriptor_Type{
	"gauge":    metricspb.MetricDescriptor_GAUGE_DOUBLE,
	"derive":   metricspb.MetricDescriptor_CUMULATIVE_DOUBLE,
	"counter":  metricspb.MetricDescriptor_CUMULATIVE_DOUBLE,
	"absolute": metricspb.MetricDescriptor_GAUGE_DOUBLE,
}

type collectDRecord struct {
	Dsnames        []*string      `json:"dsnames"`
	Dstypes        []*string      `json:"dstypes"`
	Host           *string        `json:"host"`
	Interval       *float64       `json:"interval"`
	Plugin         *string        `json:"plugin"`
	PluginInstance *string        `json:"plugin_instance"`
	Time           *float64       `json:"time"`
	TypeS          *string        `json:"type"`
	TypeInstance   *string        `json:"type_instance"`
	Values         []*json.Number `json:"values"`

	// event fields
	Message  *string                `json:"message"`
	Meta     map[string]interface{} `json:"meta"`
	Severity *string                `json:"severity"`
}

func (r collectDRecord) isEvent() bool {
	return false
}

func (r collectDRecord) protoTime() *timestamp.Timestamp {
	if r.Time == nil {
		return nil
	}
	ts := time.Unix(0, int64(float64(time.Second)**r.Time))
	tsp, err := ptypes.TimestampProto(ts)
	if err != nil {
		return nil
	}
	return tsp
}

func (r collectDRecord) extractAndAppendMetrics(metrics []*metricspb.Metric, defaultLabels map[string]string) ([]*metricspb.Metric, error) {
	// default tags/dimensions
	// value
	// timestamp
	// name
	// used DS name?? (tags??)
	// additional tags/dimensions

	labels := make(map[string]string, len(defaultLabels))
	for k, v := range defaultLabels {
		labels[k] = v
	}

	for i := range r.Dsnames {
		if i < len(r.Dstypes) && i < len(r.Values) && r.Values[i] != nil {
			dsType, dsName, val := r.Dstypes[i], r.Dsnames[i], r.Values[i]
			_ = dsName
			metricName, usedDsName := r.getReasonableMetricName(i, labels)
			_ = usedDsName

			addIfNotNullOrEmpty(labels, "plugin", true, r.Plugin)
			parseAndAddLabels(labels, r.PluginInstance, r.Host)
			addIfNotNullOrEmpty(labels, "dsname", !usedDsName, dsName)

			metric, _ := r.newMetric(metricName, dsType, val, labels)
			metrics = append(metrics, metric)

		}
	}
	return metrics, nil
}

func (r collectDRecord) newMetric(name string, dsType *string, val *json.Number, labels map[string]string) (*metricspb.Metric, error) {
	metric := &metricspb.Metric{}
	point, isDouble, err := r.newPoint(val)
	if err != nil {
		return metric, err
	}

	lKeys, lValues := labelKeysAndValues(labels)
	metric.MetricDescriptor = &metricspb.MetricDescriptor{
		Name:      name,
		Type:      r.metricType(dsType, isDouble),
		LabelKeys: lKeys,
	}
	metric.Timeseries = []*metricspb.TimeSeries{&metricspb.TimeSeries{
		LabelValues: lValues,
		Points:      []*metricspb.Point{point},
	}}

	return metric, nil
}

func labelKeysAndValues(labels map[string]string) ([]*metricspb.LabelKey, []*metricspb.LabelValue) {
	keys := make([]*metricspb.LabelKey, len(labels))
	values := make([]*metricspb.LabelValue, len(labels))
	i := 0
	for k, v := range labels {
		keys[i] = &metricspb.LabelKey{Key: k}
		values[i] = &metricspb.LabelValue{Value: v}
		i++
	}
	return keys, values
}

func (r collectDRecord) metricType(dsType *string, isDouble bool) metricspb.MetricDescriptor_Type {
	val := ""
	if dsType != nil {
		val = *dsType
	}

	var metricType metricspb.MetricDescriptor_Type
	switch val {
	case "counter", "derive":
		if isDouble {
			metricType = metricspb.MetricDescriptor_CUMULATIVE_DOUBLE
			break
		}
		metricType = metricspb.MetricDescriptor_CUMULATIVE_INT64

	// TODO: check which type to use for absolute. Prometheus collectd exporter just
	// ignores this type: https://github.com/prometheus/collectd_exporter/blob/master/main.go#L109-L129
	case "gauge", "absolute", "":
		if isDouble {
			metricType = metricspb.MetricDescriptor_GAUGE_DOUBLE
			break
		}
		metricType = metricspb.MetricDescriptor_GAUGE_INT64
	}
	return metricType
}

func (r collectDRecord) newPoint(val *json.Number) (*metricspb.Point, bool, error) {
	p := &metricspb.Point{
		Timestamp: r.protoTime(),
	}

	isDouble := true
	if v, err := val.Int64(); err == nil {
		isDouble = false
		p.Value = &metricspb.Point_Int64Value{Int64Value: v}
	} else {
		v, err := val.Float64()
		if err != nil {
			return nil, isDouble, fmt.Errorf("value could not be decoded: %v", err)
		}
		p.Value = &metricspb.Point_DoubleValue{DoubleValue: v}
	}
	return p, isDouble, nil
}

// getReasonableMetricName creates metrics names by joining them (if non empty) type.typeinstance
// if there are more than one dsname append .dsname for the particular uint. if there's only one it
// becomes a dimension
func (r collectDRecord) getReasonableMetricName(index int, attrs map[string]string) (string, bool) {
	usedDsName := false
	parts := make([]byte, 0, len(*r.TypeS)+len(*r.TypeInstance))

	if isNilOrEmpty(r.TypeS) {
		parts = append(parts, *r.TypeS...)
	}
	parts = r.pointTypeInstance(attrs, parts)
	if r.Dsnames != nil && !isNilOrEmpty(r.Dsnames[index]) && len(r.Dsnames) > 1 {
		if len(parts) > 0 {
			parts = append(parts, '.')
		}
		parts = append(parts, *r.Dsnames[index]...)
		usedDsName = true
	}
	return string(parts), usedDsName
}

func (r collectDRecord) pointTypeInstance(attrs map[string]string, parts []byte) []byte {
	if !isNilOrEmpty(r.TypeInstance) {
		instanceName, extractedAttrs := labelsFromName(r.TypeInstance)
		if instanceName != "" {
			if len(parts) > 0 {
				parts = append(parts, '.')
			}
			parts = append(parts, instanceName...)
		}
		for k, v := range extractedAttrs {
			if _, exists := attrs[k]; !exists {
				addIfNotNullOrEmpty(attrs, k, true, &v)
			}
		}
	}
	return parts
}

// labelsFromName tries to pull out dimensions out of name in the format name[k=v,f=x]-morename
// would return name-morename and extract dimensions (k,v) and (f,x)
// if we encounter something we don't expect use original
// this is a bit complicated to avoid allocations, string.split allocates, while slices
// inside same function, do not.
func labelsFromName(val *string) (instanceName string, toAddDims map[string]string) {
	instanceName = *val
	index := strings.Index(*val, "[")
	if index > -1 {
		left := (*val)[:index]
		rest := (*val)[index+1:]
		index = strings.Index(rest, "]")
		if index > -1 {
			working := make(map[string]string)
			dimensions := rest[:index]
			rest = rest[index+1:]
			cindex := strings.Index(dimensions, ",")
			prev := 0
			for {
				if cindex < prev {
					cindex = len(dimensions)
				}
				piece := dimensions[prev:cindex]
				tindex := strings.Index(piece, "=")
				if tindex == -1 || strings.Index(piece[tindex+1:], "=") > -1 {
					return
				}
				working[piece[:tindex]] = piece[tindex+1:]
				if cindex == len(dimensions) {
					break
				}
				prev = cindex + 1
				cindex = strings.Index(dimensions[prev:], ",") + prev
			}
			toAddDims = working
			instanceName = left + rest
		}
	}
	return
}

func isNilOrEmpty(str *string) bool {
	return str == nil || *str == ""
}

func addIfNotNullOrEmpty(m map[string]string, key string, cond bool, val *string) {
	if cond && val != nil && *val != "" {
		m[key] = *val
	}
}

func parseAndAddLabels(labels map[string]string, pluginInstance *string, host *string) {
	parseNameForLabels(labels, "plugin_instance", pluginInstance)
	parseNameForLabels(labels, "host", host)
}

func parseNameForLabels(labels map[string]string, key string, val *string) {
	instanceName, toAddDims := labelsFromName(val)

	for k, v := range toAddDims {
		if _, exists := labels[k]; !exists {
			addIfNotNullOrEmpty(labels, k, true, &v)
		}
	}
	addIfNotNullOrEmpty(labels, key, true, &instanceName)
}
