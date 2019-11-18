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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector/consumer"
	"github.com/open-telemetry/opentelemetry-collector/consumer/consumerdata"
	"github.com/open-telemetry/opentelemetry-collector/receiver"
)

var (
	errNilNextConsumer = errors.New("nil nextConsumer")
	errAlreadyStarted  = errors.New("already started")
	errAlreadyStopped  = errors.New("already stopped")
)

// make configurable
// const defaultAttributesPrefix = "sfxdim_"

var _ receiver.MetricsReceiver = (*collectdReceiver)(nil)

// collectdReceiver implements the receiver.TraceReceiver for CollectD protocol.
type collectdReceiver struct {
	sync.Mutex
	logger             *zap.Logger
	addr               string
	server             *http.Server
	defaultAttrsPrefix string
	nextConsumer       consumer.MetricsConsumer

	startOnce sync.Once
	stopOnce  sync.Once
}

// New creates the CollectD receiver with the given parameters.
func New(
	logger *zap.Logger,
	addr string,
	timeout time.Duration,
	defaultAttrsPrefix string,
	nextConsumer consumer.MetricsConsumer) (receiver.MetricsReceiver, error) {
	if nextConsumer == nil {
		return nil, errNilNextConsumer
	}

	r := &collectdReceiver{
		addr:               addr,
		nextConsumer:       nextConsumer,
		defaultAttrsPrefix: defaultAttrsPrefix,
	}
	r.server = &http.Server{
		Addr:         addr,
		Handler:      r,
		ReadTimeout:  timeout,
		WriteTimeout: timeout,
	}
	return r, nil
}

const traceSource string = "CollectD"

// TraceSource returns the name of the trace data source.
func (cdr *collectdReceiver) MetricsSource() string {
	return traceSource
}

func (cdr *collectdReceiver) StartMetricsReception(host receiver.Host) error {
	cdr.Lock()
	defer cdr.Unlock()

	err := errAlreadyStarted
	cdr.startOnce.Do(func() {
		err = nil
		go func() {
			_ = cdr.server.ListenAndServe()
		}()
	})

	return err
}

func (cdr *collectdReceiver) StopMetricsReception() error {
	cdr.Lock()
	defer cdr.Unlock()

	var err = errAlreadyStopped
	cdr.stopOnce.Do(func() {
		err = cdr.server.Shutdown(context.Background())
	})
	return err
}

func (cdr *collectdReceiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		// handle err
		return
	}
	var records []collectDRecord
	err = json.Unmarshal(body, &records)
	if err != nil {
		// handle err
		return
	}

	defaultAttrs := cdr.defaultAttributes(r)

	md := consumerdata.MetricsData{}
	ctx := context.Background()
	for _, record := range records {
		md.Metrics, err = record.extractAndAppendMetrics(md.Metrics, defaultAttrs)
		if err != nil {
			fmt.Println(err)
			// handle error
			continue
		}
	}
	for _, m := range md.Metrics {
		fmt.Println("===========")
		fmt.Println("name: ", m.MetricDescriptor.Name)
		fmt.Println("type: ", m.MetricDescriptor.Type)
		fmt.Println("labels: ", m.MetricDescriptor.LabelKeys)
		fmt.Println("ts: ", m.Timeseries)
		fmt.Println("===========")
	}
	cdr.nextConsumer.ConsumeMetricsData(ctx, md)
}

func (cdr *collectdReceiver) defaultAttributes(req *http.Request) map[string]string {
	if cdr.defaultAttrsPrefix == "" {
		return nil
	}
	params := req.URL.Query()
	attrs := make(map[string]string, 0)
	for key := range params {
		if strings.HasPrefix(key, cdr.defaultAttrsPrefix) {
			value := params.Get(key)
			/*
				// record metric for blank attributes
				if len(value) == 0 {
					atomic.AddInt64(&decoder.TotalBlankDims, 1)
					continue
				}
			*/
			key = key[len(cdr.defaultAttrsPrefix):]
			attrs[key] = value
		}
	}
	return attrs
}
