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

package stackdriverexporter

import (
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-collector/config"
	"github.com/open-telemetry/opentelemetry-collector/config/configmodels"
)

func TestLoadConfig(t *testing.T) {
	facotries, err := config.ExampleComponents()
	assert.Nil(t, err)

	factory := &Factory{}
	facotries.Exporters[typeStr] = factory
	cfg, err := config.LoadConfigFile(
		t, path.Join(".", "testdata", "config.yaml"), facotries,
	)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, len(cfg.Exporters), 2)

	r0 := cfg.Exporters["stackdriver"]
	assert.Equal(t, r0, factory.CreateDefaultConfig())

	r1 := cfg.Exporters["stackdriver/customname"].(*Config)
	assert.Equal(t, r1,
		&Config{
			ExporterSettings:           configmodels.ExporterSettings{TypeVal: typeStr, NameVal: "stackdriver/customname"},
			ProjectID:                  "my-project",
			Prefix:                     "prefix",
			Endpoint:                   "test-endpoint",
			NumOfWorkers:               3,
			SkipCreateMetricDescriptor: true,
			UseInsecure:                true,
		})
}
