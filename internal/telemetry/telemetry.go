/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package telemetry

import (
	"context"
	"fmt"

	"github.com/meshx-org/timescaledb-event-streamer/internal/logging"
	"github.com/meshx-org/timescaledb-event-streamer/spi/config"
	"github.com/meshx-org/timescaledb-event-streamer/spi/version"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// InstrumentationName is the tracer name used for all spans emitted by the tool.
const InstrumentationName = "github.com/meshx-org/timescaledb-event-streamer"

// ShutdownFunc flushes any buffered spans and releases telemetry resources. It
// is safe to call even when tracing is disabled (it is then a no-op).
type ShutdownFunc func(ctx context.Context) error

// Tracer returns the tool's tracer from the globally registered provider. When
// tracing is disabled this resolves to a no-op tracer with negligible overhead.
func Tracer() trace.Tracer {
	return otel.Tracer(InstrumentationName)
}

// Initialize wires up the W3C trace-context propagators and, when tracing is
// enabled, an OTLP exporter and batching TracerProvider registered globally.
// The returned ShutdownFunc must be invoked on shutdown to flush pending spans.
func Initialize(
	c *config.Config,
) (ShutdownFunc, error) {

	// Always install the W3C propagators so the tool can continue an inbound
	// trace and forward context downstream regardless of whether it exports.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	noop := func(context.Context) error { return nil }

	if !config.GetOrDefault(c, config.PropertyTelemetryTracingEnabled, false) {
		return noop, nil
	}

	logger, err := logging.NewLogger("Telemetry")
	if err != nil {
		return nil, err
	}

	exporter, err := newTraceExporter(c)
	if err != nil {
		return nil, err
	}

	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		attribute.String("service.name", serviceName(c)),
		attribute.String("service.version", version.Version),
	))
	if err != nil {
		return nil, err
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tracerProvider)

	logger.Infof("OpenTelemetry tracing enabled (service.name=%s)", serviceName(c))
	return tracerProvider.Shutdown, nil
}

func serviceName(
	c *config.Config,
) string {

	return config.GetOrDefault(c, config.PropertyTelemetryTracingServiceName, version.BinName)
}

func newTraceExporter(
	c *config.Config,
) (sdktrace.SpanExporter, error) {

	protocol := config.GetOrDefault(c, config.PropertyTelemetryTracingExporterProtocol, "grpc")
	endpoint := config.GetOrDefault(c, config.PropertyTelemetryTracingExporterEndpoint, "")
	insecure := config.GetOrDefault(c, config.PropertyTelemetryTracingExporterInsecure, false)
	headers := c.Telemetry.Tracing.Exporter.Headers
	ctx := context.Background()

	switch protocol {
	case "http", "http/protobuf":
		opts := make([]otlptracehttp.Option, 0)
		if endpoint != "" {
			opts = append(opts, otlptracehttp.WithEndpoint(endpoint))
		}
		if insecure {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		if len(headers) > 0 {
			opts = append(opts, otlptracehttp.WithHeaders(headers))
		}
		return otlptracehttp.New(ctx, opts...)
	case "grpc", "":
		opts := make([]otlptracegrpc.Option, 0)
		if endpoint != "" {
			opts = append(opts, otlptracegrpc.WithEndpoint(endpoint))
		}
		if insecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		if len(headers) > 0 {
			opts = append(opts, otlptracegrpc.WithHeaders(headers))
		}
		return otlptracegrpc.New(ctx, opts...)
	default:
		return nil, fmt.Errorf(
			"unsupported telemetry exporter protocol %q (use \"grpc\" or \"http\")", protocol,
		)
	}
}
