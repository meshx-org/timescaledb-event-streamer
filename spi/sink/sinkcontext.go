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

package sink

import (
	"context"
	"encoding/json"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

type Context interface {
	SetTransientAttribute(key string, value string)
	TransientAttribute(key string) (value string, present bool)
	SetAttribute(key string, value string)
	Attribute(key string) (value string, present bool)
}

// traceContextAttribute is the reserved transient-attribute key under which the
// W3C trace propagation headers for the current event are carried.
const traceContextAttribute = "__otel_trace_context__"

// InjectTraceContext serializes the W3C trace propagation headers of ctx onto
// the sink context so that a Sink implementation can forward them downstream via
// TraceHeaders. It always overwrites any previously injected value (the sink
// context is reused across events), clearing it when ctx carries no span.
func InjectTraceContext(
	c Context, ctx context.Context,
) {

	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if len(carrier) == 0 {
		c.SetTransientAttribute(traceContextAttribute, "")
		return
	}

	data, err := json.Marshal(map[string]string(carrier))
	if err != nil {
		c.SetTransientAttribute(traceContextAttribute, "")
		return
	}
	c.SetTransientAttribute(traceContextAttribute, string(data))
}

// TraceHeaders returns the W3C trace propagation headers (e.g. traceparent,
// tracestate, baggage) previously injected via InjectTraceContext, or nil when
// no trace context is present. Sink implementations add these to the headers /
// metadata of the messages they emit so downstream consumers can continue the
// trace.
func TraceHeaders(
	c Context,
) map[string]string {

	value, present := c.TransientAttribute(traceContextAttribute)
	if !present || value == "" {
		return nil
	}

	var headers map[string]string
	if err := json.Unmarshal([]byte(value), &headers); err != nil {
		return nil
	}
	return headers
}
