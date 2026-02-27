package etlxlib

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// OTelManager manages OpenTelemetry tracer and spans
type OTelManager struct {
	tracer           oteltrace.Tracer
	span             oteltrace.Span
	ctx              context.Context
	cancel           context.CancelFunc
	tp               *trace.TracerProvider
	mu               sync.Mutex
	processLogs      []map[string]any
	currentOperation string
}

var (
	globalOTelManager *OTelManager
	once              sync.Once
)

// InitializeOTel initializes OpenTelemetry with OTLP HTTP exporter
func InitializeOTel(serviceName string) (*OTelManager, error) {
	var err error
	once.Do(func() {
		// Get OTLP endpoint from environment or use default
		otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
		if otlpEndpoint == "" {
			otlpEndpoint = "http://localhost:4318"
		}

		// Create OTLP exporter
		exporter, expErr := otlptracehttp.New(context.Background(),
			otlptracehttp.WithEndpoint(otlpEndpoint),
		)
		if expErr != nil {
			err = fmt.Errorf("failed to create OTLP exporter: %w", expErr)
			return
		}

		// Create resource
		res, resErr := resource.New(context.Background(),
			resource.WithAttributes(
				semconv.ServiceName(serviceName),
				semconv.ServiceVersion("1.0.0"),
				attribute.String("environment", os.Getenv("ENVIRONMENT")),
			),
		)
		if resErr != nil {
			err = fmt.Errorf("failed to create resource: %w", resErr)
			return
		}

		// Create tracer provider
		tp := trace.NewTracerProvider(
			trace.WithBatcher(exporter),
			trace.WithResource(res),
		)

		otel.SetTracerProvider(tp)

		// Create context
		ctx, cancel := context.WithCancel(context.Background())

		globalOTelManager = &OTelManager{
			tracer:      tp.Tracer(serviceName),
			ctx:         ctx,
			cancel:      cancel,
			tp:          tp,
			processLogs: make([]map[string]any, 0),
		}
	})

	return globalOTelManager, err
}

// GetOTelManager returns the global OTelManager instance
func GetOTelManager() *OTelManager {
	if globalOTelManager == nil {
		panic("OpenTelemetry not initialized. Call InitializeOTel first.")
	}
	return globalOTelManager
}

// StartSpan creates a new span for an operation
func (om *OTelManager) StartSpan(operationName string, attributes map[string]any) (oteltrace.Span, context.Context) {
	om.mu.Lock()
	defer om.mu.Unlock()

	ctx, span := om.tracer.Start(om.ctx, operationName)

	// Add attributes to span
	attrs := make([]attribute.KeyValue, 0)
	for key, value := range attributes {
		attrs = append(attrs, attribute.String(key, fmt.Sprintf("%v", value)))
	}
	span.SetAttributes(attrs...)

	om.currentOperation = operationName
	return span, ctx
}

// EndSpan ends a span and records the log entry
func (om *OTelManager) EndSpan(span oteltrace.Span, logEntry map[string]any) {
	om.mu.Lock()
	defer om.mu.Unlock()

	if logEntry != nil {
		om.processLogs = append(om.processLogs, logEntry)

		// Add log entry as span attributes
		for key, value := range logEntry {
			switch v := value.(type) {
			case string:
				span.SetAttributes(attribute.String(key, v))
			case int:
				span.SetAttributes(attribute.Int(key, v))
			case float64:
				span.SetAttributes(attribute.Float64(key, v))
			case bool:
				span.SetAttributes(attribute.Bool(key, v))
			case time.Time:
				span.SetAttributes(attribute.String(key, v.Format(time.RFC3339)))
			default:
				span.SetAttributes(attribute.String(key, fmt.Sprintf("%v", v)))
			}
		}
	}

	span.End()
}

// RecordEvent records an event within a span
func (om *OTelManager) RecordEvent(span oteltrace.Span, eventName string, attributes map[string]any) {
	attrs := make([]attribute.KeyValue, 0)
	for key, value := range attributes {
		attrs = append(attrs, attribute.String(key, fmt.Sprintf("%v", value)))
	}
	span.AddEvent(eventName, oteltrace.WithAttributes(attrs...))
}

// GetProcessLogs returns all collected process logs
func (om *OTelManager) GetProcessLogs() []map[string]any {
	om.mu.Lock()
	defer om.mu.Unlock()

	// Create a copy to avoid external modifications
	logs := make([]map[string]any, len(om.processLogs))
	copy(logs, om.processLogs)
	return logs
}

// ClearProcessLogs clears the process logs
func (om *OTelManager) ClearProcessLogs() {
	om.mu.Lock()
	defer om.mu.Unlock()

	om.processLogs = make([]map[string]any, 0)
}

// Shutdown shuts down the OpenTelemetry tracer provider
func (om *OTelManager) Shutdown() error {
	om.mu.Lock()
	defer om.mu.Unlock()

	if om.cancel != nil {
		om.cancel()
	}

	if om.tp != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return om.tp.ForceFlush(ctx)
	}

	return nil
}

// StartOperationSpan creates a span for an operation and returns the span and context
func (om *OTelManager) StartOperationSpan(operationName string, attributes map[string]any) (oteltrace.Span, context.Context) {
	ctx, span := om.tracer.Start(om.ctx, operationName)
	
	// Add attributes 
	attrs := make([]attribute.KeyValue, 0)
	for key, value := range attributes {
		attrs = append(attrs, attribute.String(key, fmt.Sprintf("%v", value)))
	}
	span.SetAttributes(attrs...)
	
	return span, ctx
}

// RecordLogEntry adds a log entry to processLogs and records it as span attributes
func (om *OTelManager) RecordLogEntry(span oteltrace.Span, logEntry map[string]any) {
	om.mu.Lock()
	defer om.mu.Unlock()
	
	om.processLogs = append(om.processLogs, logEntry)
	
	// Also record to span
	for key, value := range logEntry {
		switch v := value.(type) {
		case string:
			span.SetAttributes(attribute.String(key, v))
		case int:
			span.SetAttributes(attribute.Int(key, v))
		case float64:
			span.SetAttributes(attribute.Float64(key, v))
		case bool:
			span.SetAttributes(attribute.Bool(key, v))
		case time.Time:
			span.SetAttributes(attribute.String(key, v.Format(time.RFC3339)))
		default:
			span.SetAttributes(attribute.String(key, fmt.Sprintf("%v", v)))
		}
	}
}
